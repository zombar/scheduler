package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zombar/purpletab/pkg/metrics"
	"github.com/zombar/purpletab/pkg/tracing"
	"github.com/zombar/scheduler"
	"github.com/zombar/scheduler/db"
	"github.com/zombar/scheduler/models"
	"github.com/zombar/scheduler/pkg/logging"
)

// Config contains server configuration
type Config struct {
	Addr            string
	DBConfig        db.Config
	SchedulerConfig scheduler.Config
	CORSEnabled     bool
}

// Server represents the HTTP server
type Server struct {
	config      Config
	db          *db.DB
	scheduler   *scheduler.Scheduler
	server      *http.Server
	httpMetrics *metrics.HTTPMetrics
	dbMetrics   *metrics.DatabaseMetrics
}

// NewServer creates a new server instance
func NewServer(config Config) (*Server, error) {
	// Initialize database
	database, err := db.New(config.DBConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize scheduler
	sched, err := scheduler.New(database, config.SchedulerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize scheduler: %w", err)
	}

	// Initialize Prometheus metrics
	httpMetrics := metrics.NewHTTPMetrics("scheduler")
	dbMetrics := metrics.NewDatabaseMetrics("scheduler")

	s := &Server{
		config:      config,
		db:          database,
		scheduler:   sched,
		httpMetrics: httpMetrics,
		dbMetrics:   dbMetrics,
	}

	// Start periodic database stats collection
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			dbMetrics.UpdateDBStats(database.DB())
		}
	}()

	// Setup routes
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler()) // Prometheus metrics endpoint
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/tasks/", s.handleTaskByID)

	// Wrap with middleware chain: metrics -> HTTP logging -> tracing -> CORS -> handlers
	var handler http.Handler = mux
	if config.CORSEnabled {
		handler = corsMiddleware(handler)
	}
	handler = tracing.HTTPMiddleware("scheduler")(handler)
	handler = logging.HTTPLoggingMiddleware(slog.Default())(handler)
	handler = httpMetrics.HTTPMiddleware(handler)

	s.server = &http.Server{
		Addr:    config.Addr,
		Handler: handler,
	}

	return s, nil
}

// Start starts the server and scheduler
func (s *Server) Start() error {
	// Start scheduler
	if err := s.scheduler.Start(); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	slog.Info("starting server", "addr", s.config.Addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	// Stop scheduler first
	if err := s.scheduler.Stop(); err != nil {
		slog.Error("error stopping scheduler", "error", err)
	}

	// Close database
	if err := s.db.Close(); err != nil {
		slog.Error("error closing database", "error", err)
	}

	// Shutdown HTTP server
	return s.server.Shutdown(ctx)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// handleTasks handles task list and creation
func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListTasks(w, r)
	case http.MethodPost:
		s.handleCreateTask(w, r)
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleTaskByID handles individual task operations
func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	id, err := strconv.ParseInt(path, 10, 64)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid task ID")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetTask(w, r, id)
	case http.MethodPut:
		s.handleUpdateTask(w, r, id)
	case http.MethodDelete:
		s.handleDeleteTask(w, r, id)
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleListTasks handles GET /api/tasks
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.db.ListTasks()
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list tasks: %v", err))
		return
	}

	respondJSON(w, http.StatusOK, tasks)
}

// handleCreateTask handles POST /api/tasks
func (s *Server) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	var task models.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	// Validate task
	if err := validateTask(&task); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("validation error: %v", err))
		return
	}

	// Create task
	if err := s.db.CreateTask(&task); err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create task: %v", err))
		return
	}

	// Schedule task if enabled
	if task.Enabled {
		if err := s.scheduler.RescheduleTask(&task); err != nil {
			slog.Error("failed to schedule task", "task_id", task.ID, "error", err)
		}
	}

	respondJSON(w, http.StatusCreated, task)
}

// handleGetTask handles GET /api/tasks/{id}
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request, id int64) {
	task, err := s.db.GetTask(id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get task: %v", err))
		return
	}

	if task == nil {
		respondError(w, http.StatusNotFound, "task not found")
		return
	}

	respondJSON(w, http.StatusOK, task)
}

// handleUpdateTask handles PUT /api/tasks/{id}
func (s *Server) handleUpdateTask(w http.ResponseWriter, r *http.Request, id int64) {
	var task models.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	task.ID = id

	// Validate task
	if err := validateTask(&task); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("validation error: %v", err))
		return
	}

	// Update task
	if err := s.db.UpdateTask(&task); err != nil {
		if err.Error() == "task not found" {
			respondError(w, http.StatusNotFound, "task not found")
		} else {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update task: %v", err))
		}
		return
	}

	// Reschedule task
	if err := s.scheduler.RescheduleTask(&task); err != nil {
		slog.Error("failed to reschedule task", "task_id", task.ID, "error", err)
	}

	respondJSON(w, http.StatusOK, task)
}

// handleDeleteTask handles DELETE /api/tasks/{id}
func (s *Server) handleDeleteTask(w http.ResponseWriter, r *http.Request, id int64) {
	if err := s.db.DeleteTask(id); err != nil {
		if err.Error() == "task not found" {
			respondError(w, http.StatusNotFound, "task not found")
		} else {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete task: %v", err))
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// validateTask validates a task
func validateTask(task *models.Task) error {
	if task.Name == "" {
		return fmt.Errorf("name is required")
	}

	if task.Type != models.TaskTypeSQL && task.Type != models.TaskTypeScrape {
		return fmt.Errorf("invalid task type: %s", task.Type)
	}

	if task.Schedule == "" {
		return fmt.Errorf("schedule is required")
	}

	// Validate config based on type
	switch task.Type {
	case models.TaskTypeSQL:
		config, err := db.ParseSQLTaskConfig(task.Config)
		if err != nil {
			return err
		}
		if config.SQL == "" {
			return fmt.Errorf("SQL is required for SQL tasks")
		}
		if config.TargetDB == "" {
			return fmt.Errorf("target_db is required for SQL tasks")
		}
	case models.TaskTypeScrape:
		config, err := db.ParseScrapeTaskConfig(task.Config)
		if err != nil {
			return err
		}
		if config.URL == "" {
			return fmt.Errorf("URL is required for scrape tasks")
		}
	}

	return nil
}

// respondJSON writes a JSON response
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError writes an error response
func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{
		"error": message,
	})
}

// corsMiddleware adds CORS headers
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
