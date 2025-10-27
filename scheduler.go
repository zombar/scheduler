package scheduler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/zombar/purpletab/pkg/tracing"
	"github.com/zombar/scheduler/db"
	"github.com/zombar/scheduler/models"
	"go.opentelemetry.io/otel/attribute"
)

// Config contains scheduler configuration
type Config struct {
	ControllerDBPath string
	ControllerAPIURL string
	ScraperAPIURL    string
}

// Scheduler manages scheduled tasks
type Scheduler struct {
	db            *db.DB
	cron          *cron.Cron
	config        Config
	controllerDB  *sql.DB
	taskEntries   map[int64]cron.EntryID // task ID -> cron entry ID
	mu            sync.RWMutex
}

// New creates a new Scheduler instance
func New(database *db.DB, config Config) (*Scheduler, error) {
	// Open controller database for SQL tasks
	controllerDB, err := sql.Open("sqlite", config.ControllerDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open controller database: %w", err)
	}

	s := &Scheduler{
		db:           database,
		cron:         cron.New(),
		config:       config,
		controllerDB: controllerDB,
		taskEntries:  make(map[int64]cron.EntryID),
	}

	return s, nil
}

// Start starts the scheduler
func (s *Scheduler) Start() error {
	// Load all enabled tasks
	tasks, err := s.db.GetEnabledTasks()
	if err != nil {
		return fmt.Errorf("failed to load enabled tasks: %w", err)
	}

	// Schedule all tasks
	for _, task := range tasks {
		if err := s.scheduleTask(task); err != nil {
			slog.Default().Error("failed to schedule task", "task_id", task.ID, "task_name", task.Name, "error", err)
		}
	}

	// Start cron
	s.cron.Start()
	slog.Default().Info("scheduler started")

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() error {
	ctx := s.cron.Stop()
	<-ctx.Done()

	if s.controllerDB != nil {
		s.controllerDB.Close()
	}

	slog.Default().Info("scheduler stopped")
	return nil
}

// scheduleTask adds a task to the cron scheduler
func (s *Scheduler) scheduleTask(task *models.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse cron schedule
	entryID, err := s.cron.AddFunc(task.Schedule, func() {
		if err := s.executeTask(task); err != nil {
			slog.Default().Error("task execution failed", "task_id", task.ID, "task_name", task.Name, "error", err)
		}
	})

	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	s.taskEntries[task.ID] = entryID

	// Update next run time
	entry := s.cron.Entry(entryID)
	nextRun := entry.Next
	task.NextRunAt = &nextRun
	s.db.UpdateTaskRunTime(task.ID, time.Now(), &nextRun)

	slog.Default().Info("scheduled task", "task_id", task.ID, "task_name", task.Name, "schedule", task.Schedule, "next_run", nextRun.Format(time.RFC3339))

	return nil
}

// unscheduleTask removes a task from the cron scheduler
func (s *Scheduler) unscheduleTask(taskID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.taskEntries[taskID]; exists {
		s.cron.Remove(entryID)
		delete(s.taskEntries, taskID)
		slog.Default().Info("unscheduled task", "task_id", taskID)
	}
}

// RescheduleTask updates a task's schedule
func (s *Scheduler) RescheduleTask(task *models.Task) error {
	s.unscheduleTask(task.ID)

	if task.Enabled {
		return s.scheduleTask(task)
	}

	return nil
}

// executeTask executes a single task
func (s *Scheduler) executeTask(task *models.Task) error {
	slog.Default().Info("executing task", "task_id", task.ID, "task_name", task.Name, "task_type", task.Type)

	// Create root span for scheduled task execution
	ctx, span := tracing.StartSpan(context.Background(), fmt.Sprintf("scheduler.task.%s", task.Type))
	defer span.End()

	span.SetAttributes(
		attribute.Int64("task.id", task.ID),
		attribute.String("task.name", task.Name),
		attribute.String("task.type", string(task.Type)),
		attribute.String("task.schedule", task.Schedule))

	var err error
	switch task.Type {
	case models.TaskTypeSQL:
		err = s.executeSQLTask(ctx, task)
	case models.TaskTypeScrape:
		err = s.executeScrapeTask(ctx, task)
	default:
		err = fmt.Errorf("unknown task type: %s", task.Type)
	}

	// Update last run time
	now := time.Now()
	s.mu.RLock()
	entryID := s.taskEntries[task.ID]
	s.mu.RUnlock()

	var nextRun *time.Time
	if entryID != 0 {
		entry := s.cron.Entry(entryID)
		nextRun = &entry.Next
	}

	s.db.UpdateTaskRunTime(task.ID, now, nextRun)

	if err != nil {
		tracing.RecordError(ctx, err)
		return err
	}

	tracing.AddEvent(ctx, "task_completed",
		attribute.Int64("task.id", task.ID))
	slog.Default().Info("task completed successfully", "task_id", task.ID, "task_name", task.Name)
	return nil
}

// executeSQLTask executes a SQL task with whitelist validation
func (s *Scheduler) executeSQLTask(ctx context.Context, task *models.Task) error {
	ctx, span := tracing.StartSpan(ctx, "task.sql.execute")
	defer span.End()

	config, err := db.ParseSQLTaskConfig(task.Config)
	if err != nil {
		tracing.RecordError(ctx, err)
		return err
	}

	span.SetAttributes(
		attribute.String("sql.target_db", config.TargetDB))

	// Validate SQL against whitelist
	ctx, validateSpan := tracing.StartSpan(ctx, "sql.validate")
	if err := validateSQL(config.SQL); err != nil {
		tracing.RecordError(ctx, err)
		validateSpan.End()
		return fmt.Errorf("SQL validation failed: %w", err)
	}
	validateSpan.End()

	// Execute SQL on target database
	var targetDB *sql.DB
	switch config.TargetDB {
	case "controller":
		targetDB = s.controllerDB
	default:
		err := fmt.Errorf("unknown target database: %s", config.TargetDB)
		tracing.RecordError(ctx, err)
		return err
	}

	// Execute the SQL
	ctx, execSpan := tracing.StartSpan(ctx, "sql.exec")
	result, err := targetDB.Exec(config.SQL)
	if err != nil {
		tracing.RecordError(ctx, err)
		execSpan.End()
		return fmt.Errorf("SQL execution failed: %w", err)
	}

	// Add result metrics
	if rowsAffected, err := result.RowsAffected(); err == nil {
		execSpan.SetAttributes(attribute.Int64("sql.rows_affected", rowsAffected))
	}
	execSpan.End()

	return nil
}

// validateSQL validates SQL against a whitelist of allowed operations
func validateSQL(sql string) error {
	// Normalize SQL: remove extra whitespace and convert to lowercase
	normalized := strings.ToLower(strings.TrimSpace(sql))
	normalized = strings.Join(strings.Fields(normalized), " ")

	// Whitelist of allowed SQL patterns
	allowedPatterns := []string{
		// DELETE operations
		"delete from documents where",
		"delete from images where",
		"delete from scrape_requests where",

		// UPDATE operations
		"update documents set",
		"update images set",

		// INSERT operations (for data generation)
		"insert into documents",
		"insert into images",

		// Compound statements for cleanup
		"delete from documents where created_at <",
		"delete from images where created_at <",
		"delete from documents where tombstone_datetime is not null and tombstone_datetime <",
	}

	// Check if SQL starts with any allowed pattern
	for _, pattern := range allowedPatterns {
		if strings.HasPrefix(normalized, pattern) {
			return nil
		}
	}

	return fmt.Errorf("SQL does not match any allowed pattern")
}

// executeScrapeTask executes a scrape task
func (s *Scheduler) executeScrapeTask(ctx context.Context, task *models.Task) error {
	ctx, span := tracing.StartSpan(ctx, "task.scrape.execute")
	defer span.End()

	config, err := db.ParseScrapeTaskConfig(task.Config)
	if err != nil {
		tracing.RecordError(ctx, err)
		return err
	}

	span.SetAttributes(
		attribute.String("scrape.url", config.URL),
		attribute.Bool("scrape.extract_links", config.ExtractLinks))

	// Call controller API to create scrape request (async)
	scrapeURL := fmt.Sprintf("%s/api/scrape-requests", s.config.ControllerAPIURL)

	reqBody := map[string]interface{}{
		"url":           config.URL,
		"extract_links": config.ExtractLinks,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		tracing.RecordError(ctx, err)
		return fmt.Errorf("failed to marshal scrape request: %w", err)
	}

	ctx, httpSpan := tracing.StartSpan(ctx, "http.client.controller")
	httpSpan.SetAttributes(
		attribute.String("http.method", "POST"),
		attribute.String("http.url", scrapeURL))

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Post(scrapeURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		tracing.RecordError(ctx, err)
		httpSpan.End()
		return fmt.Errorf("failed to call controller API: %w", err)
	}
	defer resp.Body.Close()

	httpSpan.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("controller API returned status %d: %s", resp.StatusCode, string(bodyBytes))
		tracing.RecordError(ctx, err)
		httpSpan.End()
		return err
	}
	httpSpan.End()

	tracing.AddEvent(ctx, "scrape_request_created",
		attribute.String("url", config.URL))
	slog.Default().Info("successfully created scrape request", "url", config.URL, "extract_links", config.ExtractLinks)
	return nil
}
