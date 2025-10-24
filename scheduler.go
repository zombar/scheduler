package scheduler

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/zombar/scheduler/db"
	"github.com/zombar/scheduler/models"
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
			log.Printf("Failed to schedule task %d (%s): %v", task.ID, task.Name, err)
		}
	}

	// Start cron
	s.cron.Start()
	log.Println("Scheduler started")

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() error {
	ctx := s.cron.Stop()
	<-ctx.Done()

	if s.controllerDB != nil {
		s.controllerDB.Close()
	}

	log.Println("Scheduler stopped")
	return nil
}

// scheduleTask adds a task to the cron scheduler
func (s *Scheduler) scheduleTask(task *models.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse cron schedule
	entryID, err := s.cron.AddFunc(task.Schedule, func() {
		if err := s.executeTask(task); err != nil {
			log.Printf("Task %d (%s) execution failed: %v", task.ID, task.Name, err)
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

	log.Printf("Scheduled task %d (%s) with schedule %s, next run: %s",
		task.ID, task.Name, task.Schedule, nextRun.Format(time.RFC3339))

	return nil
}

// unscheduleTask removes a task from the cron scheduler
func (s *Scheduler) unscheduleTask(taskID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.taskEntries[taskID]; exists {
		s.cron.Remove(entryID)
		delete(s.taskEntries, taskID)
		log.Printf("Unscheduled task %d", taskID)
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
	log.Printf("Executing task %d (%s) of type %s", task.ID, task.Name, task.Type)

	var err error
	switch task.Type {
	case models.TaskTypeSQL:
		err = s.executeSQLTask(task)
	case models.TaskTypeScrape:
		err = s.executeScrapeTask(task)
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
		return err
	}

	log.Printf("Task %d (%s) completed successfully", task.ID, task.Name)
	return nil
}

// executeSQLTask executes a SQL task with whitelist validation
func (s *Scheduler) executeSQLTask(task *models.Task) error {
	config, err := db.ParseSQLTaskConfig(task.Config)
	if err != nil {
		return err
	}

	// Validate SQL against whitelist
	if err := validateSQL(config.SQL); err != nil {
		return fmt.Errorf("SQL validation failed: %w", err)
	}

	// Execute SQL on target database
	var targetDB *sql.DB
	switch config.TargetDB {
	case "controller":
		targetDB = s.controllerDB
	default:
		return fmt.Errorf("unknown target database: %s", config.TargetDB)
	}

	// Execute the SQL
	_, err = targetDB.Exec(config.SQL)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w", err)
	}

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
func (s *Scheduler) executeScrapeTask(task *models.Task) error {
	config, err := db.ParseScrapeTaskConfig(task.Config)
	if err != nil {
		return err
	}

	// Call controller API to create scrape request (async)
	scrapeURL := fmt.Sprintf("%s/api/scrape-requests", s.config.ControllerAPIURL)

	reqBody := map[string]interface{}{
		"url":           config.URL,
		"extract_links": config.ExtractLinks,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal scrape request: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Post(scrapeURL, "application/json", bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to call controller API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("controller API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Printf("Successfully created scrape request for URL: %s (extract_links: %v)", config.URL, config.ExtractLinks)
	return nil
}
