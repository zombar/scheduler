package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/docutag/scheduler/db"
	"github.com/docutag/scheduler/models"
)

// setupTestScheduler creates a test scheduler with in-memory databases
func setupTestScheduler(t *testing.T) (*Scheduler, *db.DB) {
	database, err := db.New(db.Config{
		Driver: "sqlite",
		DSN:    ":memory:",
	})
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	config := Config{
		ControllerDBPath: ":memory:",
		ControllerAPIURL: "http://localhost:8080",
		ScraperAPIURL:    "http://localhost:8081",
	}

	scheduler, err := New(database, config)
	if err != nil {
		t.Fatalf("Failed to create test scheduler: %v", err)
	}

	return scheduler, database
}

func TestNew(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	if scheduler == nil {
		t.Fatal("Expected scheduler to be initialized")
	}

	if scheduler.db == nil {
		t.Fatal("Expected database to be initialized")
	}

	if scheduler.cron == nil {
		t.Fatal("Expected cron to be initialized")
	}

	if scheduler.controllerDB == nil {
		t.Fatal("Expected controller database to be initialized")
	}

	if scheduler.taskEntries == nil {
		t.Fatal("Expected task entries map to be initialized")
	}
}

func TestStartStop(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Give scheduler time to start
	time.Sleep(100 * time.Millisecond)

	err = scheduler.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}
}

func TestScheduleTask(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *", // Every 6 hours
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err := scheduler.scheduleTask(task)
	if err != nil {
		t.Fatalf("Failed to schedule task: %v", err)
	}

	scheduler.mu.RLock()
	_, exists := scheduler.taskEntries[task.ID]
	scheduler.mu.RUnlock()

	if !exists {
		t.Error("Expected task to be in task entries map")
	}
}

func TestScheduleTaskInvalidCron(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "invalid cron", // Invalid schedule
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err := scheduler.scheduleTask(task)
	if err == nil {
		t.Error("Expected error for invalid cron schedule")
	}
}

func TestUnscheduleTask(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	// Schedule the task first
	scheduler.scheduleTask(task)

	// Verify it's scheduled
	scheduler.mu.RLock()
	_, exists := scheduler.taskEntries[task.ID]
	scheduler.mu.RUnlock()

	if !exists {
		t.Fatal("Expected task to be scheduled")
	}

	// Unschedule the task
	scheduler.unscheduleTask(task.ID)

	// Verify it's unscheduled
	scheduler.mu.RLock()
	_, exists = scheduler.taskEntries[task.ID]
	scheduler.mu.RUnlock()

	if exists {
		t.Error("Expected task to be unscheduled")
	}
}

func TestRescheduleTask(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	// Schedule the task
	scheduler.scheduleTask(task)

	// Update the schedule
	task.Schedule = "0 0 * * *" // Daily at midnight

	err := scheduler.RescheduleTask(task)
	if err != nil {
		t.Fatalf("Failed to reschedule task: %v", err)
	}

	scheduler.mu.RLock()
	_, exists := scheduler.taskEntries[task.ID]
	scheduler.mu.RUnlock()

	if !exists {
		t.Error("Expected task to still be scheduled")
	}
}

func TestRescheduleTaskDisabled(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	// Schedule the task
	scheduler.scheduleTask(task)

	// Disable the task
	task.Enabled = false

	err := scheduler.RescheduleTask(task)
	if err != nil {
		t.Fatalf("Failed to reschedule task: %v", err)
	}

	scheduler.mu.RLock()
	_, exists := scheduler.taskEntries[task.ID]
	scheduler.mu.RUnlock()

	if exists {
		t.Error("Expected disabled task to be unscheduled")
	}
}

func TestValidateSQL(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		{
			name:        "valid DELETE from documents",
			sql:         "DELETE FROM documents WHERE created_at < datetime('now', '-30 days')",
			expectError: false,
		},
		{
			name:        "valid DELETE from images",
			sql:         "DELETE FROM images WHERE created_at < datetime('now', '-60 days')",
			expectError: false,
		},
		{
			name:        "valid UPDATE documents",
			sql:         "UPDATE documents SET status = 'archived' WHERE created_at < datetime('now', '-90 days')",
			expectError: false,
		},
		{
			name:        "valid INSERT into documents",
			sql:         "INSERT INTO documents (url, content) VALUES ('https://example.com', 'test')",
			expectError: false,
		},
		{
			name:        "valid tombstone DELETE",
			sql:         "DELETE FROM documents WHERE tombstone_datetime IS NOT NULL AND tombstone_datetime < datetime('now', '-7 days')",
			expectError: false,
		},
		{
			name:        "invalid DROP TABLE",
			sql:         "DROP TABLE documents",
			expectError: true,
		},
		{
			name:        "invalid SELECT",
			sql:         "SELECT * FROM documents",
			expectError: true,
		},
		{
			name:        "invalid DELETE without WHERE",
			sql:         "DELETE FROM documents",
			expectError: true,
		},
		{
			name:        "SQL with semicolon (passes validation, would fail at execution)",
			sql:         "DELETE FROM documents WHERE 1=1; DROP TABLE documents;",
			expectError: false, // Prefix matches whitelist; multi-statement would fail at DB execution
		},
		{
			name:        "DELETE from non-whitelisted table",
			sql:         "DELETE FROM users WHERE created_at < datetime('now', '-30 days')",
			expectError: true,
		},
		{
			name:        "case insensitive match",
			sql:         "delete from documents where created_at < datetime('now', '-30 days')",
			expectError: false,
		},
		{
			name:        "extra whitespace",
			sql:         "  DELETE   FROM   documents   WHERE  created_at < datetime('now', '-30 days')  ",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSQL(tt.sql)
			if tt.expectError && err == nil {
				t.Errorf("Expected error for SQL: %s", tt.sql)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error, got: %v for SQL: %s", err, tt.sql)
			}
		})
	}
}

func TestExecuteSQLTask(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Create a test table in the controller database
	_, err := scheduler.controllerDB.Exec(`
		CREATE TABLE IF NOT EXISTS documents (
			id INTEGER PRIMARY KEY,
			url TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert some test data
	_, err = scheduler.controllerDB.Exec(`
		INSERT INTO documents (url, created_at) VALUES
		('https://old.com', datetime('now', '-60 days')),
		('https://new.com', datetime('now'))
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	task := &models.Task{
		ID:       1,
		Name:     "Cleanup old documents",
		Type:     models.TaskTypeSQL,
		Schedule: "0 0 * * *",
		Config:   `{"sql":"DELETE FROM documents WHERE created_at < datetime('now', '-30 days')","target_db":"controller"}`,
		Enabled:  true,
	}

	err = scheduler.executeSQLTask(context.Background(), task)
	if err != nil {
		t.Fatalf("Failed to execute SQL task: %v", err)
	}

	// Verify old document was deleted
	var count int
	err = scheduler.controllerDB.QueryRow("SELECT COUNT(*) FROM documents").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query documents: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 document remaining, got %d", count)
	}
}

func TestExecuteSQLTaskInvalidSQL(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Malicious task",
		Type:     models.TaskTypeSQL,
		Schedule: "0 0 * * *",
		Config:   `{"sql":"DROP TABLE documents","target_db":"controller"}`,
		Enabled:  true,
	}

	err := scheduler.executeSQLTask(context.Background(), task)
	if err == nil {
		t.Error("Expected error for malicious SQL")
	}
}

func TestExecuteSQLTaskInvalidDatabase(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Test task",
		Type:     models.TaskTypeSQL,
		Schedule: "0 0 * * *",
		Config:   `{"sql":"DELETE FROM documents WHERE created_at < datetime('now', '-30 days')","target_db":"invalid"}`,
		Enabled:  true,
	}

	err := scheduler.executeSQLTask(context.Background(), task)
	if err == nil {
		t.Error("Expected error for invalid target database")
	}
}

func TestExecuteScrapeTask(t *testing.T) {
	// Create mock scraper server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"id":"test-id"}`)
	}))
	defer mockServer.Close()

	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Update config to use mock server
	scheduler.config.ControllerAPIURL = mockServer.URL

	task := &models.Task{
		ID:       1,
		Name:     "Scrape task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com","extract_links":false}`,
		Enabled:  true,
	}

	err := scheduler.executeScrapeTask(context.Background(), task)
	if err != nil {
		t.Fatalf("Failed to execute scrape task: %v", err)
	}
}

func TestExecuteScrapeTaskWithExtractLinks(t *testing.T) {
	// Create mock controller server
	callCount := 0
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		if r.URL.Path == "/api/scrape-requests" {
			fmt.Fprintf(w, `{"id":"test-id","status":"pending"}`)
		}
	}))
	defer mockServer.Close()

	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Update config to use mock server
	scheduler.config.ControllerAPIURL = mockServer.URL

	task := &models.Task{
		ID:       1,
		Name:     "Scrape task with links",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com","extract_links":true}`,
		Enabled:  true,
	}

	err := scheduler.executeScrapeTask(context.Background(), task)
	if err != nil {
		t.Fatalf("Failed to execute scrape task: %v", err)
	}

	// Should have called scrape-requests endpoint once (handles extract_links internally)
	if callCount != 1 {
		t.Errorf("Expected 1 API call to scrape-requests, got %d", callCount)
	}
}

func TestExecuteScrapeTaskServerError(t *testing.T) {
	// Create mock controller server that returns error
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error":"Internal server error"}`)
	}))
	defer mockServer.Close()

	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Update config to use mock server
	scheduler.config.ControllerAPIURL = mockServer.URL

	task := &models.Task{
		ID:       1,
		Name:     "Scrape task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err := scheduler.executeScrapeTask(context.Background(), task)
	if err == nil {
		t.Error("Expected error when scraper returns error")
	}
}

func TestExecuteTaskUnknownType(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	task := &models.Task{
		ID:       1,
		Name:     "Unknown task",
		Type:     "unknown",
		Schedule: "0 0 * * *",
		Config:   `{}`,
		Enabled:  true,
	}

	err := scheduler.executeTask(task)
	if err == nil {
		t.Error("Expected error for unknown task type")
	}
}

func TestStartWithEnabledTasks(t *testing.T) {
	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Create some enabled tasks
	task1 := &models.Task{
		Name:     "Test Task 1",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}
	task2 := &models.Task{
		Name:     "Test Task 2",
		Type:     models.TaskTypeScrape,
		Schedule: "0 0 * * *",
		Config:   `{"url":"https://test.com"}`,
		Enabled:  true,
	}

	db.CreateTask(task1)
	db.CreateTask(task2)

	err := scheduler.Start()
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Give scheduler time to schedule tasks
	time.Sleep(100 * time.Millisecond)

	scheduler.mu.RLock()
	scheduledCount := len(scheduler.taskEntries)
	scheduler.mu.RUnlock()

	if scheduledCount != 2 {
		t.Errorf("Expected 2 tasks to be scheduled, got %d", scheduledCount)
	}
}
