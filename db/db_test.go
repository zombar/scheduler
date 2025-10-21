package db

import (
	"testing"
	"time"

	"github.com/zombar/scheduler/models"
)

func TestNew(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Expected database to be initialized")
	}
}

func TestCreateTask(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task := &models.Task{
		Name:        "Test Task",
		Description: "Test Description",
		Type:        models.TaskTypeScrape,
		Schedule:    "0 */6 * * *",
		Config:      `{"url":"https://example.com"}`,
		Enabled:     true,
	}

	err = db.CreateTask(task)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	if task.ID == 0 {
		t.Error("Expected task ID to be set")
	}

	if task.CreatedAt.IsZero() {
		t.Error("Expected CreatedAt to be set")
	}

	if task.UpdatedAt.IsZero() {
		t.Error("Expected UpdatedAt to be set")
	}
}

func TestGetTask(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task := &models.Task{
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err = db.CreateTask(task)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	retrieved, err := db.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected task to be retrieved")
	}

	if retrieved.Name != task.Name {
		t.Errorf("Expected name %s, got %s", task.Name, retrieved.Name)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task, err := db.GetTask(999)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if task != nil {
		t.Error("Expected task to be nil for non-existent ID")
	}
}

func TestListTasks(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create multiple tasks
	for i := 0; i < 3; i++ {
		task := &models.Task{
			Name:     "Test Task",
			Type:     models.TaskTypeScrape,
			Schedule: "0 */6 * * *",
			Config:   `{"url":"https://example.com"}`,
			Enabled:  true,
		}
		err = db.CreateTask(task)
		if err != nil {
			t.Fatalf("Failed to create task: %v", err)
		}
	}

	tasks, err := db.ListTasks()
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}

	if len(tasks) != 3 {
		t.Errorf("Expected 3 tasks, got %d", len(tasks))
	}
}

func TestUpdateTask(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task := &models.Task{
		Name:     "Original Name",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err = db.CreateTask(task)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	// Update the task
	task.Name = "Updated Name"
	task.Enabled = false

	err = db.UpdateTask(task)
	if err != nil {
		t.Fatalf("Failed to update task: %v", err)
	}

	// Retrieve and verify
	retrieved, err := db.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved.Name != "Updated Name" {
		t.Errorf("Expected name to be updated to 'Updated Name', got %s", retrieved.Name)
	}

	if retrieved.Enabled {
		t.Error("Expected Enabled to be false")
	}
}

func TestDeleteTask(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task := &models.Task{
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err = db.CreateTask(task)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	err = db.DeleteTask(task.ID)
	if err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}

	// Verify task is deleted
	retrieved, err := db.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved != nil {
		t.Error("Expected task to be nil after deletion")
	}
}

func TestUpdateLastRun(t *testing.T) {
	db, err := New(Config{Driver: "sqlite3", DSN: ":memory:"})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	task := &models.Task{
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}

	err = db.CreateTask(task)
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	now := time.Now()
	nextRun := now.Add(6 * time.Hour)

	err = db.UpdateTaskRunTime(task.ID, now, &nextRun)
	if err != nil {
		t.Fatalf("Failed to update task run time: %v", err)
	}

	// Retrieve and verify
	retrieved, err := db.GetTask(task.ID)
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	if retrieved.LastRunAt == nil {
		t.Fatal("Expected LastRunAt to be set")
	}

	if retrieved.NextRunAt == nil {
		t.Fatal("Expected NextRunAt to be set")
	}

	// Allow for slight time differences due to database precision
	if retrieved.LastRunAt.Sub(now).Abs() > time.Second {
		t.Errorf("Expected LastRunAt to be close to %v, got %v", now, *retrieved.LastRunAt)
	}

	if retrieved.NextRunAt.Sub(nextRun).Abs() > time.Second {
		t.Errorf("Expected NextRunAt to be close to %v, got %v", nextRun, *retrieved.NextRunAt)
	}
}

func TestParseSQLTaskConfig(t *testing.T) {
	config := `{"sql":"DELETE FROM documents WHERE created_at < datetime('now', '-30 days')","target_db":"controller.db"}`

	sqlConfig, err := ParseSQLTaskConfig(config)
	if err != nil {
		t.Fatalf("Failed to parse SQL task config: %v", err)
	}

	if sqlConfig.SQL == "" {
		t.Error("Expected SQL to be set")
	}

	if sqlConfig.TargetDB != "controller.db" {
		t.Errorf("Expected TargetDB to be 'controller.db', got %s", sqlConfig.TargetDB)
	}
}

func TestParseScrapeTaskConfig(t *testing.T) {
	config := `{"url":"https://example.com","extract_links":true}`

	scrapeConfig, err := ParseScrapeTaskConfig(config)
	if err != nil {
		t.Fatalf("Failed to parse scrape task config: %v", err)
	}

	if scrapeConfig.URL != "https://example.com" {
		t.Errorf("Expected URL to be 'https://example.com', got %s", scrapeConfig.URL)
	}

	if !scrapeConfig.ExtractLinks {
		t.Error("Expected ExtractLinks to be true")
	}
}
