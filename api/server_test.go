package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zombar/scheduler"
	"github.com/zombar/scheduler/db"
	"github.com/zombar/scheduler/models"
)

// setupTestServer creates a test server with in-memory database
func setupTestServer(t *testing.T) *Server {
	config := Config{
		Addr: ":8083",
		DBConfig: db.Config{
			Driver: "sqlite",
			DSN:    ":memory:",
		},
		SchedulerConfig: scheduler.Config{
			ControllerDBPath: ":memory:",
			ScraperAPIURL:    "http://localhost:8081",
		},
		CORSEnabled: false,
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}

	return server
}

func TestNewServer(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	if server == nil {
		t.Fatal("Expected server to be initialized")
	}

	if server.db == nil {
		t.Fatal("Expected database to be initialized")
	}

	if server.scheduler == nil {
		t.Fatal("Expected scheduler to be initialized")
	}
}

func TestHandleHealth(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	server.handleHealth(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, status)
	}

	var response map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response["status"])
	}
}

func TestHandleListTasks(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	// Create some test tasks
	task1 := &models.Task{
		Name:     "Test Task 1",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}
	task2 := &models.Task{
		Name:     "Test Task 2",
		Type:     models.TaskTypeSQL,
		Schedule: "0 0 * * *",
		Config:   `{"sql":"DELETE FROM old_data","target_db":"controller.db"}`,
		Enabled:  false,
	}

	server.db.CreateTask(task1)
	server.db.CreateTask(task2)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	rr := httptest.NewRecorder()

	server.handleTasks(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, status)
	}

	var tasks []*models.Task
	if err := json.NewDecoder(rr.Body).Decode(&tasks); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(tasks) != 2 {
		t.Errorf("Expected 2 tasks, got %d", len(tasks))
	}
}

func TestHandleCreateTask(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	task := models.Task{
		Name:        "New Task",
		Description: "Test description",
		Type:        models.TaskTypeScrape,
		Schedule:    "0 */6 * * *",
		Config:      `{"url":"https://example.com","extract_links":true}`,
		Enabled:     true,
	}

	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	server.handleTasks(rr, req)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("Expected status code %d, got %d", http.StatusCreated, status)
	}

	var createdTask models.Task
	if err := json.NewDecoder(rr.Body).Decode(&createdTask); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if createdTask.ID == 0 {
		t.Error("Expected task ID to be set")
	}

	if createdTask.Name != task.Name {
		t.Errorf("Expected name '%s', got '%s'", task.Name, createdTask.Name)
	}
}

func TestHandleCreateTaskValidation(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	tests := []struct {
		name       string
		task       models.Task
		expectCode int
	}{
		{
			name: "missing name",
			task: models.Task{
				Type:     models.TaskTypeScrape,
				Schedule: "0 */6 * * *",
				Config:   `{"url":"https://example.com"}`,
			},
			expectCode: http.StatusBadRequest,
		},
		{
			name: "invalid type",
			task: models.Task{
				Name:     "Test",
				Type:     "invalid",
				Schedule: "0 */6 * * *",
				Config:   `{"url":"https://example.com"}`,
			},
			expectCode: http.StatusBadRequest,
		},
		{
			name: "missing schedule",
			task: models.Task{
				Name:   "Test",
				Type:   models.TaskTypeScrape,
				Config: `{"url":"https://example.com"}`,
			},
			expectCode: http.StatusBadRequest,
		},
		{
			name: "missing URL for scrape task",
			task: models.Task{
				Name:     "Test",
				Type:     models.TaskTypeScrape,
				Schedule: "0 */6 * * *",
				Config:   `{}`,
			},
			expectCode: http.StatusBadRequest,
		},
		{
			name: "missing SQL for SQL task",
			task: models.Task{
				Name:     "Test",
				Type:     models.TaskTypeSQL,
				Schedule: "0 */6 * * *",
				Config:   `{"target_db":"controller.db"}`,
			},
			expectCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.task)
			req := httptest.NewRequest(http.MethodPost, "/api/tasks", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()

			server.handleTasks(rr, req)

			if status := rr.Code; status != tt.expectCode {
				t.Errorf("Expected status code %d, got %d", tt.expectCode, status)
			}
		})
	}
}

func TestHandleGetTask(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	// Create a task
	task := &models.Task{
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}
	server.db.CreateTask(task)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/1", nil)
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, status)
	}

	var retrieved models.Task
	if err := json.NewDecoder(rr.Body).Decode(&retrieved); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if retrieved.Name != task.Name {
		t.Errorf("Expected name '%s', got '%s'", task.Name, retrieved.Name)
	}
}

func TestHandleGetTaskNotFound(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/999", nil)
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, status)
	}
}

func TestHandleUpdateTask(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	// Create a task
	task := &models.Task{
		Name:     "Original Name",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}
	server.db.CreateTask(task)

	// Update the task
	updatedTask := models.Task{
		Name:     "Updated Name",
		Type:     models.TaskTypeScrape,
		Schedule: "0 0 * * *",
		Config:   `{"url":"https://updated.com"}`,
		Enabled:  false,
	}

	body, _ := json.Marshal(updatedTask)
	req := httptest.NewRequest(http.MethodPut, "/api/tasks/1", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, status)
	}

	var result models.Task
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result.Name != "Updated Name" {
		t.Errorf("Expected name 'Updated Name', got '%s'", result.Name)
	}
}

func TestHandleUpdateTaskNotFound(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	task := models.Task{
		Name:     "Test",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
	}

	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPut, "/api/tasks/999", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, status)
	}
}

func TestHandleDeleteTask(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	// Create a task
	task := &models.Task{
		Name:     "Test Task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com"}`,
		Enabled:  true,
	}
	server.db.CreateTask(task)

	req := httptest.NewRequest(http.MethodDelete, "/api/tasks/1", nil)
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusNoContent {
		t.Errorf("Expected status code %d, got %d", http.StatusNoContent, status)
	}

	// Verify task is deleted
	retrieved, _ := server.db.GetTask(1)
	if retrieved != nil {
		t.Error("Expected task to be deleted")
	}
}

func TestHandleDeleteTaskNotFound(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	req := httptest.NewRequest(http.MethodDelete, "/api/tasks/999", nil)
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, status)
	}
}

func TestHandleTasksMethodNotAllowed(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	req := httptest.NewRequest(http.MethodPatch, "/api/tasks", nil)
	rr := httptest.NewRecorder()

	server.handleTasks(rr, req)

	if status := rr.Code; status != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d, got %d", http.StatusMethodNotAllowed, status)
	}
}

func TestHandleTaskByIDInvalidID(t *testing.T) {
	server := setupTestServer(t)
	defer server.db.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/invalid", nil)
	rr := httptest.NewRecorder()

	server.handleTaskByID(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, status)
	}
}

func TestCORSMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsHandler := corsMiddleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	corsHandler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Expected CORS header to be set")
	}
}

func TestCORSMiddlewareOptions(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	corsHandler := corsMiddleware(handler)

	req := httptest.NewRequest(http.MethodOptions, "/test", nil)
	rr := httptest.NewRecorder()

	corsHandler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Expected status code %d for OPTIONS request, got %d", http.StatusOK, status)
	}
}

func TestValidateTask(t *testing.T) {
	tests := []struct {
		name        string
		task        *models.Task
		expectError bool
	}{
		{
			name: "valid scrape task",
			task: &models.Task{
				Name:     "Test",
				Type:     models.TaskTypeScrape,
				Schedule: "0 */6 * * *",
				Config:   `{"url":"https://example.com"}`,
			},
			expectError: false,
		},
		{
			name: "valid SQL task",
			task: &models.Task{
				Name:     "Test",
				Type:     models.TaskTypeSQL,
				Schedule: "0 0 * * *",
				Config:   `{"sql":"DELETE FROM old","target_db":"controller.db"}`,
			},
			expectError: false,
		},
		{
			name: "missing name",
			task: &models.Task{
				Type:     models.TaskTypeScrape,
				Schedule: "0 */6 * * *",
				Config:   `{"url":"https://example.com"}`,
			},
			expectError: true,
		},
		{
			name: "invalid type",
			task: &models.Task{
				Name:     "Test",
				Type:     "invalid",
				Schedule: "0 */6 * * *",
				Config:   `{"url":"https://example.com"}`,
			},
			expectError: true,
		},
		{
			name: "missing schedule",
			task: &models.Task{
				Name:   "Test",
				Type:   models.TaskTypeScrape,
				Config: `{"url":"https://example.com"}`,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTask(tt.task)
			if tt.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}
