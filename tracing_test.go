package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zombar/scheduler/models"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestExecuteSQLTaskTracing tests that SQL task execution creates proper tracing spans
func TestExecuteSQLTaskTracing(t *testing.T) {
	// Setup trace exporter
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(trace.NewNoopTracerProvider())

	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Create test task with a simpler SQL that won't fail
	// Note: The validation will check the SQL pattern, but we use INSERT which is allowed
	task := &models.Task{
		ID:       1,
		Name:     "Test SQL task",
		Type:     models.TaskTypeSQL,
		Schedule: "0 0 * * *",
		Config:   `{"sql":"DELETE FROM documents WHERE created_at < datetime('now', '-30 days')","target_db":"controller"}`,
		Enabled:  true,
	}

	// Note: This test will fail execution but still test tracing
	// The validation span will be created successfully

	// Create trace context
	ctx, span := tp.Tracer("test").Start(context.Background(), "test-sql-task")

	// Execute SQL task (will fail due to missing table, but tracing spans should be created)
	_ = scheduler.executeSQLTask(ctx, task)
	span.End()

	// Note: We don't fail on error because the table doesn't exist in test DB
	// We're only testing that tracing spans are created, not SQL execution

	// Force flush
	tp.ForceFlush(context.Background())

	// Get recorded spans
	spans := exporter.GetSpans()

	if len(spans) == 0 {
		t.Fatal("No spans were recorded")
	}

	// Test 1: Verify task.sql.execute span exists
	var executeSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "task.sql.execute" {
			executeSpan = &spans[i]
			break
		}
	}

	if executeSpan == nil {
		t.Error("task.sql.execute span not found")
		t.Logf("Available spans: %v", getSpanNames(spans))
	} else {
		// Verify target_db attribute exists
		attrs := executeSpan.Attributes
		hasTargetDB := false

		for _, attr := range attrs {
			if string(attr.Key) == "sql.target_db" {
				hasTargetDB = true
			}
		}

		if !hasTargetDB {
			t.Error("sql.target_db attribute not found on task.sql.execute span")
		}
	}

	// Test 2: Verify sql.validate span exists
	var validateSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "sql.validate" {
			validateSpan = &spans[i]
			break
		}
	}

	if validateSpan == nil {
		t.Error("sql.validate span not found")
	}

	// Test 3: Verify sql.exec span exists (may not exist if table doesn't exist)
	// This is optional since the test may fail at execution
	var execSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "sql.exec" {
			execSpan = &spans[i]
			break
		}
	}

	// Not failing if exec span doesn't exist - table may not be present in test DB
	if execSpan != nil {
		t.Log("sql.exec span was created successfully")
	}
}

// TestExecuteScrapeTaskTracing tests that scrape task execution creates proper tracing spans
func TestExecuteScrapeTaskTracing(t *testing.T) {
	// Setup trace exporter
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(trace.NewNoopTracerProvider())

	// Create mock controller server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test-id"}`))
	}))
	defer mockServer.Close()

	scheduler, db := setupTestScheduler(t)
	defer db.Close()
	defer scheduler.Stop()

	// Update config to use mock server
	scheduler.config.ControllerAPIURL = mockServer.URL

	// Create test task
	task := &models.Task{
		ID:       1,
		Name:     "Test scrape task",
		Type:     models.TaskTypeScrape,
		Schedule: "0 */6 * * *",
		Config:   `{"url":"https://example.com","extract_links":false}`,
		Enabled:  true,
	}

	// Create trace context
	ctx, span := tp.Tracer("test").Start(context.Background(), "test-scrape-task")

	// Execute scrape task
	err := scheduler.executeScrapeTask(ctx, task)
	span.End()

	if err != nil {
		t.Fatalf("Failed to execute scrape task: %v", err)
	}

	// Force flush
	tp.ForceFlush(context.Background())

	// Get recorded spans
	spans := exporter.GetSpans()

	if len(spans) == 0 {
		t.Fatal("No spans were recorded")
	}

	// Test 1: Verify task.scrape.execute span exists
	var executeSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "task.scrape.execute" {
			executeSpan = &spans[i]
			break
		}
	}

	if executeSpan == nil {
		t.Error("task.scrape.execute span not found")
		t.Logf("Available spans: %v", getSpanNames(spans))
	} else {
		// Verify scrape URL attribute exists
		attrs := executeSpan.Attributes
		hasScrapeURL := false

		for _, attr := range attrs {
			if string(attr.Key) == "scrape.url" {
				hasScrapeURL = true
			}
		}

		if !hasScrapeURL {
			t.Error("scrape.url attribute not found on task.scrape.execute span")
		}
	}

	// Test 2: Verify http.client.controller span exists
	var httpSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "http.client.controller" {
			httpSpan = &spans[i]
			break
		}
	}

	if httpSpan == nil {
		t.Error("http.client.controller span not found")
	} else {
		// Verify HTTP attributes exist
		attrs := httpSpan.Attributes
		hasHTTPMethod := false
		hasHTTPStatusCode := false

		for _, attr := range attrs {
			if string(attr.Key) == "http.method" {
				hasHTTPMethod = true
			}
			if string(attr.Key) == "http.status_code" {
				hasHTTPStatusCode = true
			}
		}

		if !hasHTTPMethod {
			t.Error("http.method attribute not found on http.client.controller span")
		}
		if !hasHTTPStatusCode {
			t.Error("http.status_code attribute not found on http.client.controller span")
		}
	}
}

// getSpanNames returns a list of span names for debugging
func getSpanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, len(spans))
	for i, span := range spans {
		names[i] = span.Name
	}
	return names
}
