package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zombar/purpletab/pkg/tracing"
	"github.com/zombar/scheduler"
	"github.com/zombar/scheduler/api"
	"github.com/zombar/scheduler/db"
)

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	// Setup structured logging with JSON output
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("scheduler service initializing", "version", "1.0.0")

	// Initialize tracing
	tp, err := tracing.InitTracer("docutab-scheduler")
	if err != nil {
		logger.Warn("failed to initialize tracer, continuing without tracing", "error", err)
	} else {
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				logger.Error("error shutting down tracer", "error", err)
			}
		}()
		logger.Info("tracing initialized successfully")
	}

	// Default values
	defaultPort := getEnv("PORT", "8080")
	defaultDBPath := getEnv("DB_PATH", "scheduler.db")
	defaultControllerDBPath := getEnv("CONTROLLER_DB_PATH", "../controller/controller.db")
	defaultControllerURL := getEnv("CONTROLLER_BASE_URL", "http://localhost:8080")
	defaultScraperURL := getEnv("SCRAPER_URL", "http://localhost:8081")

	// Command-line flags (override environment variables)
	port := flag.String("port", defaultPort, "Server port")
	dbPath := flag.String("db", defaultDBPath, "Database file path")
	controllerDBPath := flag.String("controller-db", defaultControllerDBPath, "Controller database path")
	controllerURL := flag.String("controller-url", defaultControllerURL, "Controller API URL")
	scraperURL := flag.String("scraper-url", defaultScraperURL, "Scraper API URL")
	disableCORS := flag.Bool("disable-cors", false, "Disable CORS")
	flag.Parse()

	// Create server configuration
	config := api.Config{
		Addr: ":" + *port,
		DBConfig: db.Config{
			Driver: "sqlite",
			DSN:    *dbPath,
		},
		SchedulerConfig: scheduler.Config{
			ControllerDBPath: *controllerDBPath,
			ControllerAPIURL: *controllerURL,
			ScraperAPIURL:    *scraperURL,
		},
		CORSEnabled: !*disableCORS,
	}

	// Create server
	server, err := api.NewServer(config)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Start server in a goroutine
	go func() {
		logger.Info("scheduler service starting",
			"port", *port,
			"database", *dbPath,
			"controller_db_path", *controllerDBPath,
			"controller_url", *controllerURL,
			"scraper_url", *scraperURL,
		)

		if err := server.Start(); err != nil {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	logger.Info("shutting down gracefully")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server shutdown error", "error", err)
		os.Exit(1)
	}

	logger.Info("server stopped")
}
