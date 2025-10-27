package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/zombar/purpletab/pkg/metrics"
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
	defaultDBHost := getEnv("DB_HOST", "localhost")
	defaultDBPort := getEnv("DB_PORT", "5432")
	defaultDBUser := getEnv("DB_USER", "docutab")
	defaultDBPassword := getEnv("DB_PASSWORD", "docutab_dev_pass")
	defaultDBName := getEnv("DB_NAME", "docutab")
	defaultControllerURL := getEnv("CONTROLLER_BASE_URL", "http://localhost:8080")
	defaultScraperURL := getEnv("SCRAPER_URL", "http://localhost:8081")

	// Command-line flags (override environment variables)
	port := flag.String("port", defaultPort, "Server port")
	dbHost := flag.String("db-host", defaultDBHost, "PostgreSQL host")
	dbPort := flag.String("db-port", defaultDBPort, "PostgreSQL port")
	dbUser := flag.String("db-user", defaultDBUser, "PostgreSQL user")
	dbPassword := flag.String("db-password", defaultDBPassword, "PostgreSQL password")
	dbName := flag.String("db-name", defaultDBName, "PostgreSQL database name")
	controllerURL := flag.String("controller-url", defaultControllerURL, "Controller API URL")
	scraperURL := flag.String("scraper-url", defaultScraperURL, "Scraper API URL")
	disableCORS := flag.Bool("disable-cors", false, "Disable CORS")
	flag.Parse()

	// Construct PostgreSQL connection string
	dbDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		*dbHost, *dbPort, *dbUser, *dbPassword, *dbName)

	// Create server configuration
	config := api.Config{
		Addr: ":" + *port,
		DBConfig: db.Config{
			Driver: "postgres",
			DSN:    dbDSN,
		},
		SchedulerConfig: scheduler.Config{
			ControllerDBPath: "", // No longer needed - scheduler uses same PostgreSQL database
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

	// Initialize database metrics
	dbMetrics := metrics.NewDatabaseMetrics("scheduler")
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			dbMetrics.UpdateDBStats(server.DB().DB())
		}
	}()
	logger.Info("database metrics initialized")

	// Start server in a goroutine
	go func() {
		logger.Info("scheduler service starting",
			"port", *port,
			"db_host", *dbHost,
			"db_name", *dbName,
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
