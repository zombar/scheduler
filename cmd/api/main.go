package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	// Default values
	defaultPort := getEnv("PORT", "8080")
	defaultDBPath := getEnv("DB_PATH", "scheduler.db")
	defaultControllerDBPath := getEnv("CONTROLLER_DB_PATH", "../controller/controller.db")
	defaultScraperURL := getEnv("SCRAPER_URL", "http://localhost:8081")

	// Command-line flags (override environment variables)
	port := flag.String("port", defaultPort, "Server port")
	dbPath := flag.String("db", defaultDBPath, "Database file path")
	controllerDBPath := flag.String("controller-db", defaultControllerDBPath, "Controller database path")
	scraperURL := flag.String("scraper-url", defaultScraperURL, "Scraper API URL")
	disableCORS := flag.Bool("disable-cors", false, "Disable CORS")
	flag.Parse()

	// Create server configuration
	config := api.Config{
		Addr: ":" + *port,
		DBConfig: db.Config{
			Driver: "sqlite3",
			DSN:    *dbPath,
		},
		SchedulerConfig: scheduler.Config{
			ControllerDBPath: *controllerDBPath,
			ScraperAPIURL:    *scraperURL,
		},
		CORSEnabled: !*disableCORS,
	}

	// Create server
	server, err := api.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Graceful shutdown
	fmt.Println("\nShutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown error: %v", err)
	}

	fmt.Println("Server stopped")
}
