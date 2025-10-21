package models

import "time"

// TaskType represents the type of scheduled task
type TaskType string

const (
	TaskTypeSQL    TaskType = "sql"
	TaskTypeScrape TaskType = "scrape"
)

// Task represents a scheduled task
type Task struct {
	ID          int64     `json:"id" db:"id"`
	Name        string    `json:"name" db:"name"`
	Description string    `json:"description" db:"description"`
	Type        TaskType  `json:"type" db:"type"`
	Schedule    string    `json:"schedule" db:"schedule"` // Cron expression
	Config      string    `json:"config" db:"config"`     // JSON config for the task
	Enabled     bool      `json:"enabled" db:"enabled"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
	LastRunAt   *time.Time `json:"last_run_at,omitempty" db:"last_run_at"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty" db:"next_run_at"`
}

// SQLTaskConfig contains configuration for SQL tasks
type SQLTaskConfig struct {
	SQL        string `json:"sql"`          // SQL query to execute
	TargetDB   string `json:"target_db"`    // Database to execute against (e.g., "controller")
}

// ScrapeTaskConfig contains configuration for scrape tasks
type ScrapeTaskConfig struct {
	URL             string `json:"url"`                         // URL to scrape
	ExtractLinks    bool   `json:"extract_links"`               // Whether to extract and scrape links
	ExtractLinkSQL  string `json:"extract_link_sql,omitempty"` // SQL query to get URLs to scrape from extracted links
}
