package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"      // PostgreSQL driver
	_ "modernc.org/sqlite"     // SQLite driver
	"github.com/zombar/scheduler/models"
)

// Config contains database configuration
type Config struct {
	Driver string
	DSN    string
}

// DB wraps database operations
type DB struct {
	db *sql.DB
}

// New creates a new database connection
func New(config Config) (*DB, error) {
	db, err := sql.Open(config.Driver, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	d := &DB{db: db}

	// Run migrations based on driver type
	if config.Driver == "postgres" {
		if err := d.migratePostgres(); err != nil {
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
	} else {
		if err := d.migrate(); err != nil {
			return nil, fmt.Errorf("failed to run migrations: %w", err)
		}
	}

	return d, nil
}

// migrate runs database migrations
func (d *DB) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL CHECK(type IN ('sql', 'scrape')),
		schedule TEXT NOT NULL,
		config TEXT NOT NULL DEFAULT '{}',
		enabled INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_run_at TIMESTAMP,
		next_run_at TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_enabled ON tasks(enabled);
	CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at ON tasks(next_run_at);
	`

	_, err := d.db.Exec(schema)
	return err
}

// Close closes the database connection
func (d *DB) Close() error {
	return d.db.Close()
}

// DB returns the underlying database connection for metrics collection
func (d *DB) DB() *sql.DB {
	return d.db
}

// CreateTask creates a new task
func (d *DB) CreateTask(task *models.Task) error {
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now

	result, err := d.db.Exec(`
		INSERT INTO tasks (name, description, type, schedule, config, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, task.Name, task.Description, task.Type, task.Schedule, task.Config, task.Enabled, task.CreatedAt, task.UpdatedAt)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}

	task.ID = id
	return nil
}

// GetTask retrieves a task by ID
func (d *DB) GetTask(id int64) (*models.Task, error) {
	task := &models.Task{}
	err := d.db.QueryRow(`
		SELECT id, name, description, type, schedule, config, enabled,
		       created_at, updated_at, last_run_at, next_run_at
		FROM tasks WHERE id = ?
	`, id).Scan(
		&task.ID, &task.Name, &task.Description, &task.Type, &task.Schedule,
		&task.Config, &task.Enabled, &task.CreatedAt, &task.UpdatedAt,
		&task.LastRunAt, &task.NextRunAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return task, nil
}

// ListTasks retrieves all tasks
func (d *DB) ListTasks() ([]*models.Task, error) {
	rows, err := d.db.Query(`
		SELECT id, name, description, type, schedule, config, enabled,
		       created_at, updated_at, last_run_at, next_run_at
		FROM tasks
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		task := &models.Task{}
		err := rows.Scan(
			&task.ID, &task.Name, &task.Description, &task.Type, &task.Schedule,
			&task.Config, &task.Enabled, &task.CreatedAt, &task.UpdatedAt,
			&task.LastRunAt, &task.NextRunAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

// UpdateTask updates an existing task
func (d *DB) UpdateTask(task *models.Task) error {
	task.UpdatedAt = time.Now()

	result, err := d.db.Exec(`
		UPDATE tasks
		SET name = ?, description = ?, type = ?, schedule = ?, config = ?,
		    enabled = ?, updated_at = ?, next_run_at = ?
		WHERE id = ?
	`, task.Name, task.Description, task.Type, task.Schedule, task.Config,
		task.Enabled, task.UpdatedAt, task.NextRunAt, task.ID)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("task not found")
	}

	return nil
}

// DeleteTask deletes a task
func (d *DB) DeleteTask(id int64) error {
	result, err := d.db.Exec("DELETE FROM tasks WHERE id = ?", id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("task not found")
	}

	return nil
}

// UpdateTaskRunTime updates the last and next run times for a task
func (d *DB) UpdateTaskRunTime(id int64, lastRun time.Time, nextRun *time.Time) error {
	_, err := d.db.Exec(`
		UPDATE tasks
		SET last_run_at = ?, next_run_at = ?, updated_at = ?
		WHERE id = ?
	`, lastRun, nextRun, time.Now(), id)

	return err
}

// GetEnabledTasks retrieves all enabled tasks
func (d *DB) GetEnabledTasks() ([]*models.Task, error) {
	rows, err := d.db.Query(`
		SELECT id, name, description, type, schedule, config, enabled,
		       created_at, updated_at, last_run_at, next_run_at
		FROM tasks
		WHERE enabled = true
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*models.Task
	for rows.Next() {
		task := &models.Task{}
		err := rows.Scan(
			&task.ID, &task.Name, &task.Description, &task.Type, &task.Schedule,
			&task.Config, &task.Enabled, &task.CreatedAt, &task.UpdatedAt,
			&task.LastRunAt, &task.NextRunAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

// ParseSQLTaskConfig parses SQL task configuration from JSON
func ParseSQLTaskConfig(configJSON string) (*models.SQLTaskConfig, error) {
	var config models.SQLTaskConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, fmt.Errorf("failed to parse SQL task config: %w", err)
	}
	return &config, nil
}

// ParseScrapeTaskConfig parses scrape task configuration from JSON
func ParseScrapeTaskConfig(configJSON string) (*models.ScrapeTaskConfig, error) {
	var config models.ScrapeTaskConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, fmt.Errorf("failed to parse scrape task config: %w", err)
	}
	return &config, nil
}
