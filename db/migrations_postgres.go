package db

import (
	"fmt"
	"log"
)

// Migration represents a database migration
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// postgresMigrations contains all PostgreSQL-specific migrations
var postgresMigrations = []Migration{
	{
		Version: 1,
		Name:    "create_tasks_table",
		SQL: `
			CREATE TABLE IF NOT EXISTS tasks (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				type TEXT NOT NULL CHECK(type IN ('sql', 'scrape')),
				schedule TEXT NOT NULL,
				config JSONB NOT NULL DEFAULT '{}',
				enabled BOOLEAN NOT NULL DEFAULT true,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				last_run_at TIMESTAMPTZ,
				next_run_at TIMESTAMPTZ
			);

			CREATE INDEX IF NOT EXISTS idx_tasks_enabled ON tasks(enabled);
			CREATE INDEX IF NOT EXISTS idx_tasks_next_run_at ON tasks(next_run_at);
		`,
	},
	{
		Version: 2,
		Name:    "create_schema_version_table",
		SQL: `
			CREATE TABLE IF NOT EXISTS schema_version (
				version INTEGER PRIMARY KEY,
				applied_at TIMESTAMPTZ DEFAULT NOW()
			);
		`,
	},
}

// migratePostgres runs PostgreSQL-specific database migrations
func (d *DB) migratePostgres() error {
	log.Println("Creating schema_version table...")
	// Ensure schema_version table exists
	if _, err := d.db.Exec(postgresMigrations[1].SQL); err != nil {
		return fmt.Errorf("failed to create schema_version table: %w", err)
	}

	log.Println("Checking current schema version...")
	// Get current version
	var currentVersion int
	err := d.db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM schema_version").Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("failed to get current version: %w", err)
	}
	log.Printf("Current schema version: %d", currentVersion)

	// Run pending migrations
	for _, migration := range postgresMigrations {
		if migration.Version <= currentVersion {
			log.Printf("Skipping migration %d (already applied)", migration.Version)
			continue
		}

		log.Printf("Applying migration %d: %s", migration.Version, migration.Name)
		tx, err := d.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", migration.Version, err)
		}

		// Execute migration SQL
		if _, err := tx.Exec(migration.SQL); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to execute migration %d (%s): %w", migration.Version, migration.Name, err)
		}

		// Record migration (use PostgreSQL $1 placeholder instead of ?)
		if _, err := tx.Exec("INSERT INTO schema_version (version) VALUES ($1)", migration.Version); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to record migration %d: %w", migration.Version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", migration.Version, err)
		}

		log.Printf("✓ Applied migration %d: %s", migration.Version, migration.Name)
	}

	log.Println("All migrations complete")
	return nil
}
