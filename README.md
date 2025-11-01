# Scheduler Service

[![Go Report Card](https://goreportcard.com/badge/github.com/docutag/platform)](https://goreportcard.com/report/github.com/docutag/platform)
[![Go Version](https://img.shields.io/github/go-mod/go-version/docutag/platform)](go.mod)

Manages scheduled tasks for DocuTag using cron expressions.

## Task Types

### SQL Tasks
Execute SQL operations on the controller database with whitelist validation.

**Allowed Operations:**
- `DELETE FROM documents WHERE ...`
- `DELETE FROM images WHERE ...`
- `UPDATE documents SET ...`
- `UPDATE images SET ...`
- `INSERT INTO documents ...`

**Configuration:**
```json
{
  "sql": "DELETE FROM documents WHERE created_at < datetime('now', '-30 days')",
  "target_db": "controller"
}
```

### Scrape Tasks
Schedule routine scrapes with optional link extraction. Creates async scrape requests via the controller that are visible in the UI and persist for 24 hours.

**Configuration:**
```json
{
  "url": "https://example.com",
  "extract_links": true
}
```

**Behavior:**
- Scrape requests appear in the web UI's scrape requests list
- Processed asynchronously by the controller
- Progress tracked and visible to users
- Results persist for 24 hours before auto-expiration

## API Endpoints

### Tasks
- `GET /api/tasks` - List all tasks
- `POST /api/tasks` - Create task
- `GET /api/tasks/{id}` - Get task
- `PUT /api/tasks/{id}` - Update task
- `DELETE /api/tasks/{id}` - Delete task

### Health
- `GET /health` - Service health check

## Request Format

**Create Task:**
```json
{
  "name": "Daily Cleanup",
  "description": "Remove old documents",
  "type": "sql",
  "schedule": "0 2 * * *",
  "config": "{\"sql\":\"DELETE FROM documents WHERE created_at < datetime('now', '-30 days')\",\"target_db\":\"controller\"}",
  "enabled": true
}
```

**Schedule Format:**
Standard cron expressions (minute hour day month weekday)
- `*/5 * * * *` - Every 5 minutes
- `0 */2 * * *` - Every 2 hours
- `0 2 * * *` - Daily at 2 AM
- `0 0 * * 0` - Weekly on Sunday

## Environment Variables

- `PORT` - Server port (default: 8080)
- `DB_PATH` - Scheduler database path
- `CONTROLLER_DB_PATH` - Controller database path (for SQL tasks)
- `CONTROLLER_BASE_URL` - Controller API URL (default: http://localhost:8080)
- `SCRAPER_URL` - Scraper service URL (legacy, kept for compatibility)

## Running

```bash
# Build
make build

# Run
./scheduler-api -port 8080 -db scheduler.db

# Docker
docker compose up scheduler
```

## Database

PostgreSQL database storing task definitions and execution history.

**Schema:**
- `id` - Task ID
- `name` - Task name
- `type` - Task type (sql/scrape)
- `schedule` - Cron expression
- `config` - JSON configuration
- `enabled` - Execution enabled
- `last_run_at` - Last execution time
- `next_run_at` - Next scheduled execution

The shared database package (`pkg/database`) provides connection pooling and OpenTelemetry instrumentation.
