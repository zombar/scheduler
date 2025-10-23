# Scheduler Service

[![Go Report Card](https://goreportcard.com/badge/github.com/zombar/purpletab)](https://goreportcard.com/report/github.com/zombar/purpletab)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zombar/purpletab)](go.mod)

Manages scheduled tasks for PurpleTab using cron expressions.

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
Schedule routine scrapes with optional link extraction.

**Configuration:**
```json
{
  "url": "https://example.com",
  "extract_links": true
}
```

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
- `SCRAPER_URL` - Scraper service URL

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

SQLite database storing task definitions and execution history.

**Schema:**
- `id` - Task ID
- `name` - Task name
- `type` - Task type (sql/scrape)
- `schedule` - Cron expression
- `config` - JSON configuration
- `enabled` - Execution enabled
- `last_run_at` - Last execution time
- `next_run_at` - Next scheduled execution
