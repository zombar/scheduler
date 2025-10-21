# Makefile for Scheduler Service

.PHONY: help build test clean run

# Default target
help:
	@echo "Available targets:"
	@echo "  build          - Build scheduler API"
	@echo "  test           - Run all tests"
	@echo "  clean          - Remove build artifacts"
	@echo "  run            - Run scheduler API"
	@echo "  docker-build   - Build Docker image"

# Build the API server
build:
	@echo "Building scheduler API..."
	@go build -o scheduler-api -ldflags="-s -w" ./cmd/api
	@chmod +x scheduler-api
	@echo "Build complete: scheduler-api"

# Run all tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run the API server
run: build
	@echo "Starting scheduler API..."
	@./scheduler-api $(if $(PORT),-port $(PORT)) $(if $(DB),-db $(DB))

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f scheduler-api
	@rm -f scheduler.db scheduler.db-journal
	@echo "Clean complete"

# Format Go code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t scheduler:latest .

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
