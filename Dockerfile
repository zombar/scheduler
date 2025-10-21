# Multi-stage build for optimal image size
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata gcc musl-dev

# Set working directory
WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build binary with CGO for SQLite
RUN CGO_ENABLED=1 GOOS=linux go build -a -ldflags="-w -s" -o scheduler-api ./cmd/api

# Final stage
FROM alpine:3.20

# Install minimal runtime dependencies
RUN apk --no-cache add ca-certificates sqlite

# Create non-root user
RUN addgroup -g 1000 scheduler && \
    adduser -D -u 1000 -G scheduler scheduler

# Create necessary directories
RUN mkdir -p /app/data && \
    chown -R scheduler:scheduler /app

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/scheduler-api .

# Copy timezone data
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Switch to non-root user
USER scheduler

# Create volume for persistent data
VOLUME /app/data

# Expose API port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Default to running the API server
CMD ["./scheduler-api", "-port", "8080", "-db", "/app/data/scheduler.db"]
