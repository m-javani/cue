.PHONY: build test test-integration test-integration-package clean certs help coverage docker-build docker-build-local

# Binary name
BINARY=cue

# Build directory
BUILD_DIR=.

# Version and tag
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
TAG?=latest

# Default target
help:
	@echo "Available targets:"
	@echo "  build                          - Build production binary"
	@echo "  docker-build                   - Build Docker image (full build in container)"
	@echo "  test                           - Run unit tests with coverage"
	@echo "  test-sum                       - Run unit tests with gotestsum (formatted output)"
	@echo "  test-integration               - Run all integration tests with coverage"
	@echo "  test-integration-package PKG=<path> - Run integration tests for specific package"
	@echo "                                 Example: make test-integration-package PKG=./internal/cluster/integration"
	@echo "  certs                          - Generate TLS certificates for testing"
	@echo "  clean                          - Clean build artifacts"
	@echo "  license                        - Add license headers"
	@echo "  license-check                  - Check license headers"
	@echo "  license-update                 - Update license headers"

# Build production binary
build:
	@echo "Building production binary..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
		-ldflags="-s -w -extldflags '-static' -X main.version=$(VERSION)" \
		-trimpath \
		-o $(BUILD_DIR)/$(BINARY) .
	@echo "✅ Binary built at $(BUILD_DIR)/$(BINARY)"
	@file $(BUILD_DIR)/$(BINARY)
	
# Build Docker image using Dockerfile (full build inside container)
docker-build:
	@echo "Building Docker image with tag: $(TAG)..."
	docker build -t $(BINARY):$(TAG) .
	@echo "✅ Docker image built: $(BINARY):$(TAG)"
	@docker images $(BINARY):$(TAG) --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

# Run unit tests only (explicitly list packages without integration)
test:
	@echo "🧪 Running unit tests with coverage..."
	go test -failfast -v -coverprofile=coverage.unit.cov \
		. \
		./internal/cluster \
		./internal/proxy \
		./internal/model \
		./internal/state \
		./internal/api \
		./internal/utils \
		./internal \
		./cmd \
		./pkg/discovery \
		./pkg/verifier
	@echo "✅ Unit tests complete"

# Run unit tests using gotestsum (formatted output)
test-sum:
	@echo "🧪 Running unit tests with gotestsum..."
	gotestsum --format testname -- -failfast -v -coverprofile=coverage.unit.cov \
		. \
		./internal/cluster \
		./internal/proxy \
		./internal/model \
		./internal/state \
		./internal/api \
		./internal/utils \
		./internal \
		./cmd \
		./pkg/discovery \
		./pkg/verifier
	@echo "✅ Unit tests complete"

# Run all integration tests with coverage (fail-fast, quiet mode)
test-integration:
	@echo "🧪 Running integration tests..."
	@FAILED=0; \
	for pkg in ./internal/state/integration; do \
		echo "📦 Testing $$pkg..."; \
		go test -tags=integration -p 1 $$pkg || { FAILED=1; break; }; \
	done; \
	if [ $$FAILED -eq 1 ]; then \
		echo "❌ Integration tests failed"; \
		exit 1; \
	else \
		echo "✅ All integration tests passed"; \
	fi

# Run integration tests for specific package
test-integration-package:
ifeq ($(PKG),)
	@echo "Error: PKG is required"
	@echo "Usage: make test-integration-package PKG=<package-path>"
	@exit 1
endif
	@echo "🧪 Running integration tests for $(PKG)..."
	@mkdir -p ./coverage-integration
	GOCOVERDIR=./coverage-integration go test -tags=integration -v -p 1 $(PKG)
	@echo "✅ Tests complete for $(PKG)"

# Generate TLS certs
certs:
	@echo "Generating TLS certificates..."
	@chmod +x scripts/generate-tls.sh
	@scripts/generate-tls.sh
	@echo "Certificates generated in ./certs/"

# Clean build artifacts and test data
clean:
	@echo "Cleaning..."
	@rm -rf ./certs
	@rm -f coverage.unit.cov coverage.html
	@rm -f $(BUILD_DIR)/$(BINARY)
	@go clean -testcache
	@echo "Clean complete"

.PHONY: license
license: ## Add license headers to all Go files
	@echo "Adding license headers..."
	@addlicense -c "M. Javani" -l apache .

.PHONY: license-check
license-check: ## Check if all files have license headers
	@echo "Checking license headers..."
	@addlicense -c "M. Javani" -l apache -check . || (echo "Some files are missing license headers. Run 'make license' to fix." && exit 1)

.PHONY: license-update
license-update: ## Update license headers (with current year)
	@echo "Updating license headers..."
	@addlicense -c "M. Javani" -l apache -y `date +%Y` .