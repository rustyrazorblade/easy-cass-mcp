# Makefile for easy-cass-mcp
# Provides easy commands for building, testing, and releasing

# Configuration
DOCKER_REGISTRY ?= docker.io
IMAGE_NAME ?= rustyrazorblade/easy-cass-mcp
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
FULL_IMAGE_NAME = $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/,)$(IMAGE_NAME)

# Python commands
PYTHON := python
UV := uv
PYTEST := pytest
BLACK := black
ISORT := isort
FLAKE8 := flake8
MYPY := mypy

# Docker commands
DOCKER := docker
DOCKER_COMPOSE := docker-compose

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
RED := \033[0;31m
NC := \033[0m # No Color

.PHONY: help
help: ## Show this help message
	@echo "$(CYAN)easy-cass-mcp Makefile$(NC)"
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-20s$(NC) %s\n", $$1, $$2}'

.PHONY: format
format: ## Format Python code with black and isort
	@echo "$(GREEN)Formatting code...$(NC)"
	$(BLACK) .
	$(ISORT) .

.PHONY: lint
lint: ## Run linting checks
	@echo "$(GREEN)Running linting checks...$(NC)"
	$(FLAKE8) .
	$(MYPY) .

.PHONY: check
check: format lint test ## Run all code quality checks

.PHONY: run
run: ## Run the MCP server locally
	@echo "$(GREEN)Starting MCP server...$(NC)"
	$(UV) run $(PYTHON) main.py

.PHONY: docker-build
docker-build: ## Build Docker image (multi-architecture: amd64, arm64)
	@echo "$(GREEN)Building multi-architecture Docker image: $(FULL_IMAGE_NAME):$(VERSION)$(NC)"
	@echo "$(CYAN)Platforms: linux/amd64, linux/arm64$(NC)"
	@# Ensure buildx is available and set up
	@docker buildx inspect multiarch > /dev/null 2>&1 || docker buildx create --name multiarch --driver docker-container --use
	@docker buildx use multiarch
	@# Build for both architectures but don't push yet
	@echo "$(CYAN)Note: Multi-arch images cannot be loaded locally. Use 'make docker-build-local' for local testing.$(NC)"
	@echo "$(CYAN)This will build but not load the images. Use 'make docker-push' to build and push.$(NC)"
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t $(FULL_IMAGE_NAME):$(VERSION) \
		-t $(FULL_IMAGE_NAME):latest .

.PHONY: docker-build-local
docker-build-local: ## Build Docker image for local architecture only (faster)
	@echo "$(GREEN)Building Docker image for local architecture: $(FULL_IMAGE_NAME):$(VERSION)$(NC)"
	$(DOCKER) build -t $(FULL_IMAGE_NAME):$(VERSION) -t $(FULL_IMAGE_NAME):latest .

.PHONY: docker-push
docker-push: ## Push multi-architecture Docker image to registry
	@if [ -z "$(DOCKER_REGISTRY)" ]; then \
		echo "$(RED)Error: DOCKER_REGISTRY is not set$(NC)"; \
		echo "Usage: make docker-push DOCKER_REGISTRY=your-registry.com"; \
		exit 1; \
	fi
	@echo "$(GREEN)Building and pushing multi-architecture Docker image to $(DOCKER_REGISTRY)...$(NC)"
	@echo "$(CYAN)Platforms: linux/amd64, linux/arm64$(NC)"
	@# Ensure buildx is available and set up
	@docker buildx inspect multiarch > /dev/null 2>&1 || docker buildx create --name multiarch --driver docker-container --use
	@docker buildx use multiarch
	@# Build and push in one step for multi-arch support
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t $(FULL_IMAGE_NAME):$(VERSION) \
		-t $(FULL_IMAGE_NAME):latest \
		--push .

.PHONY: version
version: ## Show current version
	@echo "$(GREEN)Current version: $(VERSION)$(NC)"

.PHONY: clean
clean: ## Clean up generated files
	@echo "$(GREEN)Cleaning up...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true

# Default target
.DEFAULT_GOAL := help