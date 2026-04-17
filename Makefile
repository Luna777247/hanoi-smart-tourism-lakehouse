# File: Makefile
# Hanoi Smart Tourism Lakehouse - Packaging & Orchestration

.PHONY: build package up down clean logs help status

# Variables
DOCKER_COMPOSE = docker compose
PROJECT_NAME = lakehouse

help:
	@echo "Hanoi Smart Tourism Lakehouse Management"
	@echo "Usage:"
	@echo "  make build        Build all Docker images"
	@echo "  make package      Alias for build"
	@echo "  make up           Start all services in background"
	@echo "  make init         Initialize system (buckets, db, etc)"
	@echo "  make down         Stop and remove containers"
	@echo "  make logs         Tail logs (example: make logs s=airflow-worker)"
	@echo "  make clean        Complete reset (removes volumes)"
	@echo "  make status       Check health of all services"

build:
	@echo "==> Building all project images..."
	$(DOCKER_COMPOSE) build

package: build

init:
	@echo "==> Bootstrapping Lakehouse infrastructure..."
	$(DOCKER_COMPOSE) up -d postgres redis minio vault
	@echo "Waiting for core services (20s)..."
	sleep 20
	$(DOCKER_COMPOSE) run --rm minio-init
	$(DOCKER_COMPOSE) run --rm airflow-init
	@echo "Initialization complete."

up:
	@echo "==> Starting Lakehouse services..."
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

clean:
	@echo "WARNING: This will delete all persistent data."
	$(DOCKER_COMPOSE) down -v

logs:
	$(DOCKER_COMPOSE) logs -f --tail=100 $(s)

status:
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep $(PROJECT_NAME)
