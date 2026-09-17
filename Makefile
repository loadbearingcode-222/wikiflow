# wikiflow task runner.
#
# Windows users: `make` is not installed by default. Use ./make.ps1 <target> instead,
# which mirrors every target below. Both are kept in sync deliberately.
#
# All paths here are RELATIVE. The absolute project path contains a space, and GNU Make
# handles spaces in paths poorly - so the absolute path never appears in this file, and
# no path is ever used as a target or prerequisite.

.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help env hooks build up down restart ps logs shell test test-container test-e2e lint format typecheck check reprocess pg-sync parity reset pin-images

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

env:  ## Create .env with freshly generated secrets (no-op if it exists)
	@bash scripts/make-env.sh

hooks:  ## Install the repo's git hooks (refuses direct pushes to main)
	git config core.hooksPath .githooks
	@echo "git hooks installed from .githooks/"

build: env  ## Build the Airflow image
	$(COMPOSE) build

up: env  ## Build (if changed) and start the whole stack
	# --build: without it, `up` reuses whatever image already exists, so a Dockerfile
	# change is silently ignored. That once left dbt uninstalled for a whole phase.
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow UI  ->  http://localhost:8080   (airflow / airflow)"
	@echo "  MinIO       ->  http://localhost:9001"
	@echo "  Dashboard   ->  http://localhost:8501"
	@echo "  Postgres    ->  localhost:5433   (wikiflow / wikiflow_dev_secret)"
	@echo ""

down:  ## Stop the stack, keep volumes
	$(COMPOSE) down

restart:  ## Restart the Airflow services only
	$(COMPOSE) restart airflow-scheduler airflow-apiserver airflow-dag-processor airflow-triggerer

ps:  ## Show container status
	$(COMPOSE) ps

logs:  ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

shell:  ## Open a shell inside the scheduler container
	$(COMPOSE) exec airflow-scheduler bash

# --- quality gates -----------------------------------------------------------
# Unit tests run LOCALLY under uv: src/wikiflow has no Airflow imports, so they are
# fast and need no containers. DAG and integration tests need Airflow, which does not
# install cleanly on Windows, so those run in the container instead.

test:  ## Run unit tests locally (fast, no containers)
	uv run pytest tests/unit -m unit

test-container:  ## Run DAG + integration tests inside the container
	# --entrypoint is required: the Airflow image entrypoint treats a bare command
	# as an `airflow` subcommand and would print the CLI help instead of running it.
	$(COMPOSE) run --rm --no-deps --entrypoint pytest airflow-scheduler /opt/airflow/tests -m "dag or integration"

lint:  ## Check formatting and lint rules
	uv run ruff check .
	uv run ruff format --check .

format:  ## Auto-fix formatting and lint rules
	uv run ruff check --fix .
	uv run ruff format .

typecheck:  ## Type-check src/ and tests/
	uv run mypy

check: lint typecheck test  ## Everything CI runs, locally

# --- data operations (run inside the stack) --------------------------------------

test-e2e:  ## Both warehouses end to end, in isolated buckets and database (stack must be up)
	$(COMPOSE) exec -e WIKIFLOW_E2E_REQUIRED=1 airflow-scheduler pytest /opt/airflow/tests -m e2e

reprocess:  ## Rebuild bronze from raw, no download: make reprocess FROM=2026-09-14T00 TO=2026-09-14T23
	$(COMPOSE) exec airflow-scheduler python -m wikiflow.reprocess --start $(FROM) --end $(TO)

pg-sync:  ## Load new or rewritten bronze partitions into the Postgres warehouse
	$(COMPOSE) exec airflow-scheduler python -m wikiflow.postgres_load

parity:  ## Compare every mart in DuckDB and Postgres, row by row
	$(COMPOSE) exec airflow-scheduler python -m wikiflow.parity

# --- housekeeping ------------------------------------------------------------
reset:  ## Destroy the stack INCLUDING volumes (wipes metadata db and MinIO data)
	$(COMPOSE) down -v

pin-images:  ## Resolve floating image tags to digests and report them
	@bash scripts/pin-images.sh
