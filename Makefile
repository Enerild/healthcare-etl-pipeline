.PHONY: help run test lint format build logs db clean

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

## Docker (PostgreSQL)
run:  ## Run pipeline via Docker Compose with PostgreSQL
	docker compose up --build

build:  ## Build Docker image only
	docker compose build

logs:  ## Stream etl app logs
	docker compose logs -f etl

db:  ## Open a psql shell into the database container
	docker compose exec db psql -U etl_user -d etl_db

## Development
test:  ## Run unit tests
	PYTHONPATH=. pytest tests/ -v --tb=short

lint:  ## Run ruff lint checks
	ruff check .

format:  ## Auto-format code with ruff
	ruff format .
	ruff check . --fix

clean:  ## Tear down Docker resources
	docker compose down -v