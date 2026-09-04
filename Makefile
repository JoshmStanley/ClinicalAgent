.PHONY: install lint test infra up down logs build demo

install:
	uv sync --all-packages

lint:
	uv run ruff check .

test:
	uv run pytest -q

# Infra only: run individual services from your shell with `make run-<name>`.
infra:
	docker compose up -d postgres opensearch redpanda minio

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

build:
	docker compose build

# Run one service locally against dockerised infra, e.g. `make run-gateway`.
run-gateway:
	uv run --package gateway uvicorn gateway.main:app --port 8000 --reload
run-identity:
	uv run --package identity uvicorn identity.main:app --port 8001 --reload
run-conversations:
	uv run --package conversations uvicorn conversations.main:app --port 8002 --reload
run-financials:
	uv run --package financials uvicorn financials.main:app --port 8004 --reload
run-documents:
	uv run --package documents uvicorn documents.main:app --port 8005 --reload
run-agent:
	uv run --package agent python -m agent.worker
run-mcp:
	uv run --package clinicaltrials-mcp python -m clinicaltrials_mcp.server
run-ingestion-%:
	uv run --package ingestion python -m ingestion.worker $*

demo:
	./scripts/demo.sh
