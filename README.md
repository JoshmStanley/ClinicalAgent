# ClinicalAgent

A research agent for clinical development teams designing new clinical trials. Users can get feedback on a proposed study design, build a concept sheet from scratch, explore and compare trial-design trends, and see how a study fits into a development program, with every recommendation grounded in cited documents and registered trials.

The system is a set of Python/FastAPI microservices around a Claude-powered agent, with Kafka (Redpanda) for async work, Postgres per service, OpenSearch for hybrid retrieval, and S3-compatible object storage for documents. Auth is Clerk (multi-org from the start).

See [docs/architecture.md](docs/architecture.md) for the design and [docs/branching.md](docs/branching.md) for the git workflow.

## Services

| Service | Port | Role |
|---|---|---|
| `gateway` | 8000 | Public API. Verifies Clerk JWTs, checks budgets, routes to services, streams SSE through. |
| `identity` | 8001 | Users, orgs, memberships, roles. Synced from Clerk webhooks. |
| `conversations` | 8002 | Programs, studies, versioned concept sheets, conversations, runs and the run event log. |
| `agent` | worker | Consumes `runs.requested`, drives the Claude tool loop, streams events, reports usage. |
| `financials` | 8004 | Budgets per org / role / user and the usage ledger. |
| `documents` | 8005 | Upload to object storage, ingestion status, hybrid BM25 + kNN search with citation metadata. |
| `ingestion-*` | workers | `converter` → `sections` → `chunker` → `embed`, one Kafka topic per stage. |
| `clinicaltrials-mcp` | 8010 | MCP server wrapping the ClinicalTrials.gov v2 API. |

Infra: Postgres 16 (`:5432`), OpenSearch (`:9200`), Redpanda (`:19092`), MinIO (`:9000`, console `:9001`).

## Quick start

Prerequisites: Docker Desktop, [uv](https://docs.astral.sh/uv/), and an Anthropic API key.

```bash
cp .env.example .env          # add ANTHROPIC_API_KEY
make install                  # uv sync --all-packages
make up                       # build and start everything
make demo                     # upload a sample protocol, ask a question, stream the answer
```

The demo runs in `AUTH_MODE=dev`, where the gateway trusts `X-Dev-User-Id`, `X-Dev-Org-Id` and `X-Dev-Role` headers instead of a Clerk JWT. Never enable dev mode outside local development.

Embeddings default to a deterministic `fake` provider so the pipeline runs with no extra keys. For real retrieval quality set `EMBEDDING_PROVIDER=voyage` and `VOYAGE_API_KEY`.

### Running one service locally

```bash
make infra              # only postgres, opensearch, redpanda, minio
make run-conversations  # uvicorn with --reload against dockerised infra
```

Each service reads a `.env` in the repo root; the local defaults point at the Compose ports.

### Tests and lint

```bash
make lint
make test
# Include Postgres integration tests (CI runs these automatically):
TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/conversations make test
```

## API sketch (through the gateway)

```
GET  /api/me
POST /api/programs                       {name, description}
POST /api/studies                        {name, program_id?, phase, indication}
GET  /api/studies/{id}/concept-sheet     latest version
PUT  /api/studies/{id}/concept-sheet     {content} -> new version
POST /api/documents                      multipart file upload (starts ingestion)
GET  /api/documents/{id}                 status: uploaded → converted → sectioned → chunked → indexed
POST /api/search                         {query, top_k}
POST /api/conversations                  {title, study_id?}
POST /api/conversations/{id}/messages    {text} -> 202 {message, run_id}   (budget-checked)
GET  /api/runs/{id}/stream               SSE tail of the run; resumable with Last-Event-ID or ?after=
PUT  /api/budgets                        {scope: org|role|user, scope_key, monthly_limit_usd}  (admin)
GET  /api/usage/summary
```

## Status

Scaffold. Working end to end locally: upload → ingest → ask → cited streamed answer, with budgets enforced at the gateway. Not yet built: web client, Alembic migrations, structured trial-metadata extraction at ingest time, reservation-based hard budget caps, the production ingestion pipeline described in the HLD.
