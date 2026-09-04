# Architecture

## Principles

- **Study is the workspace.** A study owns conversations, referenced documents and a versioned concept sheet. Programs group studies into a development plan.
- **Runs are event logs.** Every agent execution appends to a persisted run event log. The client's SSE stream is a tail of that log, so runs survive disconnects and a user can resume tomorrow with `Last-Event-ID`.
- **Org id everywhere.** Every table has `org_id`; every OpenSearch query carries a server-side `org_id` filter. The principal comes from a Clerk JWT at the gateway and is forwarded to internal services with a shared internal token.
- **Citations carry chunk identity.** Chunks keep document id, section path and page. The model cites `[[chunk:ID]]`; the agent resolves those to citation events and stores them on the assistant message.

## Request flow: sending a message

```
client ──POST /api/conversations/{id}/messages──▶ gateway
  gateway ──POST /check──▶ financials            (budget gate: 402 if exhausted)
  gateway ──POST /conversations/{id}/messages──▶ conversations
      conversations: insert user message + run(queued), publish runs.requested (Kafka)
  ◀── 202 {run_id}

client ──GET /api/runs/{run_id}/stream──▶ gateway ──▶ conversations (SSE tail of run_events)

agent worker (Kafka consumer of runs.requested):
  PATCH run running → GET run context (history, study, concept sheet)
  loop: Claude stream ──▶ events (text.delta, thinking.delta, tool.call, tool.result, citation, concept_sheet.updated)
        tools: search_documents (documents /search), update_concept_sheet (conversations), ClinicalTrials.gov MCP tools
  POST assistant message, PATCH run completed + usage, POST usage → financials
```

Events are batched by the agent (text deltas coalesced, flushed every ~150 ms) and appended with a per-run sequence number. The SSE endpoint polls the table (250 ms) and emits `id: <seq>` so reconnects resume exactly.

## Ingestion (stopgap for the HLD pipeline)

```
documents.uploaded → converter → documents.converted → sections → documents.sectioned
   → chunker → documents.chunked → embed → OpenSearch `chunks` (+ documents.indexed)
```

Each stage is its own Kafka consumer group and container, reads its input artifact from object storage (`{org}/{doc}/converted.json` etc.), writes its output beside it, and reports status to the documents service. Failures mark the document `failed` and publish `documents.failed`. This mirrors the stages in the HLD so the later production pipeline can replace stages one at a time.

Search is hybrid: BM25 and kNN run as two queries and are fused with reciprocal rank fusion. Clinical text is full of exact tokens (drug names, NCT ids, endpoint names) that pure vector search handles poorly.

## Budgets

`financials` stores monthly USD limits at org, role and user scope. The gateway asks `/check` before queueing a run; the agent posts actual token usage after the run and the ledger prices it per model. Concurrent runs can overshoot a cap by about one run. A reservation model can be added if hard caps are required.

## Identity

Clerk is the source of truth. `identity` consumes Clerk webhooks (users, organizations, memberships) into its own tables so services can list members and map roles to quotas without calling Clerk. Session tokens carry `org_id` and `org_role`; the gateway requires an active org.

## Local vs production

| Concern | Local | Production |
|---|---|---|
| Auth | `AUTH_MODE=dev`, `X-Dev-*` headers | `AUTH_MODE=clerk`, JWKS verification |
| Object storage | MinIO | S3 |
| Kafka | Redpanda | Redpanda / MSK / Confluent |
| Embeddings | `fake` (deterministic) | Voyage |
| Schema | `create_all` on startup | Alembic migrations (todo) |

## Not yet built

- Web client (Next.js) consuming the SSE stream and rendering citations and the concept sheet.
- Structured trial-metadata extraction at ingest time (phase, endpoints, N, design) into queryable tables for trend analysis.
- Replay of prior runs' tool trails into model context (currently only user/assistant text is replayed; the trail lives in the event log).
- Run cancellation and per-run token budgets.
