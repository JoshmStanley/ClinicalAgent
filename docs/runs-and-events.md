# Runs, Kafka, and event batches

A conversation is the ongoing discussion about a study. A **run** is one
execution triggered by one user message: it may make several model calls,
search documents, call trial tools, and save a final answer.

## Why a run ID exists

Suppose a user asks, "Compare this protocol with similar phase II trials."
The conversations service creates a run, for example `run A`, and sends its
ID through Kafka. The worker uses that same ID when saving progress, the
answer, terminal status, and usage.

Kafka tells a worker there is work to do. Postgres stores the application
record of that work. A Kafka offset identifies a message in one topic
partition; it does not identify the business operation across HTTP calls,
model requests, progress events, and potential retries.

A returning browser can ask, "What happened to run A?" without consuming
Kafka or launching the task again.

| Record | What it represents |
| --- | --- |
| `conversations` | An ongoing discussion, optionally attached to a study |
| `messages` | User requests and saved assistant answers |
| `runs` | One execution: conversation, user/org, status, timestamps, usage, error |
| `run_events` | Ordered progress within a run: text, tools, citations, terminal event |
| `run_event_batches` | Receipts that prevent a retried event write from duplicating events |

Each event has a sequence number within its run. The SSE client reconnects
with the last sequence it received, and the server returns later events.
That is why browser disconnects need not interrupt model execution.

## An event batch is not a document batch

The worker groups small streaming updates into batches instead of making a
separate database/API write for every token. A batch gets a UUID. If the
HTTP request fails, the worker retains that batch and retries with the same
UUID and payload before sending newer events.

Sometimes the database saves the batch but the HTTP response never reaches
the worker. Retrying without a batch ID would append the same text twice.
The conversations service saves a receipt in the same transaction as the
events and returns the original receipt on retry. Reusing an ID with a
different payload returns a conflict.

These retries protect against temporary HTTP failures while the worker is
alive. They do **not** provide durable worker crash recovery: buffered events
are still in memory. Durable checkpoints, outbox dispatch, worker leases,
and resumable execution are separate future work.

## What checking a persistence response means

An awaited HTTP request can return a 500 response without raising an
exception. Previously, the agent could continue after an unsuccessful
answer/status write. Calling `raise_for_status()` makes that failure visible
before the agent declares success.

The worker now checks the running status, context load, assistant-message
write, event flush, usage submission, and completion update. A failed write
prevents a successful completion path. If recording failure also fails, the
error propagates; this change does not fix the Kafka wrapper's existing
commit-on-handler-failure behavior.

The conversations service commits terminal status and its terminal event in
one transaction. Repeating the same terminal update produces no duplicate
event; a conflicting terminal status is rejected. This does not make the
entire run a single distributed transaction. For example, an answer may be
saved before usage submission fails. Usage settlement/reconciliation remains
future work, as does accounting for model work interrupted mid-run.

## Document ingestion uses a separate identity

Every uploaded document has a document ID. The ingestion stages carry that
ID through Kafka, update the document's latest status/error in Postgres,
and store intermediate artifacts in object storage. One document may fail
while other documents succeed.

The current repository has no parent document-upload batch or complete
per-stage attempt history. A future ingestion job could identify one attempt
to process a document, with stage attempts recording start/end times,
artifact versions, and failures. Reprocessing the same document would then
create a new job while preserving the document identity.

## Configuration and rollout for this change

- Set `USAGE_WRITER_TOKEN` to the same secret in agent and financials only.
  The example and Compose value are for local development. Direct local
  service runs load it from `.env`. Missing credentials fail closed.
- Usage writes now go to `/internal/usage` and require the writer credential
  plus an authenticated principal. `/usage/summary` remains user-facing.
- Start the updated conversations service before updated agents so that
  batch receipts are supported. The scaffold's existing `create_all` creates
  the new `run_event_batches` table without rewriting existing event rows.
- Coordinate agent/financials rollout because the usage-write route changed.
  Drain old agents first: terminal events are now generated by conversations.
- These changes do not enforce hard spending caps or deduplicate usage writes.
  Reservations and durable per-request accounting are the next accounting work.

Run database tests with `TEST_DATABASE_URL` set to a test Postgres database.
Each test uses its own temporary schema. CI provisions Postgres and runs
these tests along with the unit/API tests.
