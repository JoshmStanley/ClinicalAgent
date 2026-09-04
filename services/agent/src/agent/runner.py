"""Executes one run: builds context, runs the orchestrator graph, streams events, records usage."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import anthropic
import httpx

from agent.graph import RunContext, run_orchestration
from agent.llm import CITE_RE
from agent.settings import Settings
from agent.tools import ToolBox
from clinical_common.auth import Principal
from clinical_common.events import Citation, RunEvent, RunEventType, RunRequested, Usage

__all__ = ["CITE_RE", "EventSink", "RunExecutor", "build_messages", "resolve_citations", "study_context_block"]

log = logging.getLogger(__name__)


class EventSink:
    """Buffers run events and flushes them to the conversations service in batches."""

    def __init__(self, client: httpx.AsyncClient, run_id: str, flush_seconds: float) -> None:
        self._client = client
        self._run_id = run_id
        self._buf: list[RunEvent] = []
        self._lock = asyncio.Lock()
        self._flush_seconds = flush_seconds
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> EventSink:
        self._task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._task:
            self._task.cancel()
        await self.flush()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_seconds)
            await self.flush()

    def emit(self, type_: RunEventType, **payload: Any) -> None:
        # Coalesce consecutive text/thinking deltas of the same track (same subagent) into one event per flush.
        if (
            self._buf
            and type_ in (RunEventType.TEXT_DELTA, RunEventType.THINKING_DELTA)
            and self._buf[-1].type == type_
            and self._buf[-1].payload.get("subagent_id") == payload.get("subagent_id")
        ):
            self._buf[-1].payload["text"] += payload.get("text", "")
            return
        self._buf.append(RunEvent(type=type_, payload=payload))

    async def flush(self) -> None:
        async with self._lock:
            if not self._buf:
                return
            batch, self._buf = self._buf, []
            try:
                r = await self._client.post(
                    f"/internal/runs/{self._run_id}/events", json=[e.model_dump(mode="json") for e in batch]
                )
                r.raise_for_status()
            except Exception:
                log.exception("failed to flush %d events", len(batch))


def build_messages(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prior turns as plain text. Tool trails from earlier runs live in the event log, not here."""
    msgs: list[dict[str, Any]] = []
    for m in history:
        role = "user" if m["role"] == "user" else "assistant"
        if msgs and msgs[-1]["role"] == role:
            msgs[-1]["content"] += "\n\n" + m["text"]
        else:
            msgs.append({"role": role, "content": m["text"]})
    while msgs and msgs[0]["role"] != "user":
        msgs.pop(0)
    return msgs


def study_context_block(ctx: dict[str, Any]) -> str:
    study = ctx.get("study")
    if not study:
        return "This conversation is not attached to a study."
    parts = [
        f"Study: {study['name']} (phase: {study.get('phase') or 'n/a'}; "
        f"indication: {study.get('indication') or 'n/a'}; status: {study['status']})"
    ]
    if study.get("description"):
        parts.append(f"Description: {study['description']}")
    sheet = ctx.get("concept_sheet")
    if sheet:
        parts.append(f"Current concept sheet (version {sheet['version']}):\n{json.dumps(sheet['content'], indent=2)}")
    else:
        parts.append("No concept sheet has been saved yet.")
    return "\n".join(parts)


def resolve_citations(text: str, tools: ToolBox, sink: Any) -> list[Citation]:
    """Maps every [[chunk:ID]] in the final text to a Citation seen by any subagent's search, emitting each."""
    citations: list[Citation] = []
    for cid in dict.fromkeys(CITE_RE.findall(text)):
        cite = tools.seen_chunks.get(cid)
        if cite:
            citations.append(cite)
            sink.emit(RunEventType.CITATION, **cite.model_dump())
    return citations


class RunExecutor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key or None)

    async def execute(self, req: RunRequested) -> None:
        principal = Principal(user_id=req.user_id, org_id=req.org_id, role=req.role)
        hdrs = principal.internal_headers(self.settings.internal_token)
        async with httpx.AsyncClient(base_url=self.settings.conversations_url, headers=hdrs, timeout=30) as conv:
            await conv.patch(f"/internal/runs/{req.run_id}", json={"status": "running"})
            ctx = (await conv.get(f"/internal/runs/{req.run_id}/context")).raise_for_status().json()
            study_id = ctx["conversation"].get("study_id")
            async with EventSink(conv, req.run_id, self.settings.event_flush_seconds) as sink:
                sink.emit(RunEventType.RUN_STARTED, model=self.settings.agent_model)
                try:
                    async with ToolBox(self.settings, principal, study_id) as tools:
                        text, citations, usage = await self.run(ctx, tools, sink)
                    await conv.post(
                        f"/internal/conversations/{req.conversation_id}/messages",
                        json={"text": text, "citations": [c.model_dump() for c in citations], "run_id": req.run_id},
                    )
                    sink.emit(RunEventType.USAGE, **usage.model_dump())
                    sink.emit(RunEventType.RUN_COMPLETED)
                    await sink.flush()
                    await conv.patch(
                        f"/internal/runs/{req.run_id}", json={"status": "completed", "usage": usage.model_dump()}
                    )
                    await self._report_usage(principal, req.run_id, usage)
                except Exception as exc:
                    log.exception("run %s failed", req.run_id)
                    sink.emit(RunEventType.RUN_FAILED, error=str(exc))
                    await sink.flush()
                    await conv.patch(f"/internal/runs/{req.run_id}", json={"status": "failed", "error": str(exc)})

    async def _report_usage(self, principal: Principal, run_id: str, usage: Usage) -> None:
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.financials_url,
                headers=principal.internal_headers(self.settings.internal_token),
                timeout=15,
            ) as fin:
                (await fin.post("/usage", json={**usage.model_dump(), "run_id": run_id})).raise_for_status()
        except Exception:
            log.exception("usage report failed for run %s", run_id)

    async def run(self, ctx: dict[str, Any], tools: ToolBox, sink: Any) -> tuple[str, list[Citation], Usage]:
        """Runs the orchestrator graph; returns the final text, resolved citations and aggregate usage."""
        rc = RunContext(
            settings=self.settings,
            client=self.client,
            tools=tools,
            sink=sink,
            history=build_messages(ctx["messages"]),
            study_block=study_context_block(ctx),
        )
        text = await run_orchestration(rc)
        return text, resolve_citations(text, tools, sink), rc.usage.total
