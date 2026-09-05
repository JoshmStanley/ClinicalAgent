"""Executes one run: builds context, drives the Claude tool loop, streams events."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from contextlib import suppress
from typing import Any

import anthropic
import httpx

from agent.prompts import SYSTEM_PROMPT
from agent.settings import Settings
from agent.tools import ToolBox
from clinical_common.auth import Principal
from clinical_common.events import RunEvent, RunEventType, RunRequested, Usage

log = logging.getLogger(__name__)
CITE_RE = re.compile(r"\[\[chunk:([^\]\s]+)\]\]")


class EventSink:
    """Buffers run events and flushes them to the conversations service in batches."""

    def __init__(self, client: httpx.AsyncClient, run_id: str, flush_seconds: float) -> None:
        self._client = client
        self._run_id = run_id
        self._buf: list[RunEvent] = []
        self._pending: tuple[str, list[dict[str, Any]]] | None = None
        self._lock = asyncio.Lock()
        self._flush_seconds = flush_seconds
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> EventSink:
        self._task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        await self.flush()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_seconds)
            try:
                await self.flush()
            except httpx.HTTPError:
                # Keep the pending batch intact; later events stay behind it.
                # The final flush propagates a persistent failure to the run.
                log.exception("event flush failed; retaining batch for retry")

    def emit(self, type_: RunEventType, **payload: Any) -> None:
        # Coalesce consecutive text/thinking deltas into one event per flush.
        if (
            self._buf
            and type_ in (RunEventType.TEXT_DELTA, RunEventType.THINKING_DELTA)
            and self._buf[-1].type == type_
        ):
            self._buf[-1].payload["text"] += payload.get("text", "")
            return
        self._buf.append(RunEvent(type=type_, payload=payload))

    async def flush(self) -> None:
        async with self._lock:
            while self._pending is not None or self._buf:
                if self._pending is None:
                    batch, self._buf = self._buf, []
                    self._pending = (str(uuid.uuid4()), [e.model_dump(mode="json") for e in batch])
                batch_id, payload = self._pending
                for attempt in range(3):
                    try:
                        r = await self._client.post(
                            f"/internal/runs/{self._run_id}/events",
                            json=payload,
                            headers={"X-Event-Batch-Id": batch_id},
                        )
                        r.raise_for_status()
                        break
                    except httpx.HTTPError as exc:
                        retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                            exc.response.status_code in {408, 429} or exc.response.status_code >= 500
                        )
                        if not retryable or attempt == 2:
                            raise
                        await asyncio.sleep(0.1 * 2**attempt)
                self._pending = None


def _accumulate(total: Usage, u: Any) -> None:
    total.input_tokens += getattr(u, "input_tokens", 0) or 0
    total.output_tokens += getattr(u, "output_tokens", 0) or 0
    total.cache_read_input_tokens += getattr(u, "cache_read_input_tokens", 0) or 0
    total.cache_creation_input_tokens += getattr(u, "cache_creation_input_tokens", 0) or 0


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


class RunExecutor:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key or None)

    async def execute(self, req: RunRequested) -> None:
        principal = Principal(user_id=req.user_id, org_id=req.org_id, role=req.role)
        hdrs = principal.internal_headers(self.settings.internal_token)
        async with httpx.AsyncClient(base_url=self.settings.conversations_url, headers=hdrs, timeout=30) as conv:
            try:
                (await conv.patch(f"/internal/runs/{req.run_id}", json={"status": "running"})).raise_for_status()
                ctx = (await conv.get(f"/internal/runs/{req.run_id}/context")).raise_for_status().json()
                study_id = ctx["conversation"].get("study_id")
                async with EventSink(conv, req.run_id, self.settings.event_flush_seconds) as sink:
                    sink.emit(RunEventType.RUN_STARTED, model=self.settings.agent_model)
                    async with ToolBox(self.settings, principal, study_id) as tools:
                        text, citations, usage = await self._loop(ctx, tools, sink)
                    (
                        await conv.post(
                            f"/internal/conversations/{req.conversation_id}/messages",
                            json={"text": text, "citations": [c.model_dump() for c in citations], "run_id": req.run_id},
                        )
                    ).raise_for_status()
                    sink.emit(RunEventType.USAGE, **usage.model_dump())
                # Exiting the sink awaits its final flush before success is
                # possible. The server commits status + terminal event together.
                await self._report_usage(principal, req.run_id, usage)
                (
                    await conv.patch(
                        f"/internal/runs/{req.run_id}", json={"status": "completed", "usage": usage.model_dump()}
                    )
                ).raise_for_status()
            except Exception as exc:
                log.exception("run %s failed", req.run_id)
                try:
                    (
                        await conv.patch(f"/internal/runs/{req.run_id}", json={"status": "failed", "error": str(exc)})
                    ).raise_for_status()
                except Exception:
                    # In particular, do not turn an uncertain completion into
                    # a contradictory terminal event if its response was lost.
                    log.exception("could not persist failure for run %s", req.run_id)
                    raise

    async def _report_usage(self, principal: Principal, run_id: str, usage: Usage) -> None:
        async with httpx.AsyncClient(
            base_url=self.settings.financials_url,
            headers={
                **principal.internal_headers(self.settings.internal_token),
                "X-Usage-Writer-Token": self.settings.usage_writer_token,
            },
            timeout=15,
        ) as fin:
            (await fin.post("/internal/usage", json={**usage.model_dump(), "run_id": run_id})).raise_for_status()

    async def _loop(self, ctx: dict[str, Any], tools: ToolBox, sink: EventSink):
        s = self.settings
        system = [
            {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": study_context_block(ctx)},
        ]
        messages = build_messages(ctx["messages"])
        usage = Usage(model=s.agent_model)
        collected_text: list[str] = []

        for _ in range(s.agent_max_iterations):
            async with self.client.beta.messages.stream(
                model=s.agent_model,
                max_tokens=s.agent_max_tokens,
                system=system,
                messages=messages,
                tools=tools.definitions,
                thinking={"type": "adaptive", "display": "summarized"},
                output_config={"effort": s.agent_effort},
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",  # route safety refusals to Anthropic's recommended fallback model
            ) as stream:
                async for event in stream:
                    if event.type == "content_block_delta":
                        if event.delta.type == "text_delta":
                            sink.emit(RunEventType.TEXT_DELTA, text=event.delta.text)
                        elif event.delta.type == "thinking_delta" and event.delta.thinking:
                            sink.emit(RunEventType.THINKING_DELTA, text=event.delta.thinking)
                    elif event.type == "content_block_start" and event.content_block.type == "tool_use":
                        sink.emit(
                            RunEventType.TOOL_CALL, tool_use_id=event.content_block.id, name=event.content_block.name
                        )
                final = await stream.get_final_message()

            _accumulate(usage, final.usage)
            usage.model = final.model or usage.model
            for block in final.content:
                if block.type == "text":
                    collected_text.append(block.text)
            messages.append({"role": "assistant", "content": final.content})

            if final.stop_reason == "refusal":
                details = getattr(final, "stop_details", None)
                raise RuntimeError(f"Model declined the request ({getattr(details, 'category', None) or 'refusal'})")
            if final.stop_reason == "pause_turn":
                continue
            if final.stop_reason != "tool_use":
                break

            tool_uses = [b for b in final.content if b.type == "tool_use"]
            results = await asyncio.gather(*(self._run_tool(tools, sink, b) for b in tool_uses))
            messages.append({"role": "user", "content": results})

        text = "\n\n".join(t for t in collected_text if t.strip())
        citations = []
        for cid in dict.fromkeys(CITE_RE.findall(text)):
            cite = tools.seen_chunks.get(cid)
            if cite:
                citations.append(cite)
                sink.emit(RunEventType.CITATION, **cite.model_dump())
        return text, citations, usage

    async def _run_tool(self, tools: ToolBox, sink: EventSink, block: Any) -> dict[str, Any]:
        args = block.input if isinstance(block.input, dict) else json.loads(block.input)
        try:
            output, info = await tools.execute(block.name, args)
            if "concept_sheet" in info:
                sink.emit(RunEventType.CONCEPT_SHEET_UPDATED, version=info["concept_sheet"]["version"])
            sink.emit(
                RunEventType.TOOL_RESULT,
                tool_use_id=block.id,
                name=block.name,
                ok=True,
                summary=output[:500],
                **{k: v for k, v in info.items() if k != "concept_sheet"},
            )
            return {"type": "tool_result", "tool_use_id": block.id, "content": output}
        except Exception as exc:
            log.warning("tool %s failed: %s", block.name, exc)
            sink.emit(RunEventType.TOOL_RESULT, tool_use_id=block.id, name=block.name, ok=False, error=str(exc))
            return {"type": "tool_result", "tool_use_id": block.id, "content": f"Tool error: {exc}", "is_error": True}
