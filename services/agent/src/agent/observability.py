"""Tracing for agent runs: OpenTelemetry spans plus Langfuse observations.

One run is one trace whose id *is* the run id (the run UUID as 32 hex chars),
so the Langfuse URL and a Jaeger lookup can be derived from a run id alone.
The run trace is linked (OpenTelemetry span link + `run.id` attributes both
ways) to the Kafka consumer span that delivered `runs.requested`, which is
itself part of the API request's trace.

Langfuse is active when LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set.
It attaches to the tracer provider installed by `clinical_common.telemetry`,
so Langfuse observations also show up in Jaeger, while httpx/Kafka/database
spans stay out of Langfuse. Without keys only plain OpenTelemetry spans are
produced, and those are no-ops when tracing is disabled.

API (everything runner.py needs):

    async with run_trace(settings, req, study_id=..., model=..., question=...) as rt:
        async with rt.generation(model, messages) as gen:   # one Claude call
            final = await stream.get_final_message()
            gen.record(final)          # model, output summary, token usage, cost
        async with rt.tool_span(name, args) as ts:          # one tool call
            ts.ok(output) / ts.error(exc)
        rt.complete(text, usage) / rt.fail(exc)  # a run failure is also recorded if the block raises

Langfuse trace: user_id = principal user, session_id = conversation,
tags/metadata = org_id, conversation_id, study_id, model, run_id.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import uuid
from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from typing import Any

from langfuse import Langfuse, propagate_attributes
from opentelemetry import trace
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, StatusCode, TraceFlags

from agent.settings import Settings
from clinical_common.events import RunRequested, Usage
from clinical_common.pricing import cost_breakdown

log = logging.getLogger(__name__)
tracer = trace.get_tracer("agent")
TEXT_LIMIT = 2000

_langfuse: Langfuse | None = None
_langfuse_loaded = False


def get_langfuse(settings: Settings) -> Langfuse | None:
    """The process-wide Langfuse client, or None when keys are not configured."""
    global _langfuse, _langfuse_loaded
    if not _langfuse_loaded:
        _langfuse_loaded = True
        if settings.langfuse_public_key and settings.langfuse_secret_key:
            _langfuse = Langfuse(
                public_key=settings.langfuse_public_key,
                secret_key=settings.langfuse_secret_key,
                host=settings.langfuse_host,
                environment=settings.environment,
            )
            log.info("Langfuse tracing enabled host=%s", settings.langfuse_host)
        else:
            log.info("Langfuse tracing disabled (LANGFUSE_PUBLIC_KEY/LANGFUSE_SECRET_KEY unset)")
    return _langfuse


def trace_id_for_run(run_id: str) -> str:
    """32-hex trace id for a run: the run UUID itself, or a sha256 prefix for non-UUID ids."""
    try:
        return uuid.UUID(run_id).hex
    except ValueError:
        return hashlib.sha256(run_id.encode()).digest()[:16].hex()


def _clip(text: str) -> str:
    return text if len(text) <= TEXT_LIMIT else text[:TEXT_LIMIT] + f"... [{len(text) - TEXT_LIMIT} more chars]"


def _field(block: Any, key: str) -> Any:
    return block.get(key) if isinstance(block, dict) else getattr(block, key, None)


def _content_summary(content: Any) -> Any:
    """Compact, JSON-safe view of a message's content (SDK blocks or dicts)."""
    if isinstance(content, str):
        return _clip(content)
    out = []
    for block in content or []:
        kind = _field(block, "type")
        if kind == "text":
            out.append({"type": "text", "text": _clip(_field(block, "text") or "")})
        elif kind == "tool_use":
            out.append({"type": "tool_use", "name": _field(block, "name"), "input": _field(block, "input")})
        elif kind == "tool_result":
            body = _field(block, "content")
            out.append({"type": "tool_result", "content": _clip(body if isinstance(body, str) else json.dumps(body))})
        elif kind == "thinking":
            out.append({"type": "thinking", "chars": len(_field(block, "thinking") or "")})
        else:
            out.append({"type": kind})
    return out


def _messages_summary(messages: list[dict[str, Any]]) -> dict[str, Any]:
    last = messages[-1] if messages else None
    return {
        "message_count": len(messages),
        "last_role": last["role"] if last else None,
        "last_content": _content_summary(last["content"]) if last else None,
    }


class GenerationTrace:
    def __init__(self, observation: Any, model: str) -> None:
        self._obs = observation
        self._model = model

    def record(self, final: Any) -> None:
        """Attach the finished Claude response: model, output summary, usage and cost."""
        u = final.usage
        model = final.model or self._model
        usage = {
            "input": getattr(u, "input_tokens", 0) or 0,
            "output": getattr(u, "output_tokens", 0) or 0,
            "cache_read_input_tokens": getattr(u, "cache_read_input_tokens", 0) or 0,
            "cache_creation_input_tokens": getattr(u, "cache_creation_input_tokens", 0) or 0,
        }
        cost = cost_breakdown(
            model,
            usage["input"],
            usage["output"],
            usage["cache_read_input_tokens"],
            usage["cache_creation_input_tokens"],
        )
        trace.get_current_span().set_attributes(
            {
                "gen_ai.response.model": model,
                "gen_ai.response.finish_reasons": [final.stop_reason or ""],
                "gen_ai.usage.input_tokens": usage["input"],
                "gen_ai.usage.output_tokens": usage["output"],
                "gen_ai.usage.cache_read_input_tokens": usage["cache_read_input_tokens"],
                "gen_ai.usage.cache_creation_input_tokens": usage["cache_creation_input_tokens"],
                "gen_ai.cost_usd": cost["total"],
            }
        )
        if self._obs is not None:
            metadata = {"stop_reason": final.stop_reason, "request_id": getattr(final, "_request_id", None)}
            self._obs.update(
                model=model,
                output={"stop_reason": final.stop_reason, "content": _content_summary(final.content)},
                usage_details={**usage, "total": sum(usage.values())},
                cost_details=cost,
                metadata={k: v for k, v in metadata.items() if v is not None},
            )


class ToolTrace:
    def __init__(self, observation: Any) -> None:
        self._obs = observation

    def ok(self, output: str, **info: Any) -> None:
        span = trace.get_current_span()
        span.set_attribute("tool.ok", True)
        for k, v in info.items():
            if isinstance(v, str | int | float | bool):
                span.set_attribute(f"tool.{k}", v)
        if self._obs is not None:
            self._obs.update(output=_clip(output), metadata=info or None)

    def error(self, exc: BaseException) -> None:
        span = trace.get_current_span()
        span.set_attribute("tool.ok", False)
        span.record_exception(exc)
        span.set_status(StatusCode.ERROR, str(exc))
        if self._obs is not None:
            self._obs.update(level="ERROR", status_message=str(exc), output=f"error: {exc}")


class RunTrace:
    def __init__(self, langfuse: Langfuse | None, span: trace.Span, model: str, question: str | None) -> None:
        self._lf = langfuse
        self._span = span
        self._model = model
        self._question = question
        self._root: Any = None  # Langfuse root observation, when enabled
        self._done = False

    @asynccontextmanager
    async def generation(self, model: str, messages: list[dict[str, Any]]) -> AsyncIterator[GenerationTrace]:
        """One Claude call. Call `gen.record(final_message)` inside the block."""
        summary = _messages_summary(messages)
        if self._lf is None:
            with tracer.start_as_current_span("claude.messages", attributes={"gen_ai.request.model": model}):
                yield GenerationTrace(None, model)
            return
        with self._lf.start_as_current_observation(
            name="claude.messages", as_type="generation", model=model, input=summary
        ) as obs:
            yield GenerationTrace(obs, model)

    @asynccontextmanager
    async def tool_span(self, name: str, args: dict[str, Any]) -> AsyncIterator[ToolTrace]:
        """One tool call. Call `ts.ok(output, **info)` or `ts.error(exc)` inside the block."""
        if self._lf is None:
            with tracer.start_as_current_span(f"tool {name}", attributes={"tool.name": name}):
                yield ToolTrace(None)
            return
        with self._lf.start_as_current_observation(name=name, as_type="tool", input=args) as obs:
            trace.get_current_span().set_attribute("tool.name", name)
            yield ToolTrace(obs)

    def complete(self, text: str, usage: Usage) -> None:
        """Record the final answer and the run's total usage/cost."""
        self._done = True
        cost = cost_breakdown(
            usage.model,
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_input_tokens,
            usage.cache_creation_input_tokens,
        )
        self._span.set_attributes(
            {
                "run.status": "completed",
                "run.usage.input_tokens": usage.input_tokens,
                "run.usage.output_tokens": usage.output_tokens,
                "run.usage.cache_read_input_tokens": usage.cache_read_input_tokens,
                "run.usage.cache_creation_input_tokens": usage.cache_creation_input_tokens,
                "run.cost_usd": cost["total"],
            }
        )
        if self._root is not None:
            self._root.update(output=text, metadata={"cost_usd": cost["total"]})
            self._lf.set_current_trace_io(input=self._question, output=text)

    def fail(self, exc: BaseException) -> None:
        self._done = True
        self._span.set_attribute("run.status", "failed")
        self._span.record_exception(exc)
        self._span.set_status(StatusCode.ERROR, str(exc))
        if self._root is not None:
            self._root.update(level="ERROR", status_message=str(exc), output=f"error: {exc}")


@asynccontextmanager
async def run_trace(
    settings: Settings, req: RunRequested, *, study_id: str | None, model: str, question: str | None
) -> AsyncIterator[RunTrace]:
    """Trace one run. Use as `async with run_trace(...) as rt:` around the whole execution."""
    langfuse = get_langfuse(settings)
    trace_hex = trace_id_for_run(req.run_id)
    caller = trace.get_current_span()  # the Kafka consumer span (or a no-op span)
    caller.set_attributes({"run.id": req.run_id, "run.trace_id": trace_hex})
    caller_ctx = caller.get_span_context()

    # Start a new trace whose id is the run id: OpenTelemetry only lets us pick a
    # trace id through a (synthetic) remote parent, which is how Langfuse does it too.
    parent = NonRecordingSpan(
        SpanContext(
            trace_id=int(trace_hex, 16),
            span_id=secrets.randbits(64) or 1,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
    )
    attributes = {
        "run.id": req.run_id,
        "org.id": req.org_id,
        "conversation.id": req.conversation_id,
        "user.id": req.user_id,
        "agent.model": model,
    }
    if study_id:
        attributes["study.id"] = study_id
    with tracer.start_as_current_span(
        "agent.run",
        context=trace.set_span_in_context(parent),
        links=[Link(caller_ctx)] if caller_ctx.is_valid else [],
        attributes=attributes,
    ) as span:
        rt = RunTrace(langfuse, span, model, question)
        with ExitStack() as stack:
            if langfuse is not None:
                metadata = {k: v for k, v in {**attributes, "model": model}.items() if v is not None}
                stack.enter_context(
                    propagate_attributes(
                        user_id=req.user_id,
                        session_id=req.conversation_id,
                        tags=[f"org:{req.org_id}", f"model:{model}"],
                        metadata=metadata,
                        trace_name="agent.run",
                    )
                )
                rt._root = stack.enter_context(
                    langfuse.start_as_current_observation(name="run", as_type="agent", input=question)
                )
            try:
                yield rt
            except BaseException as exc:
                if not rt._done:
                    rt.fail(exc)
                raise
