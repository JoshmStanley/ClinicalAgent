"""The two primitives every graph node uses to talk to the model and to run tools.

`call_model` and `run_tool` are the only code paths that call the Claude API or execute a tool, so
observability (tracing, metrics) hooks attach here rather than inside the graph.
"""

from __future__ import annotations

import json
import logging
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Protocol

from agent.tools import ToolBox
from clinical_common.events import RunEventType, Usage

log = logging.getLogger(__name__)
active_trace = ContextVar("active_run_trace", default=None)


@asynccontextmanager
async def generation_trace(model, messages):
    rt = active_trace.get()
    if rt is None:
        yield None
    else:
        async with rt.generation(model, messages) as generation:
            yield generation


CITE_RE = re.compile(r"\[\[chunk:([^\]\s]+)\]\]")


class Emitter(Protocol):
    def emit(self, type_: RunEventType, **payload: Any) -> None: ...


class ScopedEmitter:
    """Wraps an emitter and stamps fixed tags (e.g. subagent_id) onto every event payload."""

    def __init__(self, sink: Emitter, **tags: Any) -> None:
        self._sink = sink
        self.tags = {k: v for k, v in tags.items() if v is not None}

    def emit(self, type_: RunEventType, **payload: Any) -> None:
        self._sink.emit(type_, **{**self.tags, **payload})

    def scoped(self, **tags: Any) -> ScopedEmitter:
        return ScopedEmitter(self._sink, **{**self.tags, **tags})


class UsageMeter:
    """Aggregates token usage across every model call of a run (orchestrators, subagents, synthesizer)."""

    def __init__(self, model: str) -> None:
        self.total = Usage(model=model)
        self.calls = 0

    def add(self, final: Any) -> None:
        u = final.usage
        self.total.input_tokens += getattr(u, "input_tokens", 0) or 0
        self.total.output_tokens += getattr(u, "output_tokens", 0) or 0
        self.total.cache_read_input_tokens += getattr(u, "cache_read_input_tokens", 0) or 0
        self.total.cache_creation_input_tokens += getattr(u, "cache_creation_input_tokens", 0) or 0
        self.total.model = getattr(final, "model", None) or self.total.model
        self.calls += 1


class ModelRefused(RuntimeError):
    pass


async def call_model(
    client: Any,
    *,
    model: str,
    system: list[dict[str, Any]],
    messages: list[dict[str, Any]],
    effort: str,
    max_tokens: int,
    usage: UsageMeter,
    sink: Emitter,
    tools: list[dict[str, Any]] | None = None,
    tool_choice: dict[str, Any] | None = None,
    output_schema: dict[str, Any] | None = None,
    emit_text: bool = True,
) -> Any:
    """One streamed Claude call. Streams deltas into `sink`, records usage, raises on refusal.

    Returns the final message; the caller appends `final.content` as the assistant turn and branches on
    `final.stop_reason` (tool_use / pause_turn / max_tokens / end_turn).
    """
    output_config: dict[str, Any] = {"effort": effort}
    if output_schema is not None:
        output_config["format"] = {"type": "json_schema", "schema": output_schema}
    kwargs: dict[str, Any] = dict(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
        thinking={"type": "adaptive", "display": "summarized"},
        output_config=output_config,
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",  # route safety refusals to Anthropic's recommended fallback model
    )
    if tools:
        kwargs["tools"] = tools
        if tool_choice is not None:
            kwargs["tool_choice"] = tool_choice

    async with generation_trace(model, messages) as generation, client.beta.messages.stream(**kwargs) as stream:
        async for event in stream:
            if event.type == "content_block_delta":
                if event.delta.type == "text_delta" and emit_text:
                    sink.emit(RunEventType.TEXT_DELTA, text=event.delta.text)
                elif event.delta.type == "thinking_delta" and event.delta.thinking:
                    sink.emit(RunEventType.THINKING_DELTA, text=event.delta.thinking)
            elif event.type == "content_block_start" and event.content_block.type == "tool_use":
                sink.emit(RunEventType.TOOL_CALL, tool_use_id=event.content_block.id, name=event.content_block.name)
        final = await stream.get_final_message()
        if generation is not None:
            generation.record(final)

    usage.add(final)
    if final.stop_reason == "refusal":
        details = getattr(final, "stop_details", None)
        raise ModelRefused(f"Model declined the request ({getattr(details, 'category', None) or 'refusal'})")
    if final.stop_reason == "max_tokens":
        log.warning("model output truncated at max_tokens=%d", max_tokens)
    return final


def text_of(final: Any) -> str:
    return "\n\n".join(b.text for b in final.content if b.type == "text" and b.text.strip())


async def run_tool(tools: ToolBox, sink: Emitter, block: Any) -> dict[str, Any]:
    """Executes one tool_use block, emits tool.result (and concept_sheet.updated), returns the tool_result."""
    args = block.input if isinstance(block.input, dict) else json.loads(block.input)
    try:
        rt = active_trace.get()
        if rt is None:
            output, info = await tools.execute(block.name, args)
        else:
            async with rt.tool_span(block.name, args) as ts:
                output, info = await tools.execute(block.name, args)
                ts.ok(output)
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
