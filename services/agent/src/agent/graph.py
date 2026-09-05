"""LangGraph orchestrator -> subagent graph.

    START -> orchestrate -> [Send subagent x N] -> collect -> [Send subagent x M] -> ... -> synthesize -> END
                 \\-> END (simple_mode direct answer)

`orchestrate` asks the model for a plan (structured output). `collect` fans out every task whose
dependencies are done as a `Send`, so independent tasks run in parallel and dependents run in later
waves with their dependencies' outputs. A task typed `orchestrator` re-enters this same graph one
level deeper. `synthesize` writes the user-facing answer from all results.

Runtime objects (client, tools, event sink, usage meter) travel in `config["configurable"]["ctx"]`.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Annotated, Any, TypedDict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Send
from opentelemetry import trace

from agent.llm import CITE_RE, Emitter, ScopedEmitter, UsageMeter, call_model, run_tool, text_of
from agent.plan import Plan, SubagentResult, SubagentTask, parse_plan, plan_schema, ready_tasks
from agent.prompts import SYNTHESIZER_PROMPT, SYSTEM_PROMPT, orchestrator_prompt, subagent_prompt
from agent.settings import Settings
from agent.tools import ToolBox
from clinical_common.events import RunEventType

log = logging.getLogger(__name__)


@dataclass
class RunContext:
    settings: Settings
    client: Any  # anthropic.AsyncAnthropic (or a fake in tests)
    tools: ToolBox
    sink: Emitter
    history: list[dict[str, Any]]  # conversation so far, last message is the user's request
    study_block: str
    usage: UsageMeter = field(init=False)
    limits: Any = field(init=False)

    def __post_init__(self) -> None:
        self.usage = UsageMeter(self.settings.agent_model)
        self.limits = {"spawned": 0, "writer": None, "parallel": asyncio.Semaphore(self.settings.agent_max_parallel)}

    def system(self, *extra: str) -> list[dict[str, Any]]:
        blocks = [
            {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": self.study_block},
        ]
        blocks.extend({"type": "text", "text": t} for t in extra)
        return blocks

    @property
    def tool_names(self) -> list[str]:
        return [d["name"] for d in self.tools.definitions]


class ScopedTools:
    def __init__(self, tools, allowed):
        self.parent = tools
        self.definitions = [d for d in tools.definitions if d["name"] in allowed]
        self.seen_chunks = tools.seen_chunks

    async def execute(self, name, args):
        if name not in {d["name"] for d in self.definitions}:
            raise PermissionError(f"Tool {name} is not assigned to this task")
        return await self.parent.execute(name, args)


def _merge(a: dict[str, SubagentResult] | None, b: dict[str, SubagentResult] | None) -> dict[str, SubagentResult]:
    return {**(a or {}), **(b or {})}


class OrchestratorState(TypedDict, total=False):
    depth: int
    orchestrator_id: str | None  # subagent_id of the task this level is running; None at the root
    objective: str  # nested levels only: the orchestrator task's objective
    objective_name: str  # nested levels only: the orchestrator task's name (for event tags)
    inputs: dict[str, SubagentResult]  # nested levels only: outputs of the tasks this level depends on
    plan: Plan
    results: Annotated[dict[str, SubagentResult], _merge]
    final_text: str


def initial_state(
    *,
    depth: int = 0,
    orchestrator_id: str | None = None,
    objective: str = "",
    objective_name: str = "",
    inputs: dict[str, SubagentResult] | None = None,
) -> OrchestratorState:
    return OrchestratorState(
        depth=depth,
        orchestrator_id=orchestrator_id,
        objective=objective,
        objective_name=objective_name,
        inputs=inputs or {},
        results={},
    )


def _ctx(config: RunnableConfig) -> RunContext:
    return config["configurable"]["ctx"]


def _scope(ctx: RunContext, state: OrchestratorState) -> ScopedEmitter:
    tags: dict[str, Any] = {}
    if state.get("orchestrator_id"):
        tags = {"subagent_id": state["orchestrator_id"], "subagent_name": state.get("objective_name")}
    return ScopedEmitter(ctx.sink, **tags)


def _inputs_block(inputs: dict[str, SubagentResult]) -> str:
    if not inputs:
        return ""
    parts = ["Outputs from the tasks this depends on:"]
    for r in inputs.values():
        body = r.text if r.ok else f"(failed: {r.error})"
        parts.append(f"### {r.name}\n{body}")
    return "\n\n".join(parts)


def _level_messages(ctx: RunContext, state: OrchestratorState) -> list[dict[str, Any]]:
    """History plus, for nested levels, a user turn scoping this level to its task objective."""
    msgs = list(ctx.history)
    if state.get("depth", 0) > 0:
        brief = f"Within the request above, coordinate this sub-task: {state['objective']}"
        inputs = _inputs_block(state.get("inputs") or {})
        msgs.append({"role": "user", "content": f"{brief}\n\n{inputs}".strip()})
    return msgs


# --- nodes -------------------------------------------------------------------


async def bounded_model(ctx, *args, **kwargs):
    async with ctx.limits["parallel"]:
        return await call_model(*args, **kwargs)


async def orchestrate(state: OrchestratorState, config: RunnableConfig) -> dict[str, Any]:
    ctx, s = _ctx(config), _ctx(config).settings
    sink = _scope(ctx, state)
    depth = state.get("depth", 0)
    can_nest = depth + 1 < s.orchestrator_max_depth
    allow_answer = s.simple_mode
    tool_lines = [f"{d['name']}: {d.get('description', '').split('. ')[0]}" for d in ctx.tools.definitions]
    final = await bounded_model(
        ctx,
        ctx.client,
        model=s.agent_model,
        system=ctx.system(orchestrator_prompt(tool_lines, s.orchestrator_max_subagents, can_nest, allow_answer)),
        messages=_level_messages(ctx, state),
        effort=s.agent_effort,
        max_tokens=s.agent_max_tokens,
        usage=ctx.usage,
        sink=sink,
        output_schema=plan_schema(ctx.tool_names, can_nest=can_nest, allow_answer=allow_answer),
        emit_text=False,
    )
    plan = parse_plan(
        text_of(final), tool_names=ctx.tool_names, max_tasks=s.orchestrator_max_subagents, can_nest=can_nest
    )
    if plan.mode == "answer":
        sink.emit(RunEventType.TEXT_DELTA, text=plan.answer)
        return {"plan": plan, "final_text": plan.answer}
    sink.emit(
        RunEventType.PLAN,
        orchestrator_id=state.get("orchestrator_id"),
        depth=depth,
        rationale=plan.rationale,
        tasks=[t.brief() for t in plan.tasks],
    )
    return {"plan": plan}


def dispatch(state: OrchestratorState) -> list[Send] | str:
    """Fan out every task whose dependencies are done; synthesize once nothing is left."""
    if state.get("final_text"):
        return END
    results = state.get("results") or {}
    ready = ready_tasks(state["plan"], set(results))
    if not ready:
        return "synthesize"
    return [
        Send(
            "subagent",
            {
                "task": t,
                "depth": state.get("depth", 0),
                "parent_id": state.get("orchestrator_id"),
                "inputs": {d: results[d] for d in t.depends_on},
            },
        )
        for t in ready
    ]


async def subagent(payload: dict[str, Any], config: RunnableConfig) -> dict[str, Any]:
    task = payload["task"]
    with trace.get_tracer("agent.graph").start_as_current_span(
        "agent.task",
        attributes={
            "agent.id": task.subagent_id,
            "agent.name": task.name,
            "agent.type": task.type,
            "agent.depth": payload["depth"] + 1,
        },
    ):
        return await _subagent(payload, config)


async def _subagent(payload: dict[str, Any], config: RunnableConfig) -> dict[str, Any]:
    ctx = copy.copy(_ctx(config))
    task: SubagentTask = payload["task"]
    if ctx.limits["spawned"] >= ctx.settings.agent_max_total_agents:
        return {
            "results": {
                task.name: SubagentResult(
                    name=task.name,
                    subagent_id=task.subagent_id,
                    type=task.type,
                    ok=False,
                    error="Run-wide agent limit reached; task was not executed",
                )
            }
        }
    ctx.limits["spawned"] += 1
    ctx.tools = ScopedTools(ctx.tools, set(task.tools))
    depth: int = payload["depth"] + 1
    inputs: dict[str, SubagentResult] = payload["inputs"]
    sink = ScopedEmitter(ctx.sink, subagent_id=task.subagent_id, subagent_name=task.name)
    nested = task.type == "orchestrator" and depth < ctx.settings.orchestrator_max_depth
    sink.emit(
        RunEventType.SUBAGENT_STARTED,
        parent_id=payload["parent_id"],
        depth=depth,
        type="orchestrator" if nested else "worker",
        objective=task.objective,
        tools=task.tools,
        effort=task.effort,
        depends_on=task.depends_on,
    )
    try:
        if nested:
            result = await run_nested(ctx, task, inputs, depth)
        else:
            async with ctx.limits["parallel"]:
                result = await run_worker(ctx, task, inputs, sink)
    except Exception as exc:
        log.exception("subagent %s failed", task.name)
        result = SubagentResult(name=task.name, subagent_id=task.subagent_id, type=task.type, ok=False, error=str(exc))
    sink.emit(
        RunEventType.SUBAGENT_COMPLETED,
        depth=depth,
        ok=result.ok,
        error=result.error,
        summary=result.text[:500],
        chunk_ids=result.chunk_ids,
        tool_calls=result.tool_calls,
        iterations=result.iterations,
        truncated=result.truncated,
    )
    return {"results": {task.name: result}}


async def collect(state: OrchestratorState) -> dict[str, Any]:
    """Join point after a wave of subagents; `dispatch` decides what runs next."""
    return {}


async def synthesize(state: OrchestratorState, config: RunnableConfig) -> dict[str, Any]:
    ctx, s = _ctx(config), _ctx(config).settings
    sink = _scope(ctx, state)
    plan, results = state["plan"], state.get("results") or {}
    parts = [f"Plan rationale: {plan.rationale or '(none)'}", "Subagent results:"]
    for t in plan.tasks:
        r = results.get(t.name)
        if r is None:
            body = "(did not run)"
        elif r.ok:
            body = r.text or "(no output)"
        else:
            body = f"FAILED: {r.error}"
        parts.append(f"### {t.name} ({t.type})\nObjective: {t.objective}\n\n{body}")
    messages = _level_messages(ctx, state) + [{"role": "user", "content": "\n\n".join(parts)}]
    final = await bounded_model(
        ctx,
        ctx.client,
        model=s.agent_model,
        system=ctx.system(SYNTHESIZER_PROMPT),
        messages=messages,
        effort=s.agent_effort,
        max_tokens=s.agent_max_tokens,
        usage=ctx.usage,
        sink=sink,
    )
    return {"final_text": text_of(final)}


# --- subagent bodies ---------------------------------------------------------


async def run_worker(ctx: RunContext, task: SubagentTask, inputs: dict[str, SubagentResult], sink: Emitter):
    """Bounded tool loop for one worker task. On the last allowed iteration tools are disabled so the
    model must answer."""
    s = ctx.settings
    allowed = set(task.tools)
    if "update_concept_sheet" in allowed:
        if ctx.limits["writer"] is None:
            ctx.limits["writer"] = task.subagent_id
        elif ctx.limits["writer"] != task.subagent_id:
            allowed.remove("update_concept_sheet")
    ctx.tools = ScopedTools(ctx.tools, allowed)
    tool_defs = [d for d in ctx.tools.definitions if d["name"] in allowed]
    effort = task.effort or s.subagent_effort
    system = ctx.system(subagent_prompt(task.name, task.objective, [d["name"] for d in tool_defs], effort))
    brief = f"Your task now: {task.objective}\n\n{_inputs_block(inputs)}".strip()
    messages = list(ctx.history) + [{"role": "user", "content": brief}]
    collected: list[str] = []
    tool_calls = iterations = 0
    truncated = False

    for i in range(s.subagent_max_iterations):
        last = i == s.subagent_max_iterations - 1
        final = await call_model(
            ctx.client,
            model=s.agent_model,
            system=system,
            messages=messages,
            effort=effort,
            max_tokens=s.agent_max_tokens,
            usage=ctx.usage,
            sink=sink,
            tools=tool_defs,
            tool_choice={"type": "none"} if last else None,
        )
        iterations += 1
        collected.append(text_of(final))
        messages.append({"role": "assistant", "content": final.content})
        if final.stop_reason == "pause_turn":
            continue
        if final.stop_reason == "max_tokens":
            truncated = True
            break
        if final.stop_reason != "tool_use":
            break
        uses = [b for b in final.content if b.type == "tool_use"]
        tool_calls += len(uses)
        results = await asyncio.gather(*(run_tool(ctx.tools, sink, b) for b in uses))
        messages.append({"role": "user", "content": list(results)})

    text = "\n\n".join(t for t in collected if t)
    return SubagentResult(
        name=task.name,
        subagent_id=task.subagent_id,
        type="worker",
        text=text,
        chunk_ids=list(dict.fromkeys(CITE_RE.findall(text))),
        tool_calls=tool_calls,
        iterations=iterations,
        truncated=truncated,
    )


async def run_nested(ctx: RunContext, task: SubagentTask, inputs: dict[str, SubagentResult], depth: int):
    """Runs the orchestrator graph one level deeper for an `orchestrator` task."""
    state = initial_state(
        depth=depth,
        orchestrator_id=task.subagent_id,
        objective=task.objective,
        objective_name=task.name,
        inputs=inputs,
    )
    out = await get_graph().ainvoke(state, run_config(ctx))
    text = out.get("final_text", "")
    nested = out.get("results") or {}
    return SubagentResult(
        name=task.name,
        subagent_id=task.subagent_id,
        type="orchestrator",
        text=text,
        chunk_ids=list(dict.fromkeys(CITE_RE.findall(text))),
        tool_calls=sum(r.tool_calls for r in nested.values()),
        iterations=sum(r.iterations for r in nested.values()),
    )


# --- graph -------------------------------------------------------------------


def build_graph() -> CompiledStateGraph:
    g = StateGraph(OrchestratorState)
    g.add_node("orchestrate", orchestrate)
    g.add_node("subagent", subagent)
    g.add_node("collect", collect)
    g.add_node("synthesize", synthesize)
    g.add_edge(START, "orchestrate")
    g.add_conditional_edges("orchestrate", dispatch, ["subagent", "synthesize", END])
    g.add_edge("subagent", "collect")
    g.add_conditional_edges("collect", dispatch, ["subagent", "synthesize"])
    g.add_edge("synthesize", END)
    return g.compile()


@lru_cache
def get_graph() -> CompiledStateGraph:
    return build_graph()


def run_config(ctx: RunContext) -> RunnableConfig:
    # Supersteps per level: orchestrate + (subagent, collect) per wave + synthesize. Waves <= tasks.
    return {"configurable": {"ctx": ctx}, "recursion_limit": 10 + 4 * ctx.settings.orchestrator_max_subagents}


async def run_orchestration(ctx: RunContext) -> str:
    """Runs the whole graph for one run and returns the final user-facing text."""
    out = await get_graph().ainvoke(initial_state(), run_config(ctx))
    return out.get("final_text", "")
