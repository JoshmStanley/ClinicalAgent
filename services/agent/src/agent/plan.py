"""Orchestrator plans: the JSON schema the model fills, validation, and dependency ordering."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError

log = logging.getLogger(__name__)

TaskType = Literal["worker", "orchestrator"]
Effort = Literal["low", "medium", "high"]


def new_id() -> str:
    return uuid.uuid4().hex[:12]


class SubagentTask(BaseModel):
    name: str
    objective: str
    type: TaskType = "worker"
    tools: list[str] = Field(default_factory=list)
    effort: Effort = "low"
    depends_on: list[str] = Field(default_factory=list)
    subagent_id: str = Field(default_factory=new_id)

    def brief(self) -> dict[str, Any]:
        """The task as shown in `plan` / `subagent.started` events."""
        return self.model_dump()


class Plan(BaseModel):
    mode: Literal["plan", "answer"] = "plan"
    rationale: str = ""
    answer: str = ""
    tasks: list[SubagentTask] = Field(default_factory=list)


class SubagentResult(BaseModel):
    name: str
    subagent_id: str
    type: TaskType
    ok: bool = True
    text: str = ""
    error: str = ""
    chunk_ids: list[str] = Field(default_factory=list)
    tool_calls: int = 0
    iterations: int = 0
    truncated: bool = False


def plan_schema(tool_names: list[str], *, can_nest: bool, allow_answer: bool) -> dict[str, Any]:
    """JSON schema for the orchestrator's structured output. Every field is required so the model
    never omits one; the prose prompt carries the task-count limit (enforced again in parse_plan)."""
    tool_item: dict[str, Any] = {"type": "string"}
    if tool_names:
        tool_item["enum"] = sorted(tool_names)
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["mode", "rationale", "answer", "tasks"],
        "properties": {
            "mode": {"type": "string", "enum": ["plan", "answer"] if allow_answer else ["plan"]},
            "rationale": {"type": "string", "description": "One or two sentences on how the work is split"},
            "answer": {"type": "string", "description": "The direct reply when mode is answer, else empty"},
            "tasks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["name", "objective", "type", "tools", "effort", "depends_on"],
                    "properties": {
                        "name": {"type": "string"},
                        "objective": {"type": "string"},
                        "type": {"type": "string", "enum": ["worker", "orchestrator"] if can_nest else ["worker"]},
                        "tools": {"type": "array", "items": tool_item},
                        "effort": {"type": "string", "enum": ["low", "medium", "high"]},
                        "depends_on": {"type": "array", "items": {"type": "string"}},
                    },
                },
            },
        },
    }


def parse_plan(raw: str | dict[str, Any], *, tool_names: list[str], max_tasks: int, can_nest: bool) -> Plan:
    """Parses and sanitises the model's plan so the graph can always run it:
    duplicate names are suffixed, unknown tools and dependencies dropped, nesting disabled beyond the
    depth limit, the task list truncated to `max_tasks`, and dependency cycles broken by dropping edges."""
    data = json.loads(raw) if isinstance(raw, str) else raw
    try:
        plan = Plan.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"invalid plan: {exc.errors()[0]['msg']}") from exc
    if plan.mode == "answer":
        plan.tasks = []
        return plan

    tasks = plan.tasks[:max_tasks]
    if len(plan.tasks) > max_tasks:
        log.warning("plan had %d tasks; keeping the first %d", len(plan.tasks), max_tasks)
    seen: set[str] = set()
    names = {}
    for t in tasks:
        base = "".join(c if c.isalnum() or c == "_" else "_" for c in t.name.strip().lower()) or "task"
        name, n = base, 2
        while name in seen:
            name, n = f"{base}_{n}", n + 1
        names[t.name] = name
        seen.add(name)
        t.name = name
        t.tools = [x for x in dict.fromkeys(t.tools) if x in tool_names]
        if t.type == "orchestrator" and not can_nest:
            t.type = "worker"
    for t in tasks:
        t.depends_on = [
            names.get(d, d)
            for d in dict.fromkeys(t.depends_on)
            if names.get(d, d) in seen and names.get(d, d) != t.name
        ]
    plan.tasks = tasks
    if waves(plan) is None:
        log.warning("plan has a dependency cycle; running all tasks in parallel")
        for t in tasks:
            t.depends_on = []
    return plan


def ready_tasks(plan: Plan, done: set[str]) -> list[SubagentTask]:
    """Tasks not yet run whose dependencies have all completed."""
    return [t for t in plan.tasks if t.name not in done and all(d in done for d in t.depends_on)]


def waves(plan: Plan) -> list[list[str]] | None:
    """Execution order as waves of parallel task names, or None if the dependency graph has a cycle."""
    done: set[str] = set()
    out: list[list[str]] = []
    while len(done) < len(plan.tasks):
        ready = [t.name for t in ready_tasks(plan, done)]
        if not ready:
            return None
        out.append(ready)
        done.update(ready)
    return out
