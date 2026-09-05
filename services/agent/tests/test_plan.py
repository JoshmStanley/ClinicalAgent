import json

import pytest

from agent.plan import Plan, SubagentTask, parse_plan, plan_schema, ready_tasks, waves

TOOLS = ["search_documents", "update_concept_sheet", "search_studies"]


def _plan(tasks, mode="plan", **extra):
    return json.dumps({"mode": mode, "rationale": "r", "answer": "", "tasks": tasks, **extra})


def _task(name, deps=(), tools=(), type_="worker", effort="low"):
    return {
        "name": name,
        "objective": f"do {name}",
        "type": type_,
        "tools": list(tools),
        "effort": effort,
        "depends_on": list(deps),
    }


def test_parse_plan_keeps_valid_dependencies_and_tools():
    plan = parse_plan(
        _plan([_task("a", tools=["search_documents"]), _task("b", deps=["a"], tools=["search_studies"])]),
        tool_names=TOOLS,
        max_tasks=5,
        can_nest=True,
    )
    assert [t.name for t in plan.tasks] == ["a", "b"]
    assert plan.tasks[1].depends_on == ["a"]
    assert plan.tasks[0].tools == ["search_documents"]
    assert waves(plan) == [["a"], ["b"]]


def test_parse_plan_drops_unknown_tools_deps_and_self_deps():
    plan = parse_plan(
        _plan([_task("a", deps=["a", "ghost"], tools=["nope", "search_documents", "search_documents"])]),
        tool_names=TOOLS,
        max_tasks=5,
        can_nest=True,
    )
    assert plan.tasks[0].depends_on == []
    assert plan.tasks[0].tools == ["search_documents"]


def test_parse_plan_truncates_and_dedupes_names():
    plan = parse_plan(
        _plan([_task("Same Name"), _task("same_name"), _task("c", deps=["same_name"]), _task("d")]),
        tool_names=TOOLS,
        max_tasks=3,
        can_nest=True,
    )
    assert [t.name for t in plan.tasks] == ["same_name", "same_name_2", "c"]
    assert plan.tasks[2].depends_on == ["same_name_2"]


def test_parse_plan_breaks_cycles():
    plan = parse_plan(
        _plan([_task("a", deps=["b"]), _task("b", deps=["a"])]), tool_names=TOOLS, max_tasks=5, can_nest=True
    )
    assert all(t.depends_on == [] for t in plan.tasks)
    assert waves(plan) == [["a", "b"]]


def test_parse_plan_disables_nesting_when_not_allowed():
    plan = parse_plan(_plan([_task("a", type_="orchestrator")]), tool_names=TOOLS, max_tasks=5, can_nest=False)
    assert plan.tasks[0].type == "worker"
    plan = parse_plan(_plan([_task("a", type_="orchestrator")]), tool_names=TOOLS, max_tasks=5, can_nest=True)
    assert plan.tasks[0].type == "orchestrator"


def test_parse_plan_answer_mode_has_no_tasks():
    plan = parse_plan(_plan([_task("a")], mode="answer", answer="hi"), tool_names=TOOLS, max_tasks=5, can_nest=True)
    assert plan.mode == "answer" and plan.answer == "hi" and plan.tasks == []


def test_parse_plan_rejects_garbage():
    with pytest.raises(ValueError):
        parse_plan('{"mode": "plan", "tasks": [{"name": 1}]}', tool_names=TOOLS, max_tasks=5, can_nest=True)


def test_ready_tasks_and_waves_parallel_vs_sequential():
    plan = Plan(
        tasks=[
            SubagentTask(name="research", objective="x"),
            SubagentTask(name="sheet", objective="y"),
            SubagentTask(name="proposal", objective="z", depends_on=["research", "sheet"]),
        ]
    )
    assert [t.name for t in ready_tasks(plan, set())] == ["research", "sheet"]
    assert [t.name for t in ready_tasks(plan, {"research"})] == ["sheet"]
    assert [t.name for t in ready_tasks(plan, {"research", "sheet"})] == ["proposal"]
    assert waves(plan) == [["research", "sheet"], ["proposal"]]


def test_plan_schema_reflects_limits():
    s = plan_schema(TOOLS, can_nest=False, allow_answer=False)
    assert s["properties"]["mode"]["enum"] == ["plan"]
    assert s["properties"]["tasks"]["items"]["properties"]["type"]["enum"] == ["worker"]
    assert s["properties"]["tasks"]["items"]["properties"]["tools"]["items"]["enum"] == sorted(TOOLS)
    s = plan_schema([], can_nest=True, allow_answer=True)
    assert s["properties"]["mode"]["enum"] == ["plan", "answer"]
    assert "enum" not in s["properties"]["tasks"]["items"]["properties"]["tools"]["items"]
