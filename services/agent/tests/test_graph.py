"""Graph tests with a scripted fake model: no network, no API key."""

from __future__ import annotations

import asyncio
import json
import re
from types import SimpleNamespace as NS

import pytest

from agent.graph import build_graph
from agent.llm import ModelRefused
from agent.runner import EventSink, RunExecutor
from agent.settings import Settings
from agent.tools import SEARCH_DOCUMENTS, UPDATE_CONCEPT_SHEET
from clinical_common.events import Citation
from clinical_common.events import RunEventType as T

SEARCH_STUDIES = {
    "name": "search_studies",
    "description": "Search ClinicalTrials.gov",
    "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}},
}
USAGE = (10, 5)


# --- fakes -------------------------------------------------------------------


def msg(text="", tool_uses=(), stop=None, model="claude-opus-5"):
    content = [NS(type="text", text=text)] if text else []
    for i, (name, args) in enumerate(tool_uses):
        content.append(NS(type="tool_use", id=f"tu_{name}_{i}", name=name, input=args))
    return NS(
        content=content,
        stop_reason=stop or ("tool_use" if tool_uses else "end_turn"),
        usage=NS(
            input_tokens=USAGE[0], output_tokens=USAGE[1], cache_read_input_tokens=0, cache_creation_input_tokens=0
        ),
        model=model,
        stop_details=None,
    )


def plan_json(tasks, mode="plan", answer="", rationale="split the work"):
    return json.dumps({"mode": mode, "rationale": rationale, "answer": answer, "tasks": tasks})


def task(name, objective=None, tools=(), deps=(), type_="worker", effort="low"):
    return {
        "name": name,
        "objective": objective or f"objective of {name}",
        "type": type_,
        "tools": list(tools),
        "effort": effort,
        "depends_on": list(deps),
    }


def kind_of(kw) -> tuple[str, str]:
    """Classify a model call as ('plan', ''), ('worker', name) or ('synth', '')."""
    if "format" in kw["output_config"]:
        return "plan", ""
    tail = kw["system"][-1]["text"]
    m = re.match(r"You are a subagent named `([^`]+)`", tail)
    if m:
        return "worker", m.group(1)
    assert tail.startswith("You are the synthesizer")
    return "synth", ""


class FakeStream:
    def __init__(self, client, kw):
        self.client, self.kw = client, kw

    async def __aenter__(self):
        self.client.active += 1
        self.client.max_active = max(self.client.max_active, self.client.active)
        await asyncio.sleep(0.01)  # let parallel subagents overlap
        self.final = self.client.respond(*kind_of(self.kw), self.kw)
        return self

    async def __aexit__(self, *exc):
        self.client.active -= 1

    async def __aiter__(self):
        for b in self.final.content:
            if b.type == "text":
                yield NS(type="content_block_delta", delta=NS(type="text_delta", text=b.text))
            else:
                yield NS(type="content_block_start", content_block=b)

    async def get_final_message(self):
        return self.final


class FakeClient:
    """Stands in for anthropic.AsyncAnthropic; `respond(kind, name, kw)` returns the final message."""

    def __init__(self, respond):
        self.respond = respond
        self.calls: list[dict] = []
        self.active = self.max_active = 0
        self.beta = NS(messages=NS(stream=self._stream))

    def _stream(self, **kw):
        self.calls.append(kw)
        return FakeStream(self, kw)

    def calls_of(self, kind):
        return [c for c in self.calls if kind_of(c)[0] == kind]


class FakeTools:
    definitions = [SEARCH_DOCUMENTS, UPDATE_CONCEPT_SHEET, SEARCH_STUDIES]

    def __init__(self):
        self.seen_chunks: dict[str, Citation] = {}
        self.calls: list[tuple[str, dict]] = []

    async def execute(self, name, args):
        self.calls.append((name, args))
        if name == "search_documents":
            self.seen_chunks["c1"] = Citation(chunk_id="c1", document_id="d1", document_title="Protocol", snippet="…")
            return json.dumps([{"chunk_id": "c1", "text": "ORR 41%"}]), {"hit_count": 1}
        if name == "update_concept_sheet":
            return "Saved concept sheet version 2.", {"concept_sheet": {"version": 2}}
        if name == "search_studies":
            return "NCT00000001: phase 2", {}
        raise RuntimeError(f"Unknown tool {name}")


class Sink:
    def __init__(self):
        self.events: list[tuple[T, dict]] = []

    def emit(self, type_, **payload):
        self.events.append((type_, payload))

    def of(self, type_):
        return [p for t, p in self.events if t == type_]


def last_user_text(kw) -> str:
    return kw["messages"][-1]["content"]


CTX = {
    "conversation": {"study_id": "s1"},
    "study": {"name": "XYZ-201", "phase": "2", "indication": "NSCLC", "status": "planning"},
    "concept_sheet": None,
    "messages": [{"role": "user", "text": "Research eligibility criteria and draft a proposal."}],
}


def settings(**over) -> Settings:
    return Settings(_env_file=None, anthropic_api_key="test", **over)


async def run(respond, **over):
    executor = RunExecutor(settings(**over))
    executor.client = FakeClient(respond)
    tools, sink = FakeTools(), Sink()
    text, citations, usage = await executor.run(CTX, tools, sink)
    return NS(text=text, citations=citations, usage=usage, client=executor.client, tools=tools, sink=sink)


# --- tests -------------------------------------------------------------------


def test_graph_compiles():
    g = build_graph()
    assert {"orchestrate", "subagent", "collect", "synthesize"} <= set(g.get_graph().nodes)


async def test_simple_mode_direct_answer():
    def respond(kind, name, kw):
        assert kind == "plan"
        return msg(plan_json([], mode="answer", answer="Hello!"))

    r = await run(respond)
    assert r.text == "Hello!"
    assert len(r.client.calls) == 1
    assert r.sink.of(T.TEXT_DELTA) == [{"text": "Hello!"}]
    assert not r.sink.of(T.PLAN) and not r.sink.of(T.SUBAGENT_STARTED)
    assert r.usage.input_tokens == USAGE[0] and r.usage.output_tokens == USAGE[1]


async def test_simple_mode_off_forbids_answer_mode():
    def respond(kind, name, kw):
        if kind == "plan":
            assert kw["output_config"]["format"]["schema"]["properties"]["mode"]["enum"] == ["plan"]
            return msg(plan_json([task("a")]))
        return msg(f"{kind} done")

    r = await run(respond, simple_mode=False)
    assert r.text == "synth done"


async def test_fanout_parallel_then_dependent_with_citations_and_usage():
    def respond(kind, name, kw):
        if kind == "plan":
            return msg(
                plan_json(
                    [
                        task("docs", tools=["search_documents"]),
                        task("trials", tools=["search_studies"]),
                        task("proposal", deps=["docs", "trials"]),
                    ]
                )
            )
        if kind == "worker" and name == "docs":
            if not any(m["role"] == "assistant" for m in kw["messages"]):
                return msg("Searching", tool_uses=[("search_documents", {"query": "eligibility"})])
            return msg("ORR was 41% [[chunk:c1]]")
        if kind == "worker" and name == "trials":
            return msg("Similar trial NCT00000001")
        if kind == "worker" and name == "proposal":
            assert "ORR was 41% [[chunk:c1]]" in last_user_text(kw)
            assert "Similar trial NCT00000001" in last_user_text(kw)
            assert not kw.get("tools")  # a pure writing task
            return msg("Proposal draft citing [[chunk:c1]] and NCT00000001")
        assert kind == "synth"
        body = last_user_text(kw)
        assert "### proposal (worker)" in body and "Proposal draft" in body
        return msg("Final answer [[chunk:c1]] [[chunk:unknown]] NCT00000001")

    r = await run(respond)
    assert r.text.startswith("Final answer")
    # parallel wave overlapped, dependent ran after both
    assert r.client.max_active >= 2
    started = [p["subagent_name"] for p in r.sink.of(T.SUBAGENT_STARTED)]
    completed = [p["subagent_name"] for p in r.sink.of(T.SUBAGENT_COMPLETED)]
    assert set(started[:2]) == {"docs", "trials"} and started[2] == "proposal"
    assert set(completed[:2]) == {"docs", "trials"} and completed[2] == "proposal"
    # plan event lists every task with an id
    (plan,) = r.sink.of(T.PLAN)
    assert plan["depth"] == 0 and [t["name"] for t in plan["tasks"]] == ["docs", "trials", "proposal"]
    ids = {t["name"]: t["subagent_id"] for t in plan["tasks"]}
    # worker events are tagged; synthesizer text is not
    tool_calls = r.sink.of(T.TOOL_CALL)
    assert tool_calls == [
        {
            "subagent_id": ids["docs"],
            "subagent_name": "docs",
            "tool_use_id": "tu_search_documents_0",
            "name": "search_documents",
        }
    ]
    assert r.sink.of(T.TOOL_RESULT)[0]["subagent_id"] == ids["docs"]
    deltas = r.sink.of(T.TEXT_DELTA)
    assert {d.get("subagent_id") for d in deltas if d["text"] != r.text} == set(ids.values())
    assert [d for d in deltas if d["text"] == r.text] == [{"text": r.text}]
    # subagent.completed reports chunk ids used
    docs_done = next(p for p in r.sink.of(T.SUBAGENT_COMPLETED) if p["subagent_name"] == "docs")
    assert docs_done["ok"] and docs_done["chunk_ids"] == ["c1"] and docs_done["tool_calls"] == 1
    # citations resolved from a chunk a subagent saw; unknown ids dropped
    assert [c.chunk_id for c in r.citations] == ["c1"]
    assert r.sink.of(T.CITATION)[0]["chunk_id"] == "c1"
    # usage aggregated over plan + 4 worker calls + synth
    n = len(r.client.calls)
    assert n == 6
    assert (r.usage.input_tokens, r.usage.output_tokens) == (USAGE[0] * n, USAGE[1] * n)


async def test_nested_orchestrator_runs_own_plan_and_depth_limit_applies():
    def respond(kind, name, kw):
        if kind == "plan":
            depth0 = "coordinate this sub-task" not in last_user_text(kw)
            if depth0:
                schema_types = kw["output_config"]["format"]["schema"]["properties"]["tasks"]["items"]["properties"][
                    "type"
                ]
                assert schema_types["enum"] == ["worker", "orchestrator"]
                return msg(
                    plan_json(
                        [
                            task("research", type_="orchestrator", tools=["search_documents", "search_studies"]),
                            task("sheet", tools=["update_concept_sheet"]),
                            task("proposal", deps=["research", "sheet"]),
                        ]
                    )
                )
            # nested level: nesting is exhausted, so the schema only offers workers
            schema_types = kw["output_config"]["format"]["schema"]["properties"]["tasks"]["items"]["properties"]["type"]
            assert schema_types["enum"] == ["worker"]
            assert "objective of research" in last_user_text(kw)
            return msg(
                plan_json([task("internal", tools=["search_documents"]), task("registry", type_="orchestrator")])
            )
        if kind == "worker":
            if name == "sheet":
                if not any(m["role"] == "assistant" for m in kw["messages"]):
                    return msg("saving", tool_uses=[("update_concept_sheet", {"content": {}})])
                return msg("Saved I/E section (version 2)")
            if name == "proposal":
                assert "nested synthesis [[chunk:c1]]" in last_user_text(kw)
                assert "Saved I/E section" in last_user_text(kw)
            if name == "internal" and not any(m["role"] == "assistant" for m in kw["messages"]):
                return msg("searching", tool_uses=[("search_documents", {"query": "eligibility"})])
            return msg(f"{name} findings [[chunk:c1]]")
        nested = "coordinate this sub-task" in kw["messages"][1]["content"] if len(kw["messages"]) > 1 else False
        return msg("nested synthesis [[chunk:c1]]" if nested else "Top-level answer [[chunk:c1]]")

    r = await run(respond, orchestrator_max_depth=2)
    assert r.text == "Top-level answer [[chunk:c1]]"
    assert len(r.client.calls_of("plan")) == 2
    plans = r.sink.of(T.PLAN)
    research_id = next(t["subagent_id"] for t in plans[0]["tasks"] if t["name"] == "research")
    assert plans[0]["depth"] == 0 and "subagent_id" not in plans[0]
    assert plans[1]["depth"] == 1 and plans[1]["subagent_id"] == research_id
    started = {p["subagent_name"]: p for p in r.sink.of(T.SUBAGENT_STARTED)}
    assert started["research"]["type"] == "orchestrator" and started["research"]["depth"] == 1
    assert started["internal"]["depth"] == 2 and started["internal"]["parent_id"] == research_id
    assert started["registry"]["type"] == "worker"  # nesting beyond max_depth was downgraded
    assert r.sink.of(T.CONCEPT_SHEET_UPDATED) == [
        {"subagent_id": started["sheet"]["subagent_id"], "subagent_name": "sheet", "version": 2}
    ]
    done = {p["subagent_name"]: p for p in r.sink.of(T.SUBAGENT_COMPLETED)}
    assert done["research"]["ok"] and done["research"]["chunk_ids"] == ["c1"] and done["research"]["tool_calls"] == 1
    n = len(r.client.calls)
    assert n == 2 + 3 + 2 + 1 + 1 + 1  # plans, internal(2)+registry, sheet(2), research synth, proposal, synth
    assert (r.usage.input_tokens, r.usage.output_tokens) == (USAGE[0] * n, USAGE[1] * n)
    assert [c.chunk_id for c in r.citations] == ["c1"]


async def test_depth_limit_of_one_runs_orchestrator_tasks_as_workers():
    def respond(kind, name, kw):
        if kind == "plan":
            return msg(plan_json([task("research", type_="orchestrator", tools=["search_documents"])]))
        return msg(f"{kind} {name}".strip())

    r = await run(respond, orchestrator_max_depth=1)
    assert len(r.client.calls_of("plan")) == 1
    assert r.sink.of(T.SUBAGENT_STARTED)[0]["type"] == "worker"
    assert r.text == "synth"


async def test_subagent_failure_is_reported_not_fatal():
    def respond(kind, name, kw):
        if kind == "plan":
            return msg(plan_json([task("good"), task("bad")]))
        if name == "bad":
            raise RuntimeError("kaput")
        if kind == "worker":
            return msg("good findings")
        assert "FAILED: kaput" in last_user_text(kw) and "good findings" in last_user_text(kw)
        return msg("answer with caveat")

    r = await run(respond)
    assert r.text == "answer with caveat"
    done = {p["subagent_name"]: p for p in r.sink.of(T.SUBAGENT_COMPLETED)}
    assert done["good"]["ok"] and not done["bad"]["ok"] and done["bad"]["error"] == "kaput"


async def test_worker_iteration_bound_forces_answer_on_last_call():
    def respond(kind, name, kw):
        if kind == "plan":
            return msg(plan_json([task("loop", tools=["search_documents"])]))
        if kind == "worker":
            if kw.get("tool_choice") == {"type": "none"}:
                return msg("forced answer")
            return msg("again", tool_uses=[("search_documents", {"query": "q"})])
        return msg("final")

    r = await run(respond, subagent_max_iterations=3)
    worker_calls = r.client.calls_of("worker")
    assert [c.get("tool_choice") for c in worker_calls] == [None, None, {"type": "none"}]
    done = r.sink.of(T.SUBAGENT_COMPLETED)[0]
    assert done["iterations"] == 3 and done["tool_calls"] == 2 and "forced answer" in done["summary"]


async def test_refusal_fails_the_run():
    def respond(kind, name, kw):
        return msg(stop="refusal")

    with pytest.raises(ModelRefused):
        await run(respond)


async def test_model_call_shape_matches_claude_api_guidance():
    def respond(kind, name, kw):
        if kind == "plan":
            return msg(plan_json([task("a", tools=["search_documents"], effort="medium")]))
        return msg("x")

    r = await run(respond, agent_effort="high", subagent_effort="low")
    for kw in r.client.calls:
        assert kw["model"] == "claude-opus-5"
        assert kw["thinking"] == {"type": "adaptive", "display": "summarized"}
        assert kw["betas"] == ["server-side-fallback-2026-07-01"] and kw["fallbacks"] == "default"
        assert kw["system"][0]["cache_control"] == {"type": "ephemeral"}
    plan_kw, worker_kw, synth_kw = r.client.calls
    assert plan_kw["output_config"]["effort"] == "high" and plan_kw["output_config"]["format"]["type"] == "json_schema"
    assert worker_kw["output_config"] == {"effort": "medium"}  # from the plan
    assert [t["name"] for t in worker_kw["tools"]] == ["search_documents"]
    assert synth_kw["output_config"] == {"effort": "high"} and "tools" not in synth_kw


def test_event_sink_coalesces_deltas_per_subagent_track():
    sink = EventSink(client=None, run_id="r", flush_seconds=1)
    sink.emit(T.TEXT_DELTA, subagent_id="a", subagent_name="a", text="he")
    sink.emit(T.TEXT_DELTA, subagent_id="a", subagent_name="a", text="llo")
    sink.emit(T.TEXT_DELTA, subagent_id="b", subagent_name="b", text="world")
    sink.emit(T.TEXT_DELTA, text="top")
    sink.emit(T.TEXT_DELTA, text="-level")
    assert [(e.payload.get("subagent_id"), e.payload["text"]) for e in sink._buf] == [
        ("a", "hello"),
        ("b", "world"),
        (None, "top-level"),
    ]
