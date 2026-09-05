import uuid
from types import SimpleNamespace

from opentelemetry.trace import StatusCode

from agent import observability
from agent.observability import get_langfuse, run_trace, trace_id_for_run
from agent.settings import Settings
from clinical_common.events import RunRequested, Usage


def _settings() -> Settings:
    return Settings(langfuse_public_key="", langfuse_secret_key="", otel_exporter_otlp_endpoint="")


def _req(run_id: str) -> RunRequested:
    return RunRequested(run_id=run_id, conversation_id="c1", org_id="o1", user_id="u1", role="org:member")


def _final(text: str = "answer", stop_reason: str = "end_turn") -> SimpleNamespace:
    usage = SimpleNamespace(
        input_tokens=1000, output_tokens=200, cache_read_input_tokens=500, cache_creation_input_tokens=0
    )
    return SimpleNamespace(
        model="claude-opus-5", stop_reason=stop_reason, usage=usage, content=[SimpleNamespace(type="text", text=text)]
    )


def test_trace_id_is_the_run_uuid():
    run_id = str(uuid.uuid4())
    assert trace_id_for_run(run_id) == uuid.UUID(run_id).hex
    other = trace_id_for_run("not-a-uuid")
    assert len(other) == 32 and int(other, 16)


def test_langfuse_disabled_without_keys():
    observability._langfuse_loaded = False
    assert get_langfuse(_settings()) is None


async def test_run_trace_without_langfuse_emits_otel_spans(spans):
    observability._langfuse_loaded = False
    run_id = str(uuid.uuid4())
    messages = [{"role": "user", "content": "Is the sample size reasonable?"}]
    async with run_trace(_settings(), _req(run_id), study_id="s1", model="claude-opus-5", question="q") as rt:
        async with rt.generation("claude-opus-5", messages) as gen:
            gen.record(_final())
        async with rt.tool_span("search_documents", {"query": "endpoint"}) as ts:
            ts.ok("[]", hit_count=0)
        async with rt.tool_span("get_study", {"nct_id": "NCT1"}) as ts:
            ts.error(RuntimeError("upstream 500"))
        rt.complete("final answer", Usage(model="claude-opus-5", input_tokens=1000, output_tokens=200))

    by_name = {s.name: s for s in spans.get_finished_spans()}
    root = by_name["agent.run"]
    assert format(root.context.trace_id, "032x") == uuid.UUID(run_id).hex
    assert root.attributes["run.id"] == run_id and root.attributes["study.id"] == "s1"
    assert root.attributes["run.status"] == "completed" and root.attributes["run.cost_usd"] > 0

    gen_span = by_name["claude.messages"]
    assert gen_span.parent.span_id == root.context.span_id
    assert gen_span.attributes["gen_ai.usage.input_tokens"] == 1000
    assert gen_span.attributes["gen_ai.usage.cache_read_input_tokens"] == 500
    assert gen_span.attributes["gen_ai.response.finish_reasons"] == ("end_turn",)

    ok_tool = by_name["tool search_documents"]
    assert ok_tool.attributes["tool.ok"] is True and ok_tool.attributes["tool.hit_count"] == 0
    failed_tool = by_name["tool get_study"]
    assert failed_tool.status.status_code is StatusCode.ERROR


async def test_run_trace_records_failure_when_block_raises(spans):
    observability._langfuse_loaded = False
    try:
        async with run_trace(_settings(), _req(str(uuid.uuid4())), study_id=None, model="m", question=None):
            raise RuntimeError("model declined")
    except RuntimeError:
        pass
    (root,) = spans.get_finished_spans()
    assert root.attributes["run.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR
