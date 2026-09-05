import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from agent import runner
from agent.runner import EventSink, RunExecutor
from agent.settings import Settings
from clinical_common.events import RunEventType, RunRequested, Usage


async def test_event_retry_preserves_batch_and_new_events():
    requests = []
    sink = None

    def handle(request):
        requests.append((request.headers["X-Event-Batch-Id"], json.loads(request.content)))
        if len(requests) == 1:
            sink.emit(RunEventType.TEXT_DELTA, text="second")
            raise httpx.ReadTimeout("response lost", request=request)
        return httpx.Response(201, json={"last_seq": len(requests)})

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handle)) as client:
        sink = EventSink(client, "run", 60)
        sink.emit(RunEventType.TEXT_DELTA, text="first")
        await sink.flush()
    assert requests[0] == requests[1]
    assert requests[2][0] != requests[0][0]
    assert requests[2][1][0]["payload"]["text"] == "second"


async def test_failed_batch_is_retained_for_later_flush():
    requests = []
    healthy = False

    def handle(request):
        requests.append((request.headers["X-Event-Batch-Id"], request.content))
        return httpx.Response(201 if healthy else 503)

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handle)) as client:
        sink = EventSink(client, "run", 60)
        sink.emit(RunEventType.TEXT_DELTA, text="keep me")
        with pytest.raises(httpx.HTTPStatusError):
            await sink.flush()
        assert len(requests) == 3
        healthy = True
        await sink.flush()
        await sink.flush()
    assert len(requests) == 4
    assert all(request == requests[0] for request in requests)


async def test_final_flush_failure_is_not_silently_successful():
    async with httpx.AsyncClient(
        base_url="http://test", transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            async with EventSink(client, "run", 60) as sink:
                sink.emit(RunEventType.TEXT_DELTA, text="unsaved")


@pytest.mark.parametrize("failure", ["running", "context", "message", "events", "usage", "completed", None])
async def test_run_checks_persistence_before_success(monkeypatch, failure):
    operations = []
    real_client = httpx.AsyncClient

    def handle(request):
        body = json.loads(request.content) if request.content else {}
        if request.method == "PATCH":
            op = body["status"]
        elif request.url.path.endswith("/context"):
            op = "context"
        elif request.url.path.endswith("/messages"):
            op = "message"
        elif request.url.path.endswith("/events"):
            op = "events"
        else:
            op = "usage"
            assert request.url.path == "/internal/usage"
            assert request.headers["X-Usage-Writer-Token"] == "writer-secret"
        operations.append(op)
        if op == failure:
            return httpx.Response(503)
        if op == "context":
            return httpx.Response(200, json={"conversation": {"study_id": None}, "messages": []})
        return httpx.Response(200, json={})

    @asynccontextmanager
    async def toolbox(*args):
        yield object()

    async def model_loop(*args):
        operations.append("model")
        return "saved answer", [], Usage(model="test", output_tokens=1)

    monkeypatch.setattr(
        runner.httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(handle))
    )
    monkeypatch.setattr(runner, "ToolBox", toolbox)
    monkeypatch.setattr(runner.anthropic, "AsyncAnthropic", lambda **kwargs: SimpleNamespace(close=AsyncMock()))
    executor = RunExecutor(
        Settings(anthropic_api_key="test", usage_writer_token="writer-secret", event_flush_seconds=60)
    )
    monkeypatch.setattr(executor, "run", model_loop)
    try:
        await executor.execute(
            RunRequested(run_id="run", conversation_id="conv", user_id="u", org_id="o", role="org:member")
        )
    finally:
        await executor.client.close()
    if failure:
        assert operations[-1] == "failed"
        if failure != "completed":
            assert "completed" not in operations
        if failure in {"running", "context"}:
            assert "model" not in operations
    else:
        assert operations[-1] == "completed"
        assert operations.index("message") < operations.index("usage") < operations.index("completed")
        assert operations.index("events") < operations.index("completed")


async def test_shutdown_retries_cancelled_inflight_batch_with_same_id():
    import asyncio

    entered = asyncio.Event()
    requests = []

    async def handle(request):
        requests.append((request.headers["X-Event-Batch-Id"], request.content))
        if len(requests) == 1:
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(201)

    async with httpx.AsyncClient(base_url="http://test", transport=httpx.MockTransport(handle)) as client:
        async with EventSink(client, "run", 0.001) as sink:
            sink.emit(RunEventType.TEXT_DELTA, text="in flight")
            await asyncio.wait_for(entered.wait(), timeout=1)
    assert len(requests) == 2
    assert requests[0] == requests[1]
