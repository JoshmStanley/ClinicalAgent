from types import SimpleNamespace

import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from clinical_common.kafka import JsonProducer, extract_context, handle_message, inject_headers

tracer = trace.get_tracer("test")


def test_inject_extract_round_trip():
    with tracer.start_as_current_span("producer-side") as span:
        headers = inject_headers()
    names = [k for k, _ in headers]
    assert "traceparent" in names
    assert all(isinstance(v, bytes) for _, v in headers)

    ctx = extract_context(headers)
    remote = trace.get_current_span(ctx).get_span_context()
    assert remote.is_valid and remote.is_remote
    assert remote.trace_id == span.get_span_context().trace_id
    assert remote.span_id == span.get_span_context().span_id


def test_inject_without_active_span_is_empty():
    assert inject_headers() == []
    assert not trace.get_current_span(extract_context(None)).get_span_context().is_valid


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def send_and_wait(self, topic, value, key=None, headers=None):
        self.calls.append({"topic": topic, "value": value, "key": key, "headers": headers})


async def test_producer_to_consumer_is_one_trace(spans):
    producer = JsonProducer.__new__(JsonProducer)
    producer._producer = _FakeProducer()
    with tracer.start_as_current_span("http request") as request_span:
        await producer.send("documents.uploaded", {"document_id": "d1"}, key="d1")
    call = producer._producer.calls[0]
    assert call["key"] == b"d1"
    assert dict(call["headers"]).get("traceparent")

    seen: dict = {}

    async def handler(value):
        ctx = trace.get_current_span().get_span_context()
        seen["value"] = value
        seen["trace_id"] = ctx.trace_id

    msg = SimpleNamespace(value=call["value"], headers=call["headers"], offset=7, partition=0)
    await handle_message(msg, topic="documents.uploaded", group_id="ingestion-converter", handler=handler)

    assert seen["value"] == {"document_id": "d1"}
    assert seen["trace_id"] == request_span.get_span_context().trace_id
    by_name = {s.name: s for s in spans.get_finished_spans()}
    send, process = by_name["documents.uploaded send"], by_name["documents.uploaded process"]
    assert send.kind is SpanKind.PRODUCER and process.kind is SpanKind.CONSUMER
    assert process.parent.span_id == send.context.span_id
    assert process.attributes["messaging.kafka.offset"] == 7
    assert process.attributes["messaging.consumer.group.name"] == "ingestion-converter"


async def test_handler_failure_is_recorded_not_raised(spans):
    async def handler(value):
        raise RuntimeError("boom")

    msg = SimpleNamespace(value={}, headers=None, offset=1, partition=0)
    await handle_message(msg, topic="t", group_id="g", handler=handler)
    (span,) = spans.get_finished_spans()
    assert span.status.status_code is StatusCode.ERROR
    assert span.events[0].name == "exception"


@pytest.mark.parametrize("headers", [None, [], [("other", b"x")]])
async def test_message_without_trace_headers_starts_new_trace(spans, headers):
    async def handler(value):
        assert trace.get_current_span().get_span_context().is_valid

    await handle_message(
        SimpleNamespace(value={}, headers=headers, offset=0, partition=0), topic="t", group_id="g", handler=handler
    )
    (span,) = spans.get_finished_spans()
    assert span.parent is None
