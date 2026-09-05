"""Thin JSON wrappers over aiokafka.

Trace context crosses Kafka as W3C `traceparent`/`tracestate` message headers:
`JsonProducer.send` injects them inside a producer span and `consume_forever`
extracts them and runs the handler inside a consumer span whose parent is the
producer span. A document's four ingestion stages, or a run queued by the
conversations service and executed by the agent, therefore show up as one
trace. Without a configured tracer provider both sides are no-ops.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, StatusCode

log = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class Topics:
    DOCUMENTS_UPLOADED = "documents.uploaded"
    DOCUMENTS_CONVERTED = "documents.converted"
    DOCUMENTS_SECTIONED = "documents.sectioned"
    DOCUMENTS_CHUNKED = "documents.chunked"
    DOCUMENTS_INDEXED = "documents.indexed"
    DOCUMENTS_FAILED = "documents.failed"
    RUNS_REQUESTED = "runs.requested"


def _dumps(value: Any) -> bytes:
    return json.dumps(value, default=str).encode()


def inject_headers() -> list[tuple[str, bytes]]:
    """The current trace context as Kafka message headers (empty when there is no active span)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


def extract_context(headers: Iterable[tuple[str, bytes]] | None) -> Context:
    """The trace context carried by Kafka message headers (the current context when there is none)."""
    return propagate.extract({k: v.decode() for k, v in headers or ()})


class JsonProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=_dumps)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        attrs = {"messaging.system": "kafka", "messaging.destination.name": topic, "messaging.operation.type": "send"}
        with tracer.start_as_current_span(f"{topic} send", kind=SpanKind.PRODUCER, attributes=attrs):
            await self._producer.send_and_wait(
                topic, value, key=key.encode() if key else None, headers=inject_headers() or None
            )


Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def handle_message(msg: Any, *, topic: str, group_id: str, handler: Handler) -> None:
    """Run `handler` for one consumed message inside a consumer span continuing the producer's trace.

    Handler exceptions are logged and recorded on the span, not re-raised: a
    poison message must not block the partition; the handler is responsible
    for recording failure (e.g. marking the document failed).
    """
    attrs = {
        "messaging.system": "kafka",
        "messaging.operation.type": "process",
        "messaging.destination.name": topic,
        "messaging.consumer.group.name": group_id,
        "messaging.destination.partition.id": str(msg.partition),
        "messaging.kafka.offset": msg.offset,
    }
    with tracer.start_as_current_span(
        f"{topic} process", context=extract_context(msg.headers), kind=SpanKind.CONSUMER, attributes=attrs
    ) as span:
        try:
            await handler(msg.value)
        except Exception as exc:  # noqa: BLE001 - we want the worker to keep going
            log.exception("handler failed topic=%s offset=%s", topic, msg.offset)
            span.record_exception(exc)
            span.set_status(StatusCode.ERROR, str(exc))


async def consume_forever(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    handler: Handler,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Consume `topic` and call `handler` per message. Commits after each handled message."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    await consumer.start()
    log.info("consuming topic=%s group=%s", topic, group_id)
    try:
        async for msg in consumer:
            if stop_event is not None and stop_event.is_set():
                break
            await handle_message(msg, topic=topic, group_id=group_id, handler=handler)
            await consumer.commit()
    finally:
        await consumer.stop()
