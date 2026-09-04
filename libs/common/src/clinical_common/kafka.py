"""Thin JSON wrappers over aiokafka."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

log = logging.getLogger(__name__)


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


class JsonProducer:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=_dumps)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, value: dict[str, Any], key: str | None = None) -> None:
        await self._producer.send_and_wait(topic, value, key=key.encode() if key else None)


Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def consume_forever(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    handler: Handler,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Consume `topic` and call `handler` per message. Commits after each handled message.

    Handler exceptions are logged and the message is committed anyway so a
    poison message does not block the partition; the handler is responsible
    for recording failure (e.g. marking the document failed).
    """
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
            try:
                await handler(msg.value)
            except Exception:  # noqa: BLE001 - we want the worker to keep going
                log.exception("handler failed topic=%s offset=%s", topic, msg.offset)
            await consumer.commit()
    finally:
        await consumer.stop()
