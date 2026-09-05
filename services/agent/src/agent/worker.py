"""Agent worker: consumes `runs.requested` and executes runs."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agent.runner import RunExecutor
from agent.settings import get_settings
from clinical_common.events import RunRequested
from clinical_common.kafka import Topics, consume_forever
from clinical_common.logging import configure_logging
from clinical_common.telemetry import configure_telemetry

log = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_telemetry(settings.service_name, settings)
    configure_logging(settings.log_level, settings.service_name)
    executor = RunExecutor(settings)

    async def handler(msg: dict[str, Any]) -> None:
        req = RunRequested.model_validate(msg)
        log.info("run %s conversation=%s org=%s", req.run_id, req.conversation_id, req.org_id)
        await executor.execute(req)

    await consume_forever(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=Topics.RUNS_REQUESTED,
        group_id="agent",
        handler=handler,
    )


if __name__ == "__main__":
    asyncio.run(main())
