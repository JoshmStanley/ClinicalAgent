"""Ingestion workers. Run one stage per process:

    python -m ingestion.worker converter|sections|chunker|embed

Each stage consumes one Kafka topic, reads its input artifact from object
storage, writes its output artifact next to it, and publishes the next topic.
Failures mark the document `failed` via the documents service.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from clinical_common.auth import ROLE_ADMIN, Principal
from clinical_common.embeddings import build_embedder
from clinical_common.kafka import JsonProducer, Topics, consume_forever
from clinical_common.logging import configure_logging
from clinical_common.opensearch import CHUNKS_INDEX, ensure_chunks_index
from clinical_common.opensearch import client as os_client_factory
from clinical_common.storage import ObjectStore
from ingestion.chunk import chunk_sections
from ingestion.convert import convert
from ingestion.sections import split_sections
from ingestion.settings import Settings, get_settings

log = logging.getLogger(__name__)


class Ctx:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = ObjectStore(
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            bucket=settings.s3_bucket,
        )
        self.producer = JsonProducer(settings.kafka_bootstrap_servers)

    async def set_status(
        self, msg: dict[str, Any], status: str, error: str = "", chunk_count: int | None = None
    ) -> None:
        principal = Principal(user_id="ingestion", org_id=msg["org_id"], role=ROLE_ADMIN)
        async with httpx.AsyncClient(
            base_url=self.settings.documents_url,
            headers=principal.internal_headers(self.settings.internal_token),
            timeout=15,
        ) as c:
            r = await c.patch(
                f"/internal/documents/{msg['document_id']}/status",
                json={"status": status, "error": error, "chunk_count": chunk_count},
            )
            r.raise_for_status()

    def artifact_key(self, msg: dict[str, Any], name: str) -> str:
        return f"{msg['org_id']}/{msg['document_id']}/{name}"


Stage = Callable[[Ctx, dict[str, Any]], Awaitable[None]]


async def stage_converter(ctx: Ctx, msg: dict[str, Any]) -> None:
    data = await ctx.store.get_bytes(msg["storage_key"])
    pages = convert(data, msg.get("content_type", ""), msg.get("filename", ""))
    key = ctx.artifact_key(msg, "converted.json")
    await ctx.store.put_json(key, pages)
    await ctx.set_status(msg, "converted")
    await ctx.producer.send(
        Topics.DOCUMENTS_CONVERTED, {**msg, "artifact_key": key, "page_count": len(pages)}, key=msg["document_id"]
    )


async def stage_sections(ctx: Ctx, msg: dict[str, Any]) -> None:
    pages = await ctx.store.get_json(msg["artifact_key"])
    sections = split_sections(pages)
    key = ctx.artifact_key(msg, "sectioned.json")
    await ctx.store.put_json(key, sections)
    await ctx.set_status(msg, "sectioned")
    await ctx.producer.send(
        Topics.DOCUMENTS_SECTIONED, {**msg, "artifact_key": key, "section_count": len(sections)}, key=msg["document_id"]
    )


async def stage_chunker(ctx: Ctx, msg: dict[str, Any]) -> None:
    sections = await ctx.store.get_json(msg["artifact_key"])
    chunks = chunk_sections(
        msg["document_id"], sections, ctx.settings.chunk_target_words, ctx.settings.chunk_overlap_words
    )
    key = ctx.artifact_key(msg, "chunked.json")
    await ctx.store.put_json(key, chunks)
    await ctx.set_status(msg, "chunked")
    await ctx.producer.send(
        Topics.DOCUMENTS_CHUNKED, {**msg, "artifact_key": key, "chunk_count": len(chunks)}, key=msg["document_id"]
    )


class EmbedStage:
    def __init__(self, ctx: Ctx) -> None:
        self.ctx = ctx
        s = ctx.settings
        self.embedder = build_embedder(
            s.embedding_provider, voyage_api_key=s.voyage_api_key, voyage_model=s.voyage_model
        )
        self.opensearch = os_client_factory(s.opensearch_url)

    async def start(self) -> None:
        await ensure_chunks_index(self.opensearch, self.embedder.dim)

    async def __call__(self, ctx: Ctx, msg: dict[str, Any]) -> None:
        chunks: list[dict[str, Any]] = await ctx.store.get_json(msg["artifact_key"])
        batch = ctx.settings.embed_batch_size
        actions: list[dict[str, Any]] = []
        for i in range(0, len(chunks), batch):
            part = chunks[i : i + batch]
            vectors = await self.embedder.embed([c["text"] for c in part], "document")
            for c, v in zip(part, vectors, strict=True):
                actions.append({"index": {"_index": CHUNKS_INDEX, "_id": c["chunk_id"]}})
                actions.append(
                    {
                        **c,
                        "org_id": msg["org_id"],
                        "document_id": msg["document_id"],
                        "document_title": msg.get("title", ""),
                        "embedding": v,
                    }
                )
        if actions:
            resp = await self.opensearch.bulk(body=actions, refresh=True)
            if resp.get("errors"):
                failed = [i for i in resp["items"] if "error" in i.get("index", {})]
                raise RuntimeError(f"bulk index errors: {failed[:3]}")
        await ctx.set_status(msg, "indexed", chunk_count=len(chunks))
        await ctx.producer.send(Topics.DOCUMENTS_INDEXED, {**msg, "chunk_count": len(chunks)}, key=msg["document_id"])


STAGES: dict[str, tuple[str, str]] = {
    "converter": (Topics.DOCUMENTS_UPLOADED, "ingestion-converter"),
    "sections": (Topics.DOCUMENTS_CONVERTED, "ingestion-sections"),
    "chunker": (Topics.DOCUMENTS_SECTIONED, "ingestion-chunker"),
    "embed": (Topics.DOCUMENTS_CHUNKED, "ingestion-embed"),
}


async def main(stage_name: str) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, f"ingestion-{stage_name}")
    ctx = Ctx(settings)
    await ctx.producer.start()
    if stage_name == "embed":
        embed = EmbedStage(ctx)
        await embed.start()
        stage: Stage = embed
    else:
        stage = {"converter": stage_converter, "sections": stage_sections, "chunker": stage_chunker}[stage_name]
    topic, group = STAGES[stage_name]

    async def handler(msg: dict[str, Any]) -> None:
        log.info("%s document=%s", stage_name, msg.get("document_id"))
        try:
            await stage(ctx, msg)
        except Exception as exc:
            log.exception("%s failed document=%s", stage_name, msg.get("document_id"))
            try:
                await ctx.set_status(msg, "failed", error=f"{stage_name}: {exc}")
                await ctx.producer.send(
                    Topics.DOCUMENTS_FAILED, {**msg, "stage": stage_name, "error": str(exc)}, key=msg["document_id"]
                )
            except Exception:
                log.exception("could not record failure")

    try:
        await consume_forever(
            bootstrap_servers=settings.kafka_bootstrap_servers, topic=topic, group_id=group, handler=handler
        )
    finally:
        await ctx.producer.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in STAGES:
        print(f"usage: python -m ingestion.worker {'|'.join(STAGES)}", file=sys.stderr)
        sys.exit(2)
    asyncio.run(main(sys.argv[1]))
