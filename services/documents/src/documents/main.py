"""Documents service: upload to object storage, track ingestion status, hybrid search.

Search returns chunks with citation metadata (chunk_id, document, section,
page). Every query carries a server-side org_id filter.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from clinical_common.auth import Principal, principal_dependency
from clinical_common.db import Base, Database
from clinical_common.embeddings import Embedder, build_embedder
from clinical_common.kafka import JsonProducer, Topics
from clinical_common.logging import configure_logging
from clinical_common.opensearch import CHUNKS_INDEX, rrf_merge
from clinical_common.opensearch import client as os_client_factory
from clinical_common.storage import ObjectStore
from clinical_common.telemetry import configure_telemetry, instrument_app, shutdown_telemetry
from documents.models import STATUS_UPLOADED, Document
from documents.settings import Settings, get_settings

log = logging.getLogger(__name__)


class State:
    db: Database
    producer: JsonProducer
    store: ObjectStore
    embedder: Embedder
    opensearch: Any


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name)
    configure_telemetry(settings.service_name, settings)
    state.db = Database(settings.database_url)
    await state.db.create_all(Base)
    state.producer = JsonProducer(settings.kafka_bootstrap_servers)
    await state.producer.start()
    state.store = ObjectStore(
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        bucket=settings.s3_bucket,
    )
    await state.store.ensure_bucket()
    state.embedder = build_embedder(
        settings.embedding_provider, voyage_api_key=settings.voyage_api_key, voyage_model=settings.voyage_model
    )
    state.opensearch = os_client_factory(settings.opensearch_url)
    yield
    shutdown_telemetry()
    await state.producer.stop()
    await state.opensearch.close()
    await state.db.dispose()


app = FastAPI(title="documents", lifespan=lifespan)
instrument_app(app)
get_principal = principal_dependency(get_settings)


async def get_session():
    async for s in state.db.session():
        yield s


class DocumentOut(BaseModel):
    id: str
    title: str
    filename: str
    content_type: str
    size_bytes: int
    status: str
    error: str
    chunk_count: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, d: Document) -> DocumentOut:
        return cls(
            id=str(d.id),
            title=d.title,
            filename=d.filename,
            content_type=d.content_type,
            size_bytes=d.size_bytes,
            status=d.status,
            error=d.error,
            chunk_count=d.chunk_count,
            created_at=d.created_at,
            updated_at=d.updated_at,
        )


class StatusPatch(BaseModel):
    status: str
    error: str = ""
    chunk_count: int | None = None


class SearchIn(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=8, ge=1, le=50)
    document_ids: list[str] | None = None


class ChunkHit(BaseModel):
    chunk_id: str
    document_id: str
    document_title: str
    section_path: str
    page: int | None
    text: str
    score: float


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/documents", response_model=DocumentOut, status_code=201)
async def upload(
    file: UploadFile = File(...),
    title: str | None = None,
    p: Principal = Depends(get_principal),
    settings: Settings = Depends(get_settings),
    s: AsyncSession = Depends(get_session),
):
    data = await file.read()
    if len(data) > settings.max_upload_bytes:
        raise HTTPException(413, "File too large")
    if not data:
        raise HTTPException(400, "Empty file")
    doc_id = uuid.uuid4()
    filename = file.filename or "upload"
    key = f"{p.org_id}/{doc_id}/source/{filename}"
    content_type = file.content_type or "application/octet-stream"
    await state.store.put_bytes(key, data, content_type)
    doc = Document(
        id=doc_id,
        org_id=p.org_id,
        uploaded_by=p.user_id,
        title=title or filename,
        filename=filename,
        content_type=content_type,
        storage_key=key,
        size_bytes=len(data),
        status=STATUS_UPLOADED,
    )
    s.add(doc)
    await s.commit()
    await state.producer.send(
        Topics.DOCUMENTS_UPLOADED,
        {
            "document_id": str(doc_id),
            "org_id": p.org_id,
            "storage_key": key,
            "content_type": content_type,
            "filename": filename,
            "title": doc.title,
        },
        key=str(doc_id),
    )
    return DocumentOut.from_row(doc)


@app.get("/documents", response_model=list[DocumentOut])
async def list_documents(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    rows = (
        await s.execute(select(Document).where(Document.org_id == p.org_id).order_by(Document.created_at.desc()))
    ).scalars()
    return [DocumentOut.from_row(d) for d in rows]


@app.get("/documents/{document_id}", response_model=DocumentOut)
async def get_document(
    document_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    doc = await s.get(Document, document_id)
    if not doc or doc.org_id != p.org_id:
        raise HTTPException(404, "Document not found")
    return DocumentOut.from_row(doc)


@app.patch("/internal/documents/{document_id}/status", response_model=DocumentOut)
async def patch_status(
    document_id: uuid.UUID,
    body: StatusPatch,
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    """Called by ingestion workers as a document moves through the pipeline."""
    doc = await s.get(Document, document_id)
    if not doc or doc.org_id != p.org_id:
        raise HTTPException(404, "Document not found")
    doc.status = body.status
    doc.error = body.error
    if body.chunk_count is not None:
        doc.chunk_count = body.chunk_count
    await s.commit()
    return DocumentOut.from_row(doc)


@app.post("/search", response_model=list[ChunkHit])
async def search(body: SearchIn, p: Principal = Depends(get_principal)):
    """Hybrid BM25 + kNN search fused with reciprocal rank fusion."""
    filters: list[dict[str, Any]] = [{"term": {"org_id": p.org_id}}]
    if body.document_ids:
        filters.append({"terms": {"document_id": body.document_ids}})
    k = body.top_k * 3
    source = ["chunk_id", "document_id", "document_title", "section_path", "page", "text"]

    (qvec,) = await state.embedder.embed([body.query], "query")
    knn_body = {
        "size": k,
        "_source": source,
        "query": {"knn": {"embedding": {"vector": qvec, "k": k, "filter": {"bool": {"filter": filters}}}}},
    }
    bm25_body = {
        "size": k,
        "_source": source,
        "query": {"bool": {"must": [{"match": {"text": body.query}}], "filter": filters}},
    }
    try:
        knn_res = await state.opensearch.search(index=CHUNKS_INDEX, body=knn_body)
        bm25_res = await state.opensearch.search(index=CHUNKS_INDEX, body=bm25_body)
    except Exception as exc:  # index may not exist yet
        log.warning("search failed: %s", exc)
        return []

    def hits(res: dict[str, Any]) -> list[dict[str, Any]]:
        return [h["_source"] for h in res["hits"]["hits"]]

    merged = rrf_merge([bm25_res and hits(bm25_res), knn_res and hits(knn_res)])[: body.top_k]
    return [ChunkHit(**m) for m in merged]
