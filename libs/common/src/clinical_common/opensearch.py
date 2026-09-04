"""OpenSearch index definition for document chunks.

Shared by the ingestion `embed` stage (writes) and the documents service
(reads) so both agree on the mapping. Tenant isolation is a hard `org_id`
filter applied server-side on every query.
"""

from __future__ import annotations

from typing import Any

from opensearchpy import AsyncOpenSearch

CHUNKS_INDEX = "chunks"


def chunks_mapping(dim: int) -> dict[str, Any]:
    return {
        "settings": {"index": {"knn": True, "number_of_shards": 1, "number_of_replicas": 0}},
        "mappings": {
            "properties": {
                "org_id": {"type": "keyword"},
                "document_id": {"type": "keyword"},
                "document_title": {"type": "text"},
                "chunk_id": {"type": "keyword"},
                "section_path": {"type": "keyword"},
                "page": {"type": "integer"},
                "ordinal": {"type": "integer"},
                "text": {"type": "text"},
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dim,
                    "method": {"name": "hnsw", "space_type": "cosinesimil", "engine": "lucene"},
                },
            }
        },
    }


def client(url: str) -> AsyncOpenSearch:
    return AsyncOpenSearch(hosts=[url], use_ssl=url.startswith("https"), verify_certs=False)


async def ensure_chunks_index(os_client: AsyncOpenSearch, dim: int, index: str = CHUNKS_INDEX) -> None:
    if not await os_client.indices.exists(index=index):
        await os_client.indices.create(index=index, body=chunks_mapping(dim))


def rrf_merge(result_lists: list[list[dict[str, Any]]], k: int = 60, key: str = "chunk_id") -> list[dict[str, Any]]:
    """Reciprocal rank fusion of several ranked hit lists."""
    scores: dict[str, float] = {}
    by_key: dict[str, dict[str, Any]] = {}
    for hits in result_lists:
        for rank, hit in enumerate(hits, start=1):
            kid = hit[key]
            scores[kid] = scores.get(kid, 0.0) + 1.0 / (k + rank)
            by_key.setdefault(kid, hit)
    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    out = []
    for kid, score in ordered:
        hit = dict(by_key[kid])
        hit["score"] = score
        out.append(hit)
    return out
