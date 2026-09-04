"""Embedding providers.

Ingestion (documents) and search (queries) must use the same provider and
model so vectors are comparable. Both import from here.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Literal, Protocol

InputType = Literal["document", "query"]


class Embedder(Protocol):
    dim: int
    name: str

    async def embed(self, texts: list[str], input_type: InputType) -> list[list[float]]: ...


class FakeEmbedder:
    """Deterministic hashed bag-of-words embedding. Local pipeline testing only."""

    name = "fake"

    def __init__(self, dim: int = 256) -> None:
        self.dim = dim

    async def embed(self, texts: list[str], input_type: InputType) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            vec = [0.0] * self.dim
            for tok in re.findall(r"[a-z0-9]+", text.lower()):
                h = int(hashlib.md5(tok.encode()).hexdigest(), 16)  # noqa: S324
                vec[h % self.dim] += 1.0 if (h >> 8) % 2 else -1.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return out


class VoyageEmbedder:
    name = "voyage"
    _DIMS = {"voyage-3": 1024, "voyage-3-lite": 512, "voyage-3-large": 1024}

    def __init__(self, api_key: str, model: str = "voyage-3") -> None:
        import voyageai

        self._client = voyageai.AsyncClient(api_key=api_key)
        self.model = model
        self.dim = self._DIMS.get(model, 1024)

    async def embed(self, texts: list[str], input_type: InputType) -> list[list[float]]:
        result = await self._client.embed(texts, model=self.model, input_type=input_type)
        return result.embeddings


def build_embedder(provider: str, *, voyage_api_key: str = "", voyage_model: str = "voyage-3") -> Embedder:
    if provider == "voyage":
        if not voyage_api_key:
            raise RuntimeError("EMBEDDING_PROVIDER=voyage requires VOYAGE_API_KEY")
        return VoyageEmbedder(voyage_api_key, voyage_model)
    return FakeEmbedder()
