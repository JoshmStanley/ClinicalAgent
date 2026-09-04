import math

import pytest

from clinical_common.embeddings import FakeEmbedder


@pytest.mark.asyncio
async def test_fake_embedder_is_deterministic_and_normalised():
    e = FakeEmbedder(dim=64)
    a, b = await e.embed(["randomized double-blind trial", "randomized double-blind trial"], "document")
    assert a == b
    assert math.isclose(math.sqrt(sum(x * x for x in a)), 1.0, rel_tol=1e-6)
