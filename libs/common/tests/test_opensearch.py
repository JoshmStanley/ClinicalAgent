from clinical_common.opensearch import rrf_merge


def test_rrf_prefers_items_ranked_in_both_lists():
    bm25 = [{"chunk_id": "a"}, {"chunk_id": "b"}, {"chunk_id": "c"}]
    knn = [{"chunk_id": "c"}, {"chunk_id": "b"}, {"chunk_id": "d"}]
    merged = rrf_merge([bm25, knn])
    assert {h["chunk_id"] for h in merged[:2]} == {"b", "c"}  # items in both lists outrank singletons
    assert all("score" in h for h in merged)
