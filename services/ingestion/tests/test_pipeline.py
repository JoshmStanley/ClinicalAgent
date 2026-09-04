from ingestion.chunk import chunk_sections
from ingestion.sections import is_heading, split_sections


def test_heading_detection():
    assert is_heading("3.2 Study Design") == "3.2 Study Design"
    assert is_heading("INCLUSION CRITERIA") == "Inclusion Criteria"
    assert is_heading("Objectives:") == "Objectives"
    assert is_heading("Subjects were randomized 1:1 to receive placebo.") is None


def test_split_sections_tracks_pages():
    pages = [
        {"page": 1, "text": "Synopsis\nA phase 2 trial.\n"},
        {"page": 2, "text": "2 Objectives\nPrimary: ORR at week 24.\n"},
    ]
    secs = split_sections(pages)
    assert [s["path"] for s in secs] == ["Synopsis", "2 Objectives"]
    assert secs[1]["page_start"] == 2


def test_chunker_overlaps_and_never_crosses_sections():
    text_a = " ".join(f"a{i}" for i in range(120))
    text_b = " ".join(f"b{i}" for i in range(10))
    chunks = chunk_sections(
        "doc",
        [{"path": "A", "page_start": 1, "text": text_a}, {"path": "B", "page_start": 3, "text": text_b}],
        target_words=50,
        overlap_words=10,
    )
    a_chunks = [c for c in chunks if c["section_path"] == "A"]
    assert len(a_chunks) == 3
    assert a_chunks[1]["text"].split()[0] == "a40"  # overlap of 10 words
    assert chunks[-1]["section_path"] == "B" and chunks[-1]["page"] == 3
    assert chunks[0]["chunk_id"] == "doc:0"
