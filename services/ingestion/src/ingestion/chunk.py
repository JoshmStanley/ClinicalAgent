"""Stage 3: sections -> overlapping word-window chunks that never cross a section."""

from __future__ import annotations


def chunk_sections(
    document_id: str, sections: list[dict], target_words: int = 350, overlap_words: int = 50
) -> list[dict]:
    chunks: list[dict] = []
    ordinal = 0
    step = max(target_words - overlap_words, 1)
    for sec in sections:
        words = sec["text"].split()
        if not words:
            continue
        start = 0
        while start < len(words):
            window = words[start : start + target_words]
            chunks.append(
                {
                    "chunk_id": f"{document_id}:{ordinal}",
                    "ordinal": ordinal,
                    "section_path": sec["path"],
                    "page": sec.get("page_start"),
                    "text": " ".join(window),
                }
            )
            ordinal += 1
            if start + target_words >= len(words):
                break
            start += step
    return chunks
