"""Stage 1: raw file -> list of pages of plain text."""

from __future__ import annotations

import io

from pypdf import PdfReader


def convert(data: bytes, content_type: str, filename: str) -> list[dict]:
    name = filename.lower()
    if content_type == "application/pdf" or name.endswith(".pdf"):
        reader = PdfReader(io.BytesIO(data))
        return [{"page": i + 1, "text": (page.extract_text() or "")} for i, page in enumerate(reader.pages)]
    # Plain text / markdown: one logical page.
    return [{"page": 1, "text": data.decode("utf-8", errors="replace")}]
