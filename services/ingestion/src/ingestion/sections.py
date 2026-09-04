"""Stage 2: pages -> sections keyed by headings.

Heuristic heading detection tuned for clinical protocols and study reports:
numbered headings ("3.2 Study Design"), short ALL-CAPS lines, and a list of
headings that appear in nearly every protocol.
"""

from __future__ import annotations

import re

KNOWN_HEADINGS = [
    "synopsis",
    "protocol summary",
    "background",
    "rationale",
    "objectives",
    "endpoints",
    "study design",
    "study population",
    "inclusion criteria",
    "exclusion criteria",
    "investigational product",
    "treatment",
    "dosing",
    "randomization",
    "blinding",
    "schedule of assessments",
    "efficacy assessments",
    "safety assessments",
    "adverse events",
    "statistical considerations",
    "statistical methods",
    "sample size",
    "interim analysis",
    "data monitoring",
    "ethics",
    "references",
    "appendix",
    "results",
    "discussion",
    "conclusion",
]
_NUMBERED = re.compile(r"^\s*(\d+(\.\d+){0,3})\.?\s+([A-Z][^\n]{2,80})$")
_CAPS = re.compile(r"^\s*[A-Z][A-Z0-9 ,&/\-()]{3,60}$")


def is_heading(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or len(stripped) > 90:
        return None
    if _NUMBERED.match(stripped):
        return stripped
    if _CAPS.match(stripped) and len(stripped.split()) <= 10:
        return stripped.title()
    low = stripped.lower().rstrip(":")
    if low in KNOWN_HEADINGS:
        return stripped.rstrip(":")
    return None


def split_sections(pages: list[dict]) -> list[dict]:
    sections: list[dict] = []
    current = {"path": "Front matter", "page_start": pages[0]["page"] if pages else 1, "lines": []}
    for page in pages:
        for line in page["text"].splitlines():
            heading = is_heading(line)
            if heading:
                if current["lines"]:
                    sections.append(current)
                current = {"path": heading, "page_start": page["page"], "lines": []}
            else:
                current["lines"].append(line)
    if current["lines"]:
        sections.append(current)
    out = []
    for sec in sections:
        text = "\n".join(sec["lines"]).strip()
        if text:
            out.append({"path": sec["path"], "page_start": sec["page_start"], "text": text})
    return out
