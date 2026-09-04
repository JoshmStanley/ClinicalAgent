"""Run event schema.

A *run* is one agent execution triggered by one user message. Everything the
agent does is appended to the run's event log in the conversations service.
The SSE stream the client sees is a tail of that log, which is what makes
runs survive disconnects and lets a user resume where they left off.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RunEventType(StrEnum):
    RUN_STARTED = "run.started"
    THINKING_DELTA = "thinking.delta"
    TEXT_DELTA = "text.delta"
    TOOL_CALL = "tool.call"
    TOOL_RESULT = "tool.result"
    CITATION = "citation"
    CONCEPT_SHEET_UPDATED = "concept_sheet.updated"
    USAGE = "usage"
    RUN_COMPLETED = "run.completed"
    RUN_FAILED = "run.failed"


class RunEvent(BaseModel):
    type: RunEventType
    payload: dict[str, Any] = Field(default_factory=dict)


class RunRequested(BaseModel):
    """Kafka message published by conversations when a run is queued."""

    run_id: str
    conversation_id: str
    org_id: str
    user_id: str
    role: str


class Usage(BaseModel):
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0


class Citation(BaseModel):
    chunk_id: str
    document_id: str
    document_title: str = ""
    section_path: str = ""
    page: int | None = None
    snippet: str = ""
