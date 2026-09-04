from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from clinical_common.events import RunEvent as RunEventIn  # noqa: F401  (re-export)


class ProgramIn(BaseModel):
    name: str = Field(min_length=1)
    description: str = ""


class ProgramOut(ProgramIn):
    id: str
    created_at: datetime


class StudyIn(BaseModel):
    name: str = Field(min_length=1)
    program_id: str | None = None
    phase: str = ""
    indication: str = ""
    description: str = ""


class StudyOut(StudyIn):
    id: str
    status: str
    created_at: datetime
    updated_at: datetime


class ConceptSheetIn(BaseModel):
    content: dict[str, Any]


class ConceptSheetOut(ConceptSheetIn):
    study_id: str
    version: int
    created_by: str
    created_at: datetime


class ConversationIn(BaseModel):
    title: str = "New conversation"
    study_id: str | None = None


class ConversationOut(ConversationIn):
    id: str
    created_at: datetime
    updated_at: datetime


class MessageOut(BaseModel):
    id: str
    seq: int
    role: str
    text: str
    citations: list[dict[str, Any]]
    run_id: str | None
    created_at: datetime


class UserMessageIn(BaseModel):
    text: str = Field(min_length=1)


class SendResult(BaseModel):
    message: MessageOut
    run_id: str


class RunOut(BaseModel):
    id: str
    conversation_id: str
    status: str
    error: str
    usage: dict[str, Any]
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class RunPatch(BaseModel):
    status: str | None = None
    error: str | None = None
    usage: dict[str, Any] | None = None


class AssistantMessageIn(BaseModel):
    text: str
    citations: list[dict[str, Any]] = Field(default_factory=list)
    run_id: str


class RunEventOut(BaseModel):
    seq: int
    type: str
    payload: dict[str, Any]
    created_at: datetime


class RunContext(BaseModel):
    """Everything the agent needs to execute a run."""

    run: RunOut
    conversation: ConversationOut
    study: StudyOut | None
    concept_sheet: ConceptSheetOut | None
    messages: list[MessageOut]
