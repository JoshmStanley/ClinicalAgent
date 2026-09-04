"""Conversations service.

Owns the study workspace (programs, studies, concept sheets), conversations
and messages, and the *run event log*. The SSE endpoint tails the log, so a
client can disconnect and resume with `Last-Event-ID` and a run keeps going
regardless of whether anyone is watching.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from clinical_common.auth import Principal, principal_dependency
from clinical_common.db import Base, Database, utcnow
from clinical_common.events import RunEvent as RunEventIn
from clinical_common.events import RunRequested
from clinical_common.kafka import JsonProducer, Topics
from clinical_common.logging import configure_logging
from conversations.models import (
    RUN_QUEUED,
    RUN_TERMINAL,
    ConceptSheet,
    Conversation,
    Message,
    Program,
    Run,
    RunEvent,
    Study,
)
from conversations.schemas import (
    AssistantMessageIn,
    ConceptSheetIn,
    ConceptSheetOut,
    ConversationIn,
    ConversationOut,
    MessageOut,
    ProgramIn,
    ProgramOut,
    RunContext,
    RunEventOut,
    RunOut,
    RunPatch,
    SendResult,
    StudyIn,
    StudyOut,
    UserMessageIn,
)
from conversations.settings import Settings, get_settings

log = logging.getLogger(__name__)


class State:
    db: Database
    producer: JsonProducer


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name)
    state.db = Database(settings.database_url)
    await state.db.create_all(Base)
    state.producer = JsonProducer(settings.kafka_bootstrap_servers)
    await state.producer.start()
    yield
    await state.producer.stop()
    await state.db.dispose()


app = FastAPI(title="conversations", lifespan=lifespan)
get_principal = principal_dependency(get_settings)


async def get_session():
    async for s in state.db.session():
        yield s


# ------------------------------------------------------------------ mappers


def program_out(p: Program) -> ProgramOut:
    return ProgramOut(id=str(p.id), name=p.name, description=p.description, created_at=p.created_at)


def study_out(s: Study) -> StudyOut:
    return StudyOut(
        id=str(s.id),
        name=s.name,
        program_id=str(s.program_id) if s.program_id else None,
        phase=s.phase,
        indication=s.indication,
        description=s.description,
        status=s.status,
        created_at=s.created_at,
        updated_at=s.updated_at,
    )


def sheet_out(c: ConceptSheet) -> ConceptSheetOut:
    return ConceptSheetOut(
        study_id=str(c.study_id), version=c.version, content=c.content, created_by=c.created_by, created_at=c.created_at
    )


def conversation_out(c: Conversation) -> ConversationOut:
    return ConversationOut(
        id=str(c.id),
        title=c.title,
        study_id=str(c.study_id) if c.study_id else None,
        created_at=c.created_at,
        updated_at=c.updated_at,
    )


def message_out(m: Message) -> MessageOut:
    return MessageOut(
        id=str(m.id),
        seq=m.seq,
        role=m.role,
        text=m.text,
        citations=m.citations or [],
        run_id=str(m.run_id) if m.run_id else None,
        created_at=m.created_at,
    )


def run_out(r: Run) -> RunOut:
    return RunOut(
        id=str(r.id),
        conversation_id=str(r.conversation_id),
        status=r.status,
        error=r.error,
        usage=r.usage or {},
        created_at=r.created_at,
        started_at=r.started_at,
        finished_at=r.finished_at,
    )


async def _study(s: AsyncSession, study_id: uuid.UUID, p: Principal) -> Study:
    st = await s.get(Study, study_id)
    if not st or st.org_id != p.org_id:
        raise HTTPException(404, "Study not found")
    return st


async def _conversation(s: AsyncSession, conversation_id: uuid.UUID, p: Principal) -> Conversation:
    c = await s.get(Conversation, conversation_id)
    if not c or c.org_id != p.org_id:
        raise HTTPException(404, "Conversation not found")
    return c


async def _run(s: AsyncSession, run_id: uuid.UUID, p: Principal) -> Run:
    r = await s.get(Run, run_id)
    if not r or r.org_id != p.org_id:
        raise HTTPException(404, "Run not found")
    return r


async def _latest_sheet(s: AsyncSession, study_id: uuid.UUID) -> ConceptSheet | None:
    return await s.scalar(
        select(ConceptSheet).where(ConceptSheet.study_id == study_id).order_by(ConceptSheet.version.desc()).limit(1)
    )


async def _next_seq(s: AsyncSession, conversation_id: uuid.UUID) -> int:
    return (
        await s.scalar(
            select(func.coalesce(func.max(Message.seq), 0)).where(Message.conversation_id == conversation_id)
        )
    ) + 1


# ------------------------------------------------------------------ programs


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/programs", response_model=ProgramOut, status_code=201)
async def create_program(
    body: ProgramIn, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    prog = Program(org_id=p.org_id, name=body.name, description=body.description, created_by=p.user_id)
    s.add(prog)
    await s.commit()
    return program_out(prog)


@app.get("/programs", response_model=list[ProgramOut])
async def list_programs(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    rows = (await s.execute(select(Program).where(Program.org_id == p.org_id).order_by(Program.created_at))).scalars()
    return [program_out(x) for x in rows]


@app.get("/programs/{program_id}/studies", response_model=list[StudyOut])
async def program_studies(
    program_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    rows = (
        await s.execute(
            select(Study).where(Study.org_id == p.org_id, Study.program_id == program_id).order_by(Study.created_at)
        )
    ).scalars()
    return [study_out(x) for x in rows]


# ------------------------------------------------------------------- studies


@app.post("/studies", response_model=StudyOut, status_code=201)
async def create_study(body: StudyIn, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    program_id = None
    if body.program_id:
        prog = await s.get(Program, uuid.UUID(body.program_id))
        if not prog or prog.org_id != p.org_id:
            raise HTTPException(404, "Program not found")
        program_id = prog.id
    st = Study(
        org_id=p.org_id,
        program_id=program_id,
        name=body.name,
        phase=body.phase,
        indication=body.indication,
        description=body.description,
        created_by=p.user_id,
    )
    s.add(st)
    await s.commit()
    return study_out(st)


@app.get("/studies", response_model=list[StudyOut])
async def list_studies(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    rows = (await s.execute(select(Study).where(Study.org_id == p.org_id).order_by(Study.updated_at.desc()))).scalars()
    return [study_out(x) for x in rows]


@app.get("/studies/{study_id}", response_model=StudyOut)
async def get_study(study_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    return study_out(await _study(s, study_id, p))


@app.get("/studies/{study_id}/concept-sheet", response_model=ConceptSheetOut | None)
async def get_concept_sheet(
    study_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    await _study(s, study_id, p)
    sheet = await _latest_sheet(s, study_id)
    return sheet_out(sheet) if sheet else None


@app.put("/studies/{study_id}/concept-sheet", response_model=ConceptSheetOut)
async def put_concept_sheet(
    study_id: uuid.UUID,
    body: ConceptSheetIn,
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    st = await _study(s, study_id, p)
    latest = await _latest_sheet(s, study_id)
    sheet = ConceptSheet(
        study_id=st.id, version=(latest.version + 1) if latest else 1, content=body.content, created_by=p.user_id
    )
    s.add(sheet)
    st.updated_at = utcnow()
    await s.commit()
    return sheet_out(sheet)


@app.get("/studies/{study_id}/concept-sheet/versions", response_model=list[ConceptSheetOut])
async def concept_sheet_versions(
    study_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    await _study(s, study_id, p)
    rows = (
        await s.execute(select(ConceptSheet).where(ConceptSheet.study_id == study_id).order_by(ConceptSheet.version))
    ).scalars()
    return [sheet_out(x) for x in rows]


# ------------------------------------------------------------- conversations


@app.post("/conversations", response_model=ConversationOut, status_code=201)
async def create_conversation(
    body: ConversationIn, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    study_id = None
    if body.study_id:
        study_id = (await _study(s, uuid.UUID(body.study_id), p)).id
    c = Conversation(org_id=p.org_id, study_id=study_id, title=body.title, created_by=p.user_id)
    s.add(c)
    await s.commit()
    return conversation_out(c)


@app.get("/conversations", response_model=list[ConversationOut])
async def list_conversations(
    study_id: uuid.UUID | None = None, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    q = select(Conversation).where(Conversation.org_id == p.org_id)
    if study_id:
        q = q.where(Conversation.study_id == study_id)
    rows = (await s.execute(q.order_by(Conversation.updated_at.desc()))).scalars()
    return [conversation_out(x) for x in rows]


@app.get("/conversations/{conversation_id}", response_model=ConversationOut)
async def get_conversation(
    conversation_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    return conversation_out(await _conversation(s, conversation_id, p))


@app.get("/conversations/{conversation_id}/messages", response_model=list[MessageOut])
async def list_messages(
    conversation_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    await _conversation(s, conversation_id, p)
    rows = (
        await s.execute(select(Message).where(Message.conversation_id == conversation_id).order_by(Message.seq))
    ).scalars()
    return [message_out(m) for m in rows]


@app.get("/conversations/{conversation_id}/runs", response_model=list[RunOut])
async def list_runs(
    conversation_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    """Lets a returning client find an in-flight run to reattach to."""
    await _conversation(s, conversation_id, p)
    rows = (
        await s.execute(select(Run).where(Run.conversation_id == conversation_id).order_by(Run.created_at.desc()))
    ).scalars()
    return [run_out(r) for r in rows]


@app.post("/conversations/{conversation_id}/messages", response_model=SendResult, status_code=202)
async def send_message(
    conversation_id: uuid.UUID,
    body: UserMessageIn,
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    """Store the user message, queue a run, and publish it for the agent."""
    c = await _conversation(s, conversation_id, p)
    active = await s.scalar(
        select(func.count()).select_from(Run).where(Run.conversation_id == c.id, Run.status.notin_(RUN_TERMINAL))
    )
    if active:
        raise HTTPException(409, "A run is already in progress for this conversation")
    run = Run(conversation_id=c.id, org_id=p.org_id, user_id=p.user_id, role=p.role, status=RUN_QUEUED)
    s.add(run)
    await s.flush()
    msg = Message(
        conversation_id=c.id,
        seq=await _next_seq(s, c.id),
        role="user",
        text=body.text,
        run_id=run.id,
        created_by=p.user_id,
    )
    s.add(msg)
    c.updated_at = utcnow()
    await s.commit()
    await state.producer.send(
        Topics.RUNS_REQUESTED,
        RunRequested(
            run_id=str(run.id), conversation_id=str(c.id), org_id=p.org_id, user_id=p.user_id, role=p.role
        ).model_dump(),
        key=str(c.id),
    )
    return SendResult(message=message_out(msg), run_id=str(run.id))


# ---------------------------------------------------------------------- runs


@app.get("/runs/{run_id}", response_model=RunOut)
async def get_run(run_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    return run_out(await _run(s, run_id, p))


@app.get("/runs/{run_id}/events", response_model=list[RunEventOut])
async def list_events(
    run_id: uuid.UUID,
    after: int = Query(0, ge=0),
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    await _run(s, run_id, p)
    rows = (
        await s.execute(select(RunEvent).where(RunEvent.run_id == run_id, RunEvent.seq > after).order_by(RunEvent.seq))
    ).scalars()
    return [RunEventOut(seq=e.seq, type=e.type, payload=e.payload, created_at=e.created_at) for e in rows]


@app.get("/runs/{run_id}/stream")
async def stream_run(
    run_id: uuid.UUID,
    request: Request,
    after: int = Query(0, ge=0),
    p: Principal = Depends(get_principal),
    settings: Settings = Depends(get_settings),
):
    """SSE tail of the run event log. Resumable via `after` or `Last-Event-ID`."""
    async with state.db.session_factory() as s:
        await _run(s, run_id, p)
    last_event_id = request.headers.get("Last-Event-ID")
    cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after

    async def gen():
        nonlocal cursor
        idle_for = 0.0
        while True:
            if await request.is_disconnected():
                return
            async with state.db.session_factory() as s:
                rows = (
                    (
                        await s.execute(
                            select(RunEvent)
                            .where(RunEvent.run_id == run_id, RunEvent.seq > cursor)
                            .order_by(RunEvent.seq)
                            .limit(500)
                        )
                    )
                    .scalars()
                    .all()
                )
                status = await s.scalar(select(Run.status).where(Run.id == run_id))
            for ev in rows:
                cursor = ev.seq
                yield f"id: {ev.seq}\nevent: {ev.type}\ndata: {json.dumps(ev.payload)}\n\n"
            if rows:
                idle_for = 0.0
                continue
            if status in RUN_TERMINAL:
                yield f"id: {cursor}\nevent: done\ndata: {json.dumps({'status': status})}\n\n"
                return
            idle_for += settings.stream_poll_seconds
            if idle_for >= settings.stream_keepalive_seconds:
                idle_for = 0.0
                yield ": keepalive\n\n"
            await asyncio.sleep(settings.stream_poll_seconds)

    return StreamingResponse(
        gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


# ------------------------------------------------------------ internal (agent)


@app.get("/internal/runs/{run_id}/context", response_model=RunContext)
async def run_context(run_id: uuid.UUID, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    run = await _run(s, run_id, p)
    conv = await _conversation(s, run.conversation_id, p)
    study = await s.get(Study, conv.study_id) if conv.study_id else None
    sheet = await _latest_sheet(s, study.id) if study else None
    msgs = (await s.execute(select(Message).where(Message.conversation_id == conv.id).order_by(Message.seq))).scalars()
    return RunContext(
        run=run_out(run),
        conversation=conversation_out(conv),
        study=study_out(study) if study else None,
        concept_sheet=sheet_out(sheet) if sheet else None,
        messages=[message_out(m) for m in msgs],
    )


@app.post("/internal/runs/{run_id}/events", status_code=201)
async def append_events(
    run_id: uuid.UUID,
    events: list[RunEventIn],
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    run = await s.scalar(select(Run).where(Run.id == run_id).with_for_update())
    if not run or run.org_id != p.org_id:
        raise HTTPException(404, "Run not found")
    seq = await s.scalar(select(func.coalesce(func.max(RunEvent.seq), 0)).where(RunEvent.run_id == run_id))
    for ev in events:
        seq += 1
        s.add(RunEvent(run_id=run_id, seq=seq, type=str(ev.type), payload=ev.payload))
    await s.commit()
    return {"last_seq": seq}


@app.patch("/internal/runs/{run_id}", response_model=RunOut)
async def patch_run(
    run_id: uuid.UUID, body: RunPatch, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)
):
    run = await _run(s, run_id, p)
    if body.status:
        run.status = body.status
        if body.status == "running" and not run.started_at:
            run.started_at = utcnow()
        if body.status in RUN_TERMINAL:
            run.finished_at = utcnow()
    if body.error is not None:
        run.error = body.error
    if body.usage is not None:
        run.usage = body.usage
    await s.commit()
    return run_out(run)


@app.post("/internal/conversations/{conversation_id}/messages", response_model=MessageOut, status_code=201)
async def append_assistant_message(
    conversation_id: uuid.UUID,
    body: AssistantMessageIn,
    p: Principal = Depends(get_principal),
    s: AsyncSession = Depends(get_session),
):
    c = await _conversation(s, conversation_id, p)
    msg = Message(
        conversation_id=c.id,
        seq=await _next_seq(s, c.id),
        role="assistant",
        text=body.text,
        citations=body.citations,
        run_id=uuid.UUID(body.run_id),
        created_by="agent",
    )
    s.add(msg)
    c.updated_at = utcnow()
    await s.commit()
    return message_out(msg)
