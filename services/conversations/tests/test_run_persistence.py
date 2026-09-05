"""Database guarantees: run with TEST_DATABASE_URL pointing to disposable Postgres.

Each test owns an isolated schema; CI provisions Postgres and always runs these.
"""

import asyncio
import os
import uuid

import httpx
import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from clinical_common.auth import Principal
from clinical_common.db import Base
from conversations.main import app, get_session
from conversations.models import Conversation, Run, RunEvent, RunEventBatch


@pytest.fixture
async def database_api():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for Postgres integration tests")
    schema = "test_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as conn:
        await conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with sessions() as session:
        conversation = Conversation(org_id="o", created_by="u")
        session.add(conversation)
        await session.flush()
        run = Run(conversation_id=conversation.id, org_id="o", user_id="u", role="org:member", status="running")
        session.add(run)
        await session.commit()
        run_id = run.id

    async def session_dependency():
        async with sessions() as session:
            yield session

    from conversations.main import get_principal

    app.dependency_overrides[get_session] = session_dependency
    app.dependency_overrides[get_principal] = lambda: Principal("u", "o", "org:member")
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            yield client, run_id, sessions
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await admin.dispose()


async def test_concurrent_batch_retries_append_once(database_api):
    client, run_id, sessions = database_api
    headers = {"X-Event-Batch-Id": str(uuid.uuid4())}
    payload = [{"type": "text.delta", "payload": {"text": "hello"}}]
    responses = await asyncio.gather(
        *(client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload) for _ in range(4))
    )
    assert all(r.status_code == 201 and r.json() == {"last_seq": 1} for r in responses)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(RunEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(RunEventBatch)) == 1
    changed = await client.post(
        f"/internal/runs/{run_id}/events",
        headers=headers,
        json=[
            {"type": "text.delta", "payload": {"text": "different"}},
        ],
    )
    assert changed.status_code == 409


async def test_completion_is_idempotent_and_cannot_be_reversed(database_api):
    client, run_id, sessions = database_api
    for _ in range(2):
        response = await client.patch(f"/internal/runs/{run_id}", json={"status": "completed"})
        assert response.status_code == 200
    failed = await client.patch(f"/internal/runs/{run_id}", json={"status": "failed", "error": "response lost"})
    assert failed.status_code == 409
    async with sessions() as session:
        assert (await session.get(Run, run_id)).status == "completed"
        events = (await session.scalars(select(RunEvent))).all()
        assert [event.type for event in events] == ["run.completed"]


async def test_batch_receipt_does_not_bypass_org_check(database_api):
    client, run_id, sessions = database_api
    from conversations.main import get_principal

    headers = {"X-Event-Batch-Id": str(uuid.uuid4())}
    payload = [{"type": "text.delta", "payload": {"text": "private"}}]
    assert (await client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload)).status_code == 201
    app.dependency_overrides[get_principal] = lambda: Principal("other", "other-org", "org:member")
    assert (await client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload)).status_code == 404


async def test_terminal_status_and_event_rollback_together(database_api):
    client, run_id, sessions = database_api

    async def fail_before_commit():
        async with sessions() as session:

            async def failed_commit():
                await session.flush()
                raise RuntimeError("simulated transaction failure")

            session.commit = failed_commit
            yield session

    app.dependency_overrides[get_session] = fail_before_commit
    with pytest.raises(RuntimeError, match="simulated transaction failure"):
        await client.patch(f"/internal/runs/{run_id}", json={"status": "completed"})
    async with sessions() as session:
        assert (await session.get(Run, run_id)).status == "running"
        assert await session.scalar(select(func.count()).select_from(RunEvent)) == 0


async def test_terminal_run_accepts_receipt_replay_but_not_new_events(database_api):
    client, run_id, sessions = database_api
    headers = {"X-Event-Batch-Id": str(uuid.uuid4())}
    payload = [{"type": "text.delta", "payload": {"text": "answer"}}]
    assert (await client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload)).status_code == 201
    assert (await client.patch(f"/internal/runs/{run_id}", json={"status": "completed"})).status_code == 200
    retry = await client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload)
    assert retry.status_code == 201
    assert retry.json() == {"last_seq": 1}
    headers = {"X-Event-Batch-Id": str(uuid.uuid4())}
    assert (await client.post(f"/internal/runs/{run_id}/events", headers=headers, json=payload)).status_code == 409
