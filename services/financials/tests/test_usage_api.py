import httpx
import pytest

from clinical_common.auth import Principal
from financials.main import app, get_session, get_settings
from financials.settings import Settings


class Session:
    def __init__(self):
        self.rows = []

    def add(self, row):
        self.rows.append(row)

    async def commit(self):
        pass


@pytest.fixture
async def api():
    session = Session()

    async def get_test_session():
        yield session

    app.dependency_overrides[get_settings] = lambda: Settings(
        auth_mode="dev", internal_token="internal-secret", usage_writer_token="writer-secret"
    )
    app.dependency_overrides[get_session] = get_test_session
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield client, session
    app.dependency_overrides.clear()


async def test_member_cannot_write_usage_even_with_forwarded_principal(api):
    client, session = api
    headers = Principal("u", "o", "org:member").internal_headers("internal-secret")
    response = await client.post("/internal/usage", headers=headers, json={"run_id": "r", "model": "test"})
    assert response.status_code == 403
    assert session.rows == []
    assert (await client.post("/usage", headers=headers, json={"run_id": "r", "model": "test"})).status_code == 404


@pytest.mark.parametrize(
    "field", ["input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"]
)
async def test_negative_tokens_rejected(api, field):
    client, session = api
    headers = {
        **Principal("u", "o", "org:member").internal_headers("internal-secret"),
        "X-Usage-Writer-Token": "writer-secret",
    }
    response = await client.post("/internal/usage", headers=headers, json={"run_id": "r", "model": "test", field: -1})
    assert response.status_code == 422
    assert session.rows == []


async def test_worker_records_usage_for_original_user(api):
    client, session = api
    headers = {
        **Principal("u", "o", "org:member").internal_headers("internal-secret"),
        "X-Usage-Writer-Token": "writer-secret",
    }
    response = await client.post(
        "/internal/usage",
        headers=headers,
        json={
            "run_id": "r",
            "model": "claude-opus-5",
            "input_tokens": 1000,
        },
    )
    assert response.status_code == 201
    assert session.rows[0].user_id == "u"
    assert session.rows[0].org_id == "o"
    assert session.rows[0].cost_usd > 0


async def test_unconfigured_writer_fails_closed(api):
    client, session = api
    app.dependency_overrides[get_settings] = lambda: Settings(auth_mode="dev", usage_writer_token="")
    response = await client.post(
        "/internal/usage",
        headers={"X-Dev-User-Id": "u", "X-Dev-Org-Id": "o"},
        json={
            "run_id": "r",
            "model": "test",
        },
    )
    assert response.status_code == 503
    assert session.rows == []
