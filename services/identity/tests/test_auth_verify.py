from fastapi.testclient import TestClient

from identity.main import app
from identity.settings import Settings, get_settings


def _client(**overrides) -> TestClient:
    app.dependency_overrides[get_settings] = lambda: Settings(**overrides)
    return TestClient(app)  # no lifespan: /auth/verify needs no database


def test_verify_dev_headers_sets_principal_headers():
    c = _client(auth_mode="dev", internal_token="secret")
    r = c.get("/auth/verify", headers={"X-Dev-User-Id": "u1", "X-Dev-Org-Id": "o1", "X-Dev-Role": "org:admin"})
    assert r.status_code == 204
    assert r.headers["X-Internal-Token"] == "secret"
    assert r.headers["X-Principal-Org"] == "o1"
    assert r.headers["X-Principal-Role"] == "org:admin"


def test_verify_rejects_missing_credentials():
    c = _client(auth_mode="dev")
    assert c.get("/auth/verify").status_code == 401


def test_verify_ignores_smuggled_principal_headers():
    c = _client(auth_mode="clerk")
    r = c.get("/auth/verify", headers={"X-Internal-Token": "x", "X-Principal-Org": "evil"})
    assert r.status_code == 401
