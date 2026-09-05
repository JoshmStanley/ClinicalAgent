import httpx

from gateway.main import app


async def test_public_proxy_cannot_reach_internal_usage():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/internal/usage",
            headers={
                "X-Dev-User-Id": "u",
                "X-Dev-Org-Id": "o",
                "X-Usage-Writer-Token": "untrusted",
            },
            json={"run_id": "r", "model": "test"},
        )
    assert response.status_code == 404
