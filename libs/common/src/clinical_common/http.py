"""Internal HTTP client that forwards the principal to other services."""

from __future__ import annotations

import httpx

from clinical_common.auth import Principal


def internal_client(base_url: str, principal: Principal, internal_token: str, **kw) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        headers=principal.internal_headers(internal_token),
        timeout=kw.pop("timeout", 30.0),
        **kw,
    )
