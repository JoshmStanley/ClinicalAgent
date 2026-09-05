"""API gateway.

Verifies the caller (Clerk JWT in production, X-Dev-* headers locally),
forwards the principal to internal services with a shared token, enforces
budget checks before a run is queued, and streams SSE responses through
without buffering.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from clinical_common.auth import Principal, principal_dependency
from clinical_common.logging import configure_logging
from gateway.settings import Settings, get_settings

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-authorization",
    "proxy-authenticate",
    "host",
    "content-length",
}


class State:
    http: httpx.AsyncClient


state = State()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name)
    state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=None))
    yield
    await state.http.aclose()


app = FastAPI(title="ClinicalAgent API", lifespan=lifespan)
settings_ = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings_.cors_origins.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
get_principal = principal_dependency(get_settings)


def route_for(first_segment: str, settings: Settings) -> str:
    table = {
        "programs": settings.conversations_url,
        "studies": settings.conversations_url,
        "conversations": settings.conversations_url,
        "runs": settings.conversations_url,
        "documents": settings.documents_url,
        "search": settings.documents_url,
        "budgets": settings.financials_url,
        "usage": settings.financials_url,
        "me": settings.identity_url,
        "orgs": settings.identity_url,
    }
    if first_segment not in table:
        raise HTTPException(404, "Unknown route")
    return table[first_segment]


def _forward_headers(request: Request, principal: Principal, settings: Settings) -> dict[str, str]:
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP
        and not k.lower().startswith(("x-dev-", "x-principal-"))
        and k.lower() not in {"authorization", "x-internal-token", "x-usage-writer-token"}
    }
    headers.update(principal.internal_headers(settings.internal_token))
    return headers


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/conversations/{conversation_id}/messages")
async def send_message(
    conversation_id: str,
    request: Request,
    principal: Principal = Depends(get_principal),
    settings: Settings = Depends(get_settings),
):
    """Budget gate, then queue the run."""
    hdrs = principal.internal_headers(settings.internal_token)
    check = await state.http.post(f"{settings.financials_url}/check", headers=hdrs)
    check.raise_for_status()
    verdict = check.json()
    if not verdict["allowed"]:
        raise HTTPException(
            402, {"message": "Usage budget exhausted", "reason": verdict["reason"], "budgets": verdict["budgets"]}
        )
    r = await state.http.post(
        f"{settings.conversations_url}/conversations/{conversation_id}/messages",
        headers={**hdrs, "content-type": "application/json"},
        content=await request.body(),
    )
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))


@app.get("/api/runs/{run_id}/stream")
async def stream_run(
    run_id: str,
    request: Request,
    principal: Principal = Depends(get_principal),
    settings: Settings = Depends(get_settings),
):
    """SSE passthrough. Honors Last-Event-ID and ?after= for resume."""
    headers = _forward_headers(request, principal, settings)
    url = f"{settings.conversations_url}/runs/{run_id}/stream"
    req = state.http.build_request("GET", url, headers=headers, params=dict(request.query_params))
    upstream = await state.http.send(req, stream=True)
    if upstream.status_code != 200:
        body = await upstream.aread()
        await upstream.aclose()
        return Response(content=body, status_code=upstream.status_code, media_type=upstream.headers.get("content-type"))

    async def gen():
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
        finally:
            await upstream.aclose()

    return StreamingResponse(
        gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(
    path: str,
    request: Request,
    principal: Principal = Depends(get_principal),
    settings: Settings = Depends(get_settings),
):
    if path.startswith("internal"):
        raise HTTPException(404, "Unknown route")
    base = route_for(path.split("/", 1)[0], settings)
    r = await state.http.request(
        request.method,
        f"{base}/{path}",
        headers=_forward_headers(request, principal, settings),
        params=dict(request.query_params),
        content=await request.body(),
    )
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))
