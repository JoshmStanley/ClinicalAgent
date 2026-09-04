"""Identity service: mirrors Clerk users/orgs/memberships and exposes them internally."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from svix.webhooks import Webhook, WebhookVerificationError

from clinical_common.auth import Principal, principal_dependency, principal_from_edge, require_admin
from clinical_common.db import Base, Database
from clinical_common.logging import configure_logging
from identity.models import Membership, Org, User
from identity.settings import Settings, get_settings

log = logging.getLogger(__name__)
db: Database | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name)
    db = Database(settings.database_url)
    await db.create_all(Base)
    yield
    await db.dispose()


app = FastAPI(title="identity", lifespan=lifespan)
get_principal = principal_dependency(get_settings)


async def get_session():
    async for s in db.session():
        yield s


class UserOut(BaseModel):
    id: str
    email: str
    name: str


class MemberOut(BaseModel):
    user: UserOut
    role: str


class MeOut(BaseModel):
    user_id: str
    org_id: str
    role: str
    user: UserOut | None = None
    org_name: str | None = None


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/auth/verify")
@app.api_route("/auth/verify", methods=["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"], include_in_schema=False)
async def auth_verify(request: Request, settings: Settings = Depends(get_settings)):
    """Traefik ForwardAuth target.

    Traefik calls this with the original request's headers. A 2xx lets the
    request through and the headers listed in Traefik's `authResponseHeaders`
    are copied onto the upstream request; anything else is returned to the
    client as-is. We always set all four principal headers so a client can
    never smuggle its own.
    """
    principal = principal_from_edge(request, settings)
    return Response(status_code=204, headers=principal.internal_headers(settings.internal_token))


@app.get("/me", response_model=MeOut)
async def me(principal: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    user = await s.get(User, principal.user_id)
    org = await s.get(Org, principal.org_id)
    return MeOut(
        user_id=principal.user_id,
        org_id=principal.org_id,
        role=principal.role,
        user=UserOut(id=user.id, email=user.email, name=user.name) if user else None,
        org_name=org.name if org else None,
    )


@app.get("/orgs/members", response_model=list[MemberOut])
async def list_members(principal: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    require_admin(principal)
    rows = await s.execute(
        select(Membership, User).join(User, User.id == Membership.user_id).where(Membership.org_id == principal.org_id)
    )
    return [MemberOut(user=UserOut(id=u.id, email=u.email, name=u.name), role=m.role) for m, u in rows]


# --------------------------------------------------------------- Clerk sync


def _upsert_user(data: dict[str, Any]) -> User:
    emails = data.get("email_addresses") or []
    primary = data.get("primary_email_address_id")
    email = next((e["email_address"] for e in emails if e.get("id") == primary), None)
    if email is None and emails:
        email = emails[0].get("email_address", "")
    name = " ".join(p for p in [data.get("first_name"), data.get("last_name")] if p)
    return User(id=data["id"], email=email or "", name=name)


@app.post("/webhooks/clerk")
async def clerk_webhook(
    request: Request,
    settings: Settings = Depends(get_settings),
    s: AsyncSession = Depends(get_session),
):
    payload = await request.body()
    if settings.clerk_webhook_secret:
        try:
            event = Webhook(settings.clerk_webhook_secret).verify(payload, dict(request.headers))
        except WebhookVerificationError as exc:
            raise HTTPException(400, f"Invalid webhook signature: {exc}") from exc
    elif settings.auth_mode == "dev":
        event = await request.json()  # unsigned webhooks are only accepted in dev mode
    else:
        raise HTTPException(500, "CLERK_WEBHOOK_SECRET not configured")

    kind: str = event.get("type", "")
    data: dict[str, Any] = event.get("data", {})
    log.info("clerk webhook %s", kind)

    if kind in ("user.created", "user.updated"):
        await s.merge(_upsert_user(data))
    elif kind == "user.deleted":
        await s.execute(delete(User).where(User.id == data.get("id")))
    elif kind in ("organization.created", "organization.updated"):
        await s.merge(Org(id=data["id"], name=data.get("name", ""), slug=data.get("slug", "")))
    elif kind == "organization.deleted":
        await s.execute(delete(Org).where(Org.id == data.get("id")))
    elif kind in ("organizationMembership.created", "organizationMembership.updated"):
        org = data.get("organization", {})
        pud = data.get("public_user_data", {})
        await s.merge(Org(id=org["id"], name=org.get("name", ""), slug=org.get("slug", "")))
        if not await s.get(User, pud["user_id"]):
            await s.merge(User(id=pud["user_id"], email=pud.get("identifier", ""), name=""))
        await s.merge(
            Membership(id=data["id"], user_id=pud["user_id"], org_id=org["id"], role=data.get("role", "org:member"))
        )
    elif kind == "organizationMembership.deleted":
        await s.execute(delete(Membership).where(Membership.id == data.get("id")))
    await s.commit()
    return {"ok": True}
