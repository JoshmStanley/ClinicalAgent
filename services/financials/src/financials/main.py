"""Financials: budgets per org / role / user and the usage ledger.

Enforcement model (v1): the gateway calls `/check` before queueing a run;
the agent posts actual usage after the run. Concurrent runs can overshoot a
cap by roughly one run's cost. Move to a reservation model if hard caps
become a requirement.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from secrets import compare_digest

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from clinical_common.auth import Principal, principal_dependency, require_admin
from clinical_common.db import Base, Database
from clinical_common.events import Usage
from clinical_common.logging import configure_logging
from clinical_common.telemetry import configure_telemetry, instrument_app, shutdown_telemetry
from financials.models import SCOPE_ORG, SCOPE_ROLE, SCOPE_USER, Budget, UsageRecord
from financials.pricing import cost_usd
from financials.settings import Settings, get_settings

db: Database | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    settings = get_settings()
    configure_logging(settings.log_level, settings.service_name)
    configure_telemetry(settings.service_name, settings)
    db = Database(settings.database_url)
    await db.create_all(Base)
    yield
    shutdown_telemetry()
    await db.dispose()


app = FastAPI(title="financials", lifespan=lifespan)
instrument_app(app)
get_principal = principal_dependency(get_settings)


async def get_session():
    async for s in db.session():
        yield s


def month_start() -> datetime:
    now = datetime.now(UTC)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


class BudgetIn(BaseModel):
    scope: str = Field(pattern="^(org|role|user)$")
    scope_key: str = ""
    monthly_limit_usd: float = Field(gt=0)


class BudgetOut(BudgetIn):
    id: str
    org_id: str


class BudgetStatus(BaseModel):
    scope: str
    scope_key: str
    monthly_limit_usd: float
    spent_usd: float
    remaining_usd: float


class CheckOut(BaseModel):
    allowed: bool
    reason: str = ""
    budgets: list[BudgetStatus]


class UsageIn(Usage):
    run_id: str


class SummaryOut(BaseModel):
    org_id: str
    month_start: datetime
    spent_usd: float
    by_user: dict[str, float]
    by_role: dict[str, float]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/budgets", response_model=list[BudgetOut])
async def list_budgets(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    rows = (await s.execute(select(Budget).where(Budget.org_id == p.org_id))).scalars()
    return [
        BudgetOut(
            id=str(b.id),
            org_id=b.org_id,
            scope=b.scope,
            scope_key=b.scope_key,
            monthly_limit_usd=float(b.monthly_limit_usd),
        )
        for b in rows
    ]


@app.put("/budgets", response_model=BudgetOut)
async def upsert_budget(body: BudgetIn, p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    require_admin(p)
    if body.scope == SCOPE_ORG:
        body.scope_key = ""
    if body.scope in (SCOPE_ROLE, SCOPE_USER) and not body.scope_key:
        raise HTTPException(400, "scope_key required for role/user budgets")
    existing = await s.scalar(
        select(Budget).where(Budget.org_id == p.org_id, Budget.scope == body.scope, Budget.scope_key == body.scope_key)
    )
    if existing:
        existing.monthly_limit_usd = body.monthly_limit_usd
        b = existing
    else:
        b = Budget(
            org_id=p.org_id, scope=body.scope, scope_key=body.scope_key, monthly_limit_usd=body.monthly_limit_usd
        )
        s.add(b)
    await s.commit()
    return BudgetOut(
        id=str(b.id),
        org_id=b.org_id,
        scope=b.scope,
        scope_key=b.scope_key,
        monthly_limit_usd=float(b.monthly_limit_usd),
    )


async def _spent(s: AsyncSession, org_id: str, *, role: str | None = None, user_id: str | None = None) -> float:
    q = select(func.coalesce(func.sum(UsageRecord.cost_usd), 0)).where(
        UsageRecord.org_id == org_id, UsageRecord.created_at >= month_start()
    )
    if role is not None:
        q = q.where(UsageRecord.role == role)
    if user_id is not None:
        q = q.where(UsageRecord.user_id == user_id)
    return float(await s.scalar(q) or 0)


@app.post("/check", response_model=CheckOut)
async def check(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    """Can this principal start another run this month?"""
    budgets = (await s.execute(select(Budget).where(Budget.org_id == p.org_id))).scalars().all()
    statuses: list[BudgetStatus] = []
    blocked: list[str] = []
    for b in budgets:
        if b.scope == SCOPE_ORG:
            spent = await _spent(s, p.org_id)
        elif b.scope == SCOPE_ROLE and b.scope_key == p.role:
            spent = await _spent(s, p.org_id, role=p.role)
        elif b.scope == SCOPE_USER and b.scope_key == p.user_id:
            spent = await _spent(s, p.org_id, user_id=p.user_id)
        else:
            continue
        limit = float(b.monthly_limit_usd)
        statuses.append(
            BudgetStatus(
                scope=b.scope,
                scope_key=b.scope_key,
                monthly_limit_usd=limit,
                spent_usd=spent,
                remaining_usd=max(limit - spent, 0.0),
            )
        )
        if spent >= limit:
            blocked.append(f"{b.scope} budget exhausted (${spent:.2f} of ${limit:.2f})")
    return CheckOut(allowed=not blocked, reason="; ".join(blocked), budgets=statuses)


def usage_writer(
    request: Request, settings: Settings = Depends(get_settings), p: Principal = Depends(get_principal)
) -> Principal:
    # This credential is provisioned only to the agent and financials services.
    # A forwarded user principal identifies who incurred the charge; it does
    # not grant permission to write the ledger.
    token = request.headers.get("X-Usage-Writer-Token", "")
    if not settings.usage_writer_token:
        raise HTTPException(503, "Usage writer credential not configured")
    if not compare_digest(token.encode(), settings.usage_writer_token.encode()):
        raise HTTPException(403, "Usage writer credential required")
    return p


@app.post("/internal/usage", status_code=201)
async def record_usage(body: UsageIn, p: Principal = Depends(usage_writer), s: AsyncSession = Depends(get_session)):
    cost = cost_usd(
        body.model,
        body.input_tokens,
        body.output_tokens,
        body.cache_read_input_tokens,
        body.cache_creation_input_tokens,
    )
    s.add(
        UsageRecord(
            org_id=p.org_id,
            user_id=p.user_id,
            role=p.role,
            run_id=body.run_id,
            model=body.model,
            input_tokens=body.input_tokens,
            output_tokens=body.output_tokens,
            cache_read_input_tokens=body.cache_read_input_tokens,
            cache_creation_input_tokens=body.cache_creation_input_tokens,
            cost_usd=cost,
        )
    )
    await s.commit()
    return {"cost_usd": cost}


@app.get("/usage/summary", response_model=SummaryOut)
async def usage_summary(p: Principal = Depends(get_principal), s: AsyncSession = Depends(get_session)):
    since = month_start()
    base = select(UsageRecord).where(UsageRecord.org_id == p.org_id, UsageRecord.created_at >= since)
    if not p.is_admin:
        base = base.where(UsageRecord.user_id == p.user_id)
    rows = (await s.execute(base)).scalars().all()
    by_user: dict[str, float] = {}
    by_role: dict[str, float] = {}
    for r in rows:
        by_user[r.user_id] = by_user.get(r.user_id, 0.0) + float(r.cost_usd)
        by_role[r.role] = by_role.get(r.role, 0.0) + float(r.cost_usd)
    return SummaryOut(
        org_id=p.org_id, month_start=since, spent_usd=sum(by_user.values()), by_user=by_user, by_role=by_role
    )
