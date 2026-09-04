import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, Numeric, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from clinical_common.db import Base, TimestampMixin, new_id, utcnow

SCOPE_ORG = "org"
SCOPE_ROLE = "role"
SCOPE_USER = "user"


class Budget(Base, TimestampMixin):
    """Monthly USD limit. scope_key is '' for org, the role name for role, the user id for user."""

    __tablename__ = "budgets"
    __table_args__ = (UniqueConstraint("org_id", "scope", "scope_key"),)
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    org_id: Mapped[str] = mapped_column(String, index=True)
    scope: Mapped[str] = mapped_column(String)
    scope_key: Mapped[str] = mapped_column(String, default="")
    monthly_limit_usd: Mapped[float] = mapped_column(Numeric(12, 4))


class UsageRecord(Base):
    __tablename__ = "usage_records"
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    org_id: Mapped[str] = mapped_column(String, index=True)
    user_id: Mapped[str] = mapped_column(String, index=True)
    role: Mapped[str] = mapped_column(String)
    run_id: Mapped[str] = mapped_column(String, index=True)
    model: Mapped[str] = mapped_column(String)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cache_read_input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cache_creation_input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[float] = mapped_column(Numeric(12, 6))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
