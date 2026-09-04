from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from clinical_common.db import Base, TimestampMixin


class Org(Base, TimestampMixin):
    __tablename__ = "orgs"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # Clerk org id
    name: Mapped[str] = mapped_column(String, default="")
    slug: Mapped[str] = mapped_column(String, default="")


class User(Base, TimestampMixin):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # Clerk user id
    email: Mapped[str] = mapped_column(String, default="")
    name: Mapped[str] = mapped_column(String, default="")


class Membership(Base, TimestampMixin):
    __tablename__ = "memberships"
    __table_args__ = (UniqueConstraint("user_id", "org_id"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)  # Clerk membership id
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id", ondelete="CASCADE"), index=True)
    role: Mapped[str] = mapped_column(String, default="org:member")
