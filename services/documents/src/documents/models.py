import uuid

from sqlalchemy import String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from clinical_common.db import Base, TimestampMixin, new_id

STATUS_UPLOADED = "uploaded"
STATUS_CONVERTED = "converted"
STATUS_SECTIONED = "sectioned"
STATUS_CHUNKED = "chunked"
STATUS_INDEXED = "indexed"
STATUS_FAILED = "failed"


class Document(Base, TimestampMixin):
    __tablename__ = "documents"
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=new_id)
    org_id: Mapped[str] = mapped_column(String, index=True)
    uploaded_by: Mapped[str] = mapped_column(String)
    title: Mapped[str] = mapped_column(String)
    filename: Mapped[str] = mapped_column(String)
    content_type: Mapped[str] = mapped_column(String)
    storage_key: Mapped[str] = mapped_column(String)
    size_bytes: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String, default=STATUS_UPLOADED, index=True)
    error: Mapped[str] = mapped_column(Text, default="")
    chunk_count: Mapped[int] = mapped_column(default=0)
