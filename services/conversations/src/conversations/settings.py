from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "conversations"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/conversations"
    stream_poll_seconds: float = 0.25
    stream_keepalive_seconds: float = 15.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
