from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "identity"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/identity"
    clerk_webhook_secret: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
