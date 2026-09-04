from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "financials"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/financials"


@lru_cache
def get_settings() -> Settings:
    return Settings()
