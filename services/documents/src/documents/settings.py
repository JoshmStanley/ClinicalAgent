from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "documents"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/documents"
    opensearch_url: str = "http://localhost:9200"
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "clinical-documents"
    max_upload_bytes: int = 50 * 1024 * 1024


@lru_cache
def get_settings() -> Settings:
    return Settings()
