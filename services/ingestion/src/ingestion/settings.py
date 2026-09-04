from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "ingestion"
    opensearch_url: str = "http://localhost:9200"
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "clinical-documents"
    chunk_target_words: int = 350
    chunk_overlap_words: int = 50
    embed_batch_size: int = 64


@lru_cache
def get_settings() -> Settings:
    return Settings()
