from functools import lru_cache

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "agent"
    anthropic_api_key: str = ""
    agent_model: str = "claude-opus-5"
    agent_effort: str = "medium"  # low | medium | high | xhigh | max
    agent_max_tokens: int = 16000
    agent_max_iterations: int = 12
    event_flush_seconds: float = 0.15


@lru_cache
def get_settings() -> Settings:
    return Settings()
