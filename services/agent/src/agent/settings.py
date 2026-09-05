from functools import lru_cache

from pydantic import Field

from clinical_common.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "agent"
    anthropic_api_key: str = ""
    agent_model: str = "claude-opus-5"
    agent_effort: str = "medium"  # low | medium | high | xhigh | max
    agent_max_tokens: int = 16000
    agent_max_iterations: int = 12
    # --- orchestration ---------------------------------------------------
    orchestrator_max_depth: int = Field(
        default=2, ge=1, le=4
    )  # orchestration levels: root (0) plus nested orchestrators up to depth-1
    orchestrator_max_subagents: int = Field(default=5, ge=1, le=20)  # tasks per plan; extra tasks are dropped
    subagent_effort: str = "low"  # default effort for subagents when the plan does not set one
    subagent_max_iterations: int = 8  # model calls per subagent tool loop
    simple_mode: bool = True  # let the orchestrator answer trivial single-step questions without planning
    event_flush_seconds: float = 0.15
    agent_max_parallel: int = Field(default=4, ge=1, le=32)
    agent_max_total_agents: int = Field(default=12, ge=1, le=100)
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"
    usage_writer_token: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
