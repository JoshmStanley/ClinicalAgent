"""Base settings shared by every service.

Each service subclasses `BaseServiceSettings` and adds its own fields.
Values come from environment variables (or a `.env` file in local dev).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "service"
    environment: str = "local"  # local | dev | prod
    log_level: str = "INFO"

    # --- auth -------------------------------------------------------------
    # "clerk": verify Clerk-issued JWTs (production).
    # "dev":   trust X-Dev-* headers (local development only).
    auth_mode: str = "dev"
    clerk_jwks_url: str = ""
    clerk_issuer: str = ""
    # Shared secret the gateway uses to forward an already-verified principal
    # to internal services. Every service must have the same value.
    internal_token: str = "dev-internal-token"

    # --- infra ------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:19092"

    # --- service discovery ------------------------------------------------
    identity_url: str = "http://localhost:8001"
    conversations_url: str = "http://localhost:8002"
    agent_url: str = "http://localhost:8003"
    financials_url: str = "http://localhost:8004"
    documents_url: str = "http://localhost:8005"
    clinicaltrials_mcp_url: str = "http://localhost:8010/mcp"

    # --- embeddings (shared by ingestion and search so vectors match) -----
    # "voyage" needs VOYAGE_API_KEY. "fake" is a deterministic local embedder
    # for pipeline testing without any API key.
    embedding_provider: str = "fake"
    voyage_api_key: str = ""
    voyage_model: str = "voyage-3"
