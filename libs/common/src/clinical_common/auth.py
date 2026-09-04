"""Authentication and multi-tenant principal resolution.

Three ways a request can carry identity:

1. Internal (service-to-service): the gateway verified the caller and forwards
   `X-Internal-Token` plus `X-Principal-*` headers. Services trust these only
   when the token matches their configured `internal_token`.
2. Clerk JWT (`Authorization: Bearer <token>`) - production mode at the gateway.
3. Dev headers (`X-Dev-User-Id`, `X-Dev-Org-Id`, `X-Dev-Role`) - local only.

Every request resolves to a `Principal` that carries the org id. All data
access must be scoped by `principal.org_id`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import jwt
from fastapi import Depends, HTTPException, Request, status
from jwt import PyJWKClient

from clinical_common.config import BaseServiceSettings

HEADER_INTERNAL_TOKEN = "X-Internal-Token"
HEADER_PRINCIPAL_USER = "X-Principal-User"
HEADER_PRINCIPAL_ORG = "X-Principal-Org"
HEADER_PRINCIPAL_ROLE = "X-Principal-Role"

HEADER_DEV_USER = "X-Dev-User-Id"
HEADER_DEV_ORG = "X-Dev-Org-Id"
HEADER_DEV_ROLE = "X-Dev-Role"

ROLE_ADMIN = "org:admin"
ROLE_MEMBER = "org:member"


@dataclass(frozen=True)
class Principal:
    user_id: str
    org_id: str
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == ROLE_ADMIN

    def internal_headers(self, internal_token: str) -> dict[str, str]:
        """Headers to forward this principal to another internal service."""
        return {
            HEADER_INTERNAL_TOKEN: internal_token,
            HEADER_PRINCIPAL_USER: self.user_id,
            HEADER_PRINCIPAL_ORG: self.org_id,
            HEADER_PRINCIPAL_ROLE: self.role,
        }


@lru_cache(maxsize=4)
def _jwks_client(url: str) -> PyJWKClient:
    return PyJWKClient(url, cache_keys=True)


def _principal_from_clerk_token(token: str, settings: BaseServiceSettings) -> Principal:
    if not settings.clerk_jwks_url:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "CLERK_JWKS_URL not set")
    try:
        signing_key = _jwks_client(settings.clerk_jwks_url).get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=settings.clerk_issuer or None,
            options={"verify_aud": False},
        )
    except jwt.PyJWTError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"Invalid token: {exc}") from exc

    user_id = claims.get("sub")
    # Clerk session token v1 uses org_id/org_role; v2 nests them under "o".
    org = claims.get("o") or {}
    org_id = claims.get("org_id") or org.get("id")
    role = claims.get("org_role") or org.get("rol")
    if role and not role.startswith("org:"):
        role = f"org:{role}"
    if not user_id or not org_id:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Token must carry an active organization (sub + org_id)",
        )
    return Principal(user_id=user_id, org_id=org_id, role=role or ROLE_MEMBER)


def resolve_principal(request: Request, settings: BaseServiceSettings) -> Principal:
    headers = request.headers

    # 1. Internal forwarded principal.
    internal = headers.get(HEADER_INTERNAL_TOKEN)
    if internal is not None:
        if internal != settings.internal_token:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bad internal token")
        user_id = headers.get(HEADER_PRINCIPAL_USER)
        org_id = headers.get(HEADER_PRINCIPAL_ORG)
        role = headers.get(HEADER_PRINCIPAL_ROLE, ROLE_MEMBER)
        if not user_id or not org_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Missing principal headers")
        return Principal(user_id=user_id, org_id=org_id, role=role)

    # 2. Clerk bearer token.
    authz = headers.get("Authorization", "")
    if authz.lower().startswith("bearer "):
        if settings.auth_mode != "clerk":
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bearer auth disabled in dev mode")
        return _principal_from_clerk_token(authz.split(" ", 1)[1].strip(), settings)

    # 3. Dev headers.
    if settings.auth_mode == "dev":
        user_id = headers.get(HEADER_DEV_USER)
        org_id = headers.get(HEADER_DEV_ORG)
        if user_id and org_id:
            return Principal(user_id=user_id, org_id=org_id, role=headers.get(HEADER_DEV_ROLE, ROLE_MEMBER))

    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")


def principal_dependency(get_settings):
    """Build a FastAPI dependency bound to a service's settings provider."""

    def _dep(request: Request, settings=Depends(get_settings)) -> Principal:
        return resolve_principal(request, settings)

    return _dep


def require_admin(principal: Principal) -> Principal:
    if not principal.is_admin:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin role required")
    return principal
