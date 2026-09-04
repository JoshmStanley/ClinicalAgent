# Single image recipe for every Python service; PACKAGE selects the workspace member.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock .python-version ./
COPY libs ./libs
COPY services ./services

ARG PACKAGE
RUN uv sync --frozen --no-dev --package "${PACKAGE}"

ENV PATH="/app/.venv/bin:${PATH}"
