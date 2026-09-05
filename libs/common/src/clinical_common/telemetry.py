"""OpenTelemetry setup shared by every service and worker.

Tracing is off unless ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set (Compose points it
at Jaeger). Without it no provider is registered, every span is a no-op, and
tests and local runs need no collector.

Once configured, these are traced automatically: inbound HTTP (FastAPI or
Starlette, ``/health`` excluded), outbound httpx calls (which also carry the
W3C ``traceparent`` header to the next service), SQLAlchemy and asyncpg.
Kafka propagation lives in `clinical_common.kafka`.

Langfuse (agent only) adds its own span processor to this same provider, so a
run has one trace id in both systems. Its spans carry prompts and document
text, so they are dropped from the OTLP export here and only reach Langfuse.
"""

from __future__ import annotations

from collections.abc import Sequence

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import DEPLOYMENT_ENVIRONMENT, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.applications import Starlette

from clinical_common.config import BaseServiceSettings

# Instrumentation scopes whose spans never leave for the OTLP collector.
LANGFUSE_SCOPE = "langfuse-sdk"
_provider: TracerProvider | None = None


class DropScopesExporter(SpanExporter):
    """Forwards to `inner` every span except those from the given instrumentation scopes."""

    def __init__(self, inner: SpanExporter, scopes: frozenset[str]) -> None:
        self._inner = inner
        self._scopes = scopes

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        kept = [s for s in spans if not (s.instrumentation_scope and s.instrumentation_scope.name in self._scopes)]
        return self._inner.export(kept) if kept else SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        self._inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._inner.force_flush(timeout_millis)


def configure_telemetry(service_name: str, settings: BaseServiceSettings) -> None:
    """Register the global tracer provider and library instrumentation. Idempotent."""
    global _provider
    if _provider is not None or not settings.otel_exporter_otlp_endpoint:
        return
    resource = Resource.create({SERVICE_NAME: service_name, DEPLOYMENT_ENVIRONMENT: settings.environment})
    _provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
    _provider.add_span_processor(BatchSpanProcessor(DropScopesExporter(exporter, frozenset({LANGFUSE_SCOPE}))))
    trace.set_tracer_provider(_provider)
    HTTPXClientInstrumentor().instrument()
    AsyncPGInstrumentor().instrument()


def shutdown_telemetry() -> None:
    """Flush pending spans; call when a service or worker stops."""
    if _provider is not None:
        _provider.shutdown()


def instrument_app(app: Starlette) -> None:
    """Trace inbound requests.

    Call right after constructing the app: Starlette builds its middleware
    stack before the lifespan runs, so this cannot go there. It binds to the
    global provider lazily and is a no-op until `configure_telemetry` runs.
    """
    if isinstance(app, FastAPI):
        FastAPIInstrumentor.instrument_app(app, excluded_urls="/health", exclude_spans=["receive", "send"])
    else:
        app.add_middleware(OpenTelemetryMiddleware, excluded_urls="/health", exclude_spans=["receive", "send"])


def instrument_engine(engine: AsyncEngine) -> None:
    if _provider is not None:
        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine, tracer_provider=_provider)
