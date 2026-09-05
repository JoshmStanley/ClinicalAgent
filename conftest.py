"""Installs an in-memory OpenTelemetry provider once per test session so tests can assert on spans."""

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    _exporter.clear()
    return _exporter


@pytest.fixture
def spans(span_exporter):
    return span_exporter
