from types import SimpleNamespace

from opentelemetry.sdk.trace.export import SpanExportResult

from clinical_common.telemetry import LANGFUSE_SCOPE, DropScopesExporter


def test_langfuse_content_is_excluded_from_generic_export():
    class Exporter:
        def export(self, spans):
            self.spans = spans
            return SpanExportResult.SUCCESS

    inner = Exporter()
    exporter = DropScopesExporter(inner, frozenset({LANGFUSE_SCOPE}))
    private = SimpleNamespace(instrumentation_scope=SimpleNamespace(name=LANGFUSE_SCOPE))
    public = SimpleNamespace(instrumentation_scope=SimpleNamespace(name="agent"))
    assert exporter.export([private, public]) == SpanExportResult.SUCCESS
    assert inner.spans == [public]
