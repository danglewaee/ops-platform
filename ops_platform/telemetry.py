from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from typing import Any

try:  # pragma: no cover - optional dependency import
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

    OTEL_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - optional dependency import
    trace = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    SpanExporter = object
    SpanExportResult = None
    OTEL_AVAILABLE = False


DEFAULT_SERVICE_NAME = "ops-decision-platform"
_TRACING_CONFIGURED = False


class _NullSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        return

    def record_exception(self, error: BaseException) -> None:
        return

    def set_status(self, status: Any) -> None:
        return


if OTEL_AVAILABLE:  # pragma: no branch - small optional definition
    class _DiscardSpanExporter(SpanExporter):
        def export(self, spans: Any) -> SpanExportResult:
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            return


def configure_tracing(
    *,
    service_name: str = DEFAULT_SERVICE_NAME,
    otlp_endpoint: str | None = None,
) -> bool:
    global _TRACING_CONFIGURED

    if not OTEL_AVAILABLE:
        return False

    if _TRACING_CONFIGURED:
        return True

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))

    if otlp_endpoint:
        try:  # pragma: no cover - optional dependency import
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        except ModuleNotFoundError:
            return False
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
    else:
        exporter = _DiscardSpanExporter()

    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACING_CONFIGURED = True
    return True


@contextmanager
def traced_span(name: str, attributes: dict[str, Any] | None = None):
    if not OTEL_AVAILABLE:
        yield _NullSpan()
        return

    tracer = trace.get_tracer(DEFAULT_SERVICE_NAME)
    with tracer.start_as_current_span(name) as span:
        for key, value in (attributes or {}).items():
            _set_span_attribute(span, key, value)
        yield span


def current_trace_id() -> str | None:
    if not OTEL_AVAILABLE:
        return None

    span = trace.get_current_span()
    context = span.get_span_context()
    if not getattr(context, "is_valid", False):
        return None
    return f"{context.trace_id:032x}"


def annotate_span(span: Any, **attributes: Any) -> None:
    for key, value in attributes.items():
        _set_span_attribute(span, key, value)


def _set_span_attribute(span: Any, key: str, value: Any) -> None:
    normalized = _normalize_attribute(value)
    if normalized is None:
        return
    span.set_attribute(key, normalized)


def _normalize_attribute(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    if is_dataclass(value):
        return str(asdict(value))
    if isinstance(value, dict):
        return str(value)
    return str(value)
