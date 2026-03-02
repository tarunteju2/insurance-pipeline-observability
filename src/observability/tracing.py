"""
OpenTelemetry distributed tracing setup for insurance claims pipeline.
"""

import functools
import time
from typing import Optional, Callable, Any

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import StatusCode, Status
from opentelemetry.trace.propagation import set_span_in_context

import structlog

from src.config import config

logger = structlog.get_logger(__name__)

_tracer_provider: Optional[TracerProvider] = None
_initialized = False


def init_tracing(service_name: str = None) -> TracerProvider:
    """Initialize OpenTelemetry tracing with Jaeger as the exporter."""
    global _tracer_provider, _initialized

    if _initialized and _tracer_provider:
        return _tracer_provider

    svc_name = service_name or config.jaeger.service_name

    # Create a resource describing this service
    resource = Resource.create({
        "service.name": svc_name,
        "service.version": "1.0.0",
        "deployment.environment": "development",
        "service.namespace": "insurance-pipeline",
    })

    _tracer_provider = TracerProvider(resource=resource)

    # Attempt to export spans to Jaeger via OTLP; fall back to console output if unavailable
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=config.jaeger.endpoint,
            insecure=True
        )
        _tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        logger.info("OTLP tracing exporter initialized", endpoint=config.jaeger.endpoint)
    except Exception as e:
        logger.warning("Failed to init OTLP exporter, using console", error=str(e))
        _tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(_tracer_provider)
    _initialized = True

    logger.info("Tracing initialized", service=svc_name)
    return _tracer_provider


def get_tracer(component_name: str = "insurance-pipeline") -> trace.Tracer:
    """Get a tracer instance for the given component."""
    if not _initialized:
        init_tracing()
    return trace.get_tracer(component_name, "1.0.0")


def trace_operation(
    operation_name: str,
    component: str = "pipeline",
    attributes: Optional[dict] = None
) -> Callable:
    """Decorator to trace a function/method with OpenTelemetry."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            tracer = get_tracer(component)
            span_attributes = {
                "pipeline.component": component,
                "pipeline.operation": operation_name,
            }
            if attributes:
                span_attributes.update(attributes)

            with tracer.start_as_current_span(
                operation_name,
                attributes=span_attributes
            ) as span:
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    latency = (time.time() - start_time) * 1000
                    span.set_attribute("pipeline.latency_ms", latency)
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)
                    raise
        return wrapper
    return decorator


class PipelineSpanContext:
    """Context manager for creating pipeline-specific spans with claim data."""

    def __init__(self, tracer: trace.Tracer, operation: str, claim_id: str = None,
                 claim_type: str = None, stage: str = None):
        self.tracer = tracer
        self.operation = operation
        self.claim_id = claim_id
        self.claim_type = claim_type
        self.stage = stage
        self.span = None
        self.start_time = None

    def __enter__(self):
        attributes = {"pipeline.stage": self.stage or self.operation}
        if self.claim_id:
            attributes["claim.id"] = self.claim_id
        if self.claim_type:
            attributes["claim.type"] = self.claim_type

        self.span = self.tracer.start_span(self.operation, attributes=attributes)
        self.start_time = time.time()
        context = set_span_in_context(self.span)
        return self.span, context

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span:
            latency = (time.time() - self.start_time) * 1000
            self.span.set_attribute("pipeline.latency_ms", latency)
            if exc_type:
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                self.span.record_exception(exc_val)
            else:
                self.span.set_status(Status(StatusCode.OK))
            self.span.end()
        return False