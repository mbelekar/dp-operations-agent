"""Agent middleware for tool calls that fail because a live backend is down.

create_agent's built-in ToolNode only turns argument-validation errors into a
ToolMessage the model can see; any other exception aborts the whole
diagnosis, discarding every signal already collected. For a backend being
unreachable or erroring (JMX aggregator, Schema Registry, Flink REST,
Marquez, the Kafka brokers themselves) that's the wrong trade: one missing
signal shouldn't cost the entire session.

Only BACKEND_ERRORS are converted. Anything else (a KeyError, a pydantic
ValidationError) is a bug in our code and still propagates, so it's seen
rather than fed to the model. A failed call records no Signal, so the
grounding validator won't let the model cite it as evidence.
"""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
from confluent_kafka import KafkaException
from langchain.agents.middleware import (
    ToolCallRequest,
    ToolErrorMiddleware,
    ToolRetryMiddleware,
)

from dp_ops_agent.audit.models import AuditEvent
from dp_ops_agent.audit.sink import AuditSink

# TimeoutError: confluent-kafka AdminClient futures (describe_consumer_groups
# etc.) raise the builtin from .result(timeout=...) when a broker or group
# coordinator doesn't answer, not KafkaException.
BACKEND_ERRORS: tuple[type[Exception], ...] = (httpx.HTTPError, KafkaException, TimeoutError)

_MAX_CONTENT_CHARS = 300


def build_tool_error_middleware(audit: AuditSink, session_id: str) -> ToolErrorMiddleware:
    def on_error(exc: Exception, request: ToolCallRequest) -> str | None:
        if not isinstance(exc, BACKEND_ERRORS):
            return None
        tool_name = request.tool_call["name"]
        error_type = type(exc).__name__
        audit.append(
            AuditEvent(
                event_type="tool_error",
                timestamp=datetime.now(timezone.utc),
                session_id=session_id,
                actor="tool",
                payload={"tool": tool_name, "error_type": error_type, "message": str(exc)},
            )
        )
        content = f"{tool_name} failed: backend unavailable ({error_type}: {exc})"
        return content[:_MAX_CONTENT_CHARS]

    return ToolErrorMiddleware(on_error)


def build_tool_retry_middleware(initial_delay: float = 1.0) -> ToolRetryMiddleware:
    """One retry, transport errors only (connection refused/reset, timeouts,
    a dropped response): the kind of failure a second attempt can fix. HTTP
    4xx/5xx and Kafka errors aren't retried. on_failure="error" re-raises
    once retries are spent, so the exception reaches the error middleware,
    which must therefore wrap this one (sit earlier in create_agent's
    middleware list)."""
    return ToolRetryMiddleware(
        max_retries=1,
        retry_on=(httpx.TransportError,),
        on_failure="error",
        initial_delay=initial_delay,
    )
