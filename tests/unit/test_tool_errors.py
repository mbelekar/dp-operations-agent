import httpx
import pytest
from confluent_kafka import KafkaError, KafkaException
from langchain.agents.middleware import ToolCallRequest
from langchain_core.messages import ToolMessage

from dp_ops_agent.audit.jsonl_sink import JsonlAuditSink
from dp_ops_agent.orchestrator.tool_errors import (
    build_tool_error_middleware,
    build_tool_retry_middleware,
)

_REQUEST = httpx.Request("GET", "http://metrics-aggregator:5559/metrics")


def _request(tool_name: str = "hot_partition_skew") -> ToolCallRequest:
    return ToolCallRequest(
        tool_call={"name": tool_name, "args": {}, "id": "call-1", "type": "tool_call"},
        tool=None,
        state={},
        runtime=None,
    )


async def _run_raising(middleware, exc: Exception):
    async def handler(_request):
        raise exc

    return await middleware.awrap_tool_call(_request(), handler)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        httpx.RemoteProtocolError("Server disconnected without sending a response.", request=_REQUEST),
        httpx.HTTPStatusError(
            "502 Bad Gateway", request=_REQUEST, response=httpx.Response(502, request=_REQUEST)
        ),
        KafkaException(KafkaError(KafkaError._TRANSPORT)),
        # AdminClient futures' .result(timeout=...) raise the builtin, not KafkaException.
        TimeoutError(),
    ],
    ids=["transport", "http-status", "kafka", "admin-future-timeout"],
)
async def test_backend_error_becomes_error_tool_message_and_is_audited(tmp_path, exc):
    audit = JsonlAuditSink(tmp_path, "s1")
    middleware = build_tool_error_middleware(audit, "s1")

    result = await _run_raising(middleware, exc)

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.content.startswith("hot_partition_skew failed: backend unavailable")
    assert type(exc).__name__ in result.content
    [event] = audit.query(event_type="tool_error")
    assert event.actor == "tool"
    assert event.payload["tool"] == "hot_partition_skew"
    assert event.payload["error_type"] == type(exc).__name__


@pytest.mark.asyncio
async def test_error_content_is_truncated(tmp_path):
    middleware = build_tool_error_middleware(JsonlAuditSink(tmp_path, "s1"), "s1")

    result = await _run_raising(middleware, httpx.ConnectError("x" * 5000, request=_REQUEST))

    assert len(result.content) <= 300


@pytest.mark.asyncio
async def test_non_backend_error_propagates_and_is_not_audited(tmp_path):
    audit = JsonlAuditSink(tmp_path, "s1")
    middleware = build_tool_error_middleware(audit, "s1")

    with pytest.raises(ValueError, match="real bug"):
        await _run_raising(middleware, ValueError("real bug"))

    assert audit.query(event_type="tool_error") == []


def _flaky_handler(*failures: Exception):
    """Raises each of `failures` in turn, then succeeds."""
    calls = {"n": 0}
    remaining = list(failures)

    async def handler(request):
        calls["n"] += 1
        if remaining:
            raise remaining.pop(0)
        return ToolMessage(content="ok", tool_call_id=request.tool_call["id"])

    return handler, calls


@pytest.mark.asyncio
async def test_retry_recovers_from_one_transport_error():
    middleware = build_tool_retry_middleware(initial_delay=0)
    handler, calls = _flaky_handler(httpx.ConnectError("refused", request=_REQUEST))

    result = await middleware.awrap_tool_call(_request(), handler)

    assert result.content == "ok"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retry_gives_up_after_one_retry_and_reraises():
    middleware = build_tool_retry_middleware(initial_delay=0)
    handler, calls = _flaky_handler(
        httpx.ReadTimeout("t1", request=_REQUEST), httpx.ReadTimeout("t2", request=_REQUEST)
    )

    with pytest.raises(httpx.ReadTimeout):
        await middleware.awrap_tool_call(_request(), handler)
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retry_does_not_retry_http_status_errors():
    middleware = build_tool_retry_middleware(initial_delay=0)
    handler, calls = _flaky_handler(
        httpx.HTTPStatusError(
            "404", request=_REQUEST, response=httpx.Response(404, request=_REQUEST)
        )
    )

    with pytest.raises(httpx.HTTPStatusError):
        await middleware.awrap_tool_call(_request(), handler)
    assert calls["n"] == 1
