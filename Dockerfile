# syntax=docker/dockerfile:1
FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ ./src/

# Runtime dependencies exactly as pinned in uv.lock, installed (not
# editable) into /opt/venv so the runtime stage can copy it on its own.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim AS runtime

RUN groupadd --gid 1000 dpops \
    && useradd --uid 1000 --gid dpops --create-home --shell /bin/bash dpops

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/src /app/src

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
USER dpops

ENTRYPOINT ["dp-ops-agent"]
CMD ["--help"]
