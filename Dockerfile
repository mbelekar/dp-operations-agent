# syntax=docker/dockerfile:1
FROM python:3.11-slim AS builder

WORKDIR /app
COPY pyproject.toml ./
COPY src/ ./src/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.11-slim AS runtime

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
