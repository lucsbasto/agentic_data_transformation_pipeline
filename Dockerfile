# Stage 1 — builder
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

# Stage 2 — runtime
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN groupadd --system --gid 10001 pipeline \
 && useradd --system --uid 10001 --gid pipeline --create-home pipeline

WORKDIR /app

COPY --from=builder --chown=pipeline:pipeline /app/.venv /app/.venv
COPY --chown=pipeline:pipeline src ./src

USER pipeline

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -m pipeline agent --help >/dev/null || exit 1

ENTRYPOINT ["python", "-m", "pipeline"]
CMD ["agent", "run-forever"]
