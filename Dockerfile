# syntax=docker/dockerfile:1
# Multi-stage build. Default image excludes torch/transformers (~2.5GB);
# build with --build-arg EXTRAS=all to include FinBERT.

FROM python:3.12-slim AS builder

ARG EXTRAS=exchange,onchain,redis,speed

WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir ".[${EXTRAS}]"


FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="CADB" \
      org.opencontainers.image.description="Crypto Anomaly Detection Bot — market manipulation surveillance" \
      org.opencontainers.image.licenses="MIT"

# Run unprivileged.
RUN useradd --create-home --shell /bin/bash --uid 10001 cadb

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HF_HOME=/home/cadb/.cache/huggingface

WORKDIR /app
COPY --chown=cadb:cadb config.yaml ./
COPY --chown=cadb:cadb src/ ./src/

RUN mkdir -p /app/models /app/state && chown -R cadb:cadb /app
USER cadb

# `cadb validate` exercises config loading and the import graph.
HEALTHCHECK --interval=60s --timeout=15s --start-period=45s --retries=3 \
    CMD cadb validate -c config.yaml || exit 1

ENTRYPOINT ["cadb"]
CMD ["run", "-c", "config.yaml"]
