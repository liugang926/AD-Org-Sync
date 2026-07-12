FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN python -m pip install --upgrade pip

COPY pyproject.toml README.md ./
COPY sync_app ./sync_app

RUN python -m pip install --prefix=/install ".[web]"

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder /install/ /usr/local/
COPY --chown=app:app config.example.ini /app/config.example.ini

RUN mkdir -p /data /app/logs \
    && chown -R app:app /data /app/logs

USER app

EXPOSE 8010
VOLUME ["/data", "/app/logs"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8010/healthz', timeout=3).read()"]

CMD ["python", "-m", "sync_app.cli", "web", "--db-path", "/data/app.db", "--config", "/app/config.example.ini", "--host", "0.0.0.0", "--port", "8010", "--secure-cookies", "auto"]
