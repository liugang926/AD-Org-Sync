FROM python:3.12-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /build
COPY pyproject.toml README.md ./
COPY sync_app ./sync_app
RUN python -m pip install --prefix=/install .

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 AD_ORG_SYNC_DATA_DIR=/data
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=builder /install/ /usr/local/
RUN mkdir -p /data /app/logs && chown -R app:app /data /app/logs
USER app
EXPOSE 8010
VOLUME ["/data", "/app/logs"]
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8010/healthz', timeout=3).read()"]
CMD ["python", "-m", "sync_app.cli", "serve", "--host", "0.0.0.0"]
