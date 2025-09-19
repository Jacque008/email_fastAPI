FROM python:3.11-slim as builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        gcc \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .
RUN pip install --user -r requirements.txt

FROM python:3.11-slim as production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PATH=/home/appuser/.local/bin:$PATH

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq5 \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && \
    useradd -r -g appuser -d /home/appuser -s /bin/bash -c "App user" appuser && \
    mkdir -p /home/appuser/.local && \
    chown -R appuser:appuser /home/appuser

COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local

WORKDIR /app

COPY --chown=appuser:appuser ./app ./app
COPY --chown=appuser:appuser ./static ./static
COPY --chown=appuser:appuser ./templates ./templates
COPY --chown=appuser:appuser ./data ./data
COPY --chown=appuser:appuser .env* ./

USER appuser
EXPOSE 5000
CMD exec gunicorn app.main:app -w 1 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT:-5000} --timeout 120 --preload --max-requests 1000 --max-requests-jitter 100
