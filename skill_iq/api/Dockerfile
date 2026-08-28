# Build context is the REPO ROOT (not api/), so the semantic_model/ dir
# used by app/core/semantic_model.py can be copied in alongside the API:
#   docker build -f api/Dockerfile -t skill-iq-api:latest .

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY api/requirements.txt ./api/requirements.txt
RUN pip install --no-cache-dir -r ./api/requirements.txt

COPY api/ ./api/
COPY semantic_model/ ./semantic_model/

WORKDIR /srv/api

# unprivileged runtime user
RUN useradd --create-home appuser && chown -R appuser /srv
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
