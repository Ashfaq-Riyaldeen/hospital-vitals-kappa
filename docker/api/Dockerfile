# The serving layer.
#
# Deliberately lightweight: exposes FastAPI endpoints over Cassandra query-first tables.
# Uses a single uvicorn worker to prevent connection pool exhaustion and metrics fragmentation.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      "fastapi>=0.111" \
      "uvicorn[standard]>=0.30" \
      "cassandra-driver>=3.29.0" \
      "pydantic>=2.7" \
      "pydantic-settings>=2.3" \
      "structlog>=24.1" \
      "prometheus-client>=0.20" \
      "prometheus-fastapi-instrumentator>=7.0"

COPY ward/ /app/ward/

EXPOSE 8100

CMD ["uvicorn", "ward.api.app:app", "--host", "0.0.0.0", "--port", "8100", "--workers", "1"]
