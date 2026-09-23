# Both simulated sources run from this image. They share the ward model and the
# physiology package, so building it twice would let the two drift apart.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir \
      "confluent-kafka[avro,schemaregistry]>=2.4" \
      "pydantic>=2.7" "pydantic-settings>=2.3" \
      "structlog>=24.1" "fastavro>=1.9" "prometheus-client>=0.20"

COPY ward/ /app/ward/

# curl for the compose healthcheck; the slim image has none.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

CMD ["python", "-m", "ward.producers.bedside_monitor"]
