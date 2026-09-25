# One-shot initialisation: topics with their cleanup policies, schema registration,
# and the shared simulated-clock anchor. Runs to completion and exits; everything
# else depends on it via `service_completed_successfully`.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/scripts

WORKDIR /app

RUN pip install --no-cache-dir \
      "confluent-kafka[avro,schemaregistry]>=2.4" \
      "pydantic>=2.7" "pydantic-settings>=2.3" \
      "structlog>=24.1" "fastavro>=1.9" "prometheus-client>=0.20" \
      "cassandra-driver>=3.29"

COPY ward/ /app/ward/
COPY scripts/ /app/scripts/

CMD ["python", "/app/scripts/init_stack.py"]
