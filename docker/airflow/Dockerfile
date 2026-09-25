# Airflow, for orchestration only.
#
# Under the Kappa architecture, the streaming pipeline is the sole computation path.
# Airflow orchestrates episodic workflows around the stream (daily lab file ingestion,
# daily clinical PDF report generation, 30-day replay governance gates, and liveness probes)
# without ever computing clinical scores or duplicating stream processing.
FROM apache/airflow:2.10.5-python3.11

USER root

# Docker CLI to interact with the host socket, curl, and native libraries for WeasyPrint
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      docker.io \
      curl \
      libpango-1.0-0 \
      libharfbuzz0b \
      libpangoft2-1.0-0 \
      libcairo2 \
      libgdk-pixbuf-2.0-0 \
      fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Python dependencies for Airflow tasks:
# Cassandra driver, Confluent Kafka, Pydantic, Jinja2, WeasyPrint, Structlog, Prometheus client
RUN pip install --no-cache-dir \
      "psycopg2-binary>=2.9" \
      "cassandra-driver>=3.29.0" \
      "confluent-kafka[avro]>=2.4.0" \
      "fastavro>=1.9" \
      "pydantic>=2.7" \
      "pydantic-settings>=2.3" \
      "structlog>=24.1" \
      "prometheus-client>=0.20" \
      "jinja2>=3.1" \
      "weasyprint>=60.0"

ENV PYTHONPATH=/opt/airflow
