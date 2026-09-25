# Spark streaming application image for hospital-vitals-kappa.
#
# Built FROM python:3.11-slim-bookworm with OpenJDK 17.
# Runs PySpark Structured Streaming with master("local[4]") inside the container.
#
# Pinned to bookworm (Debian 12) for openjdk-17 compatibility with Spark 3.5.1.

FROM python:3.11-slim-bookworm

ENV PYSPARK_VERSION=3.5.1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

# Kafka source/sink and Avro codec for PySpark.
RUN SPARK_JARS="$(python -c 'import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / "jars")')" \
 && M2=https://repo1.maven.org/maven2 \
 && for artifact in \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
      "org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar" \
      "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
      "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do curl -fsSL -o "${SPARK_JARS}/$(basename $artifact)" "${M2}/${artifact}"; done \
 && ls -1 "${SPARK_JARS}" | grep -E 'kafka|avro|commons-pool'

RUN pip install --no-cache-dir \
      "pydantic>=2.7" "pydantic-settings>=2.3" "structlog>=24.1" \
      "prometheus-client>=0.20" "cassandra-driver>=3.29" \
      "confluent-kafka[avro,schemaregistry]>=2.4" "fastavro>=1.9"

WORKDIR /app
COPY ward/ /app/ward/
COPY scripts/ /app/scripts/

CMD ["python", "-m", "ward.stream.pipeline"]
