"""ward_healthcheck: an independent watcher over the streaming system.

Runs every 2 real minutes. In patient monitoring a silent failure is the most
dangerous one: a stopped pipeline looks exactly like a calm ward. Each check here
FAILS the run when its condition does not hold, so a red square in the Airflow grid
means something. (The first version returned True from every check whatever it found,
and reported a hard-coded consumer lag of 0.)

Workflow:
    check_vitals_flowing    <- the vitals topic's end offsets grow over 15 seconds
           |
    check_ward_snapshot     <- the ward snapshot holds a fresh score for (nearly) every bed
           |
    check_stream_progress   <- the Spark query reported progress in the last minute
           |
    report_health           <- summary line in the task log

Under Kappa, Airflow only orchestrates. This DAG reads; it never transforms data or
computes clinical risk.
"""

from __future__ import annotations

import time
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "ward-sre",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=15),
}

MIN_BEDS_REPORTING = 38  # of 40; one or two beds can be between readings or rejected
STREAM_METRICS_URL = "http://ward-stream:8104/metrics"


@dag(
    dag_id="ward_healthcheck",
    description="Independent liveness probe: data flowing, ward screen fresh, query alive",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ward", "observability", "healthcheck"],
)
def ward_healthcheck() -> None:
    @task
    def check_vitals_flowing() -> int:
        """Fail unless new readings arrive on the vitals topic within 15 seconds."""
        from confluent_kafka import Consumer, TopicPartition
        from ward import settings

        k_cfg = settings.kafka()
        consumer = Consumer({"bootstrap.servers": k_cfg.bootstrap, "group.id": "ward-health"})
        try:
            parts = (
                consumer.list_topics(k_cfg.vitals_topic, timeout=10)
                .topics[k_cfg.vitals_topic]
                .partitions
            )

            def end_total() -> int:
                return sum(
                    consumer.get_watermark_offsets(
                        TopicPartition(k_cfg.vitals_topic, p), timeout=10
                    )[1]
                    for p in parts
                )

            before = end_total()
            time.sleep(15)
            grew = end_total() - before
        finally:
            consumer.close()
        if grew <= 0:
            raise AirflowFailException("no new readings on the vitals topic in 15 seconds")
        print(f"{grew} new readings in 15 s ({grew / 15:.1f}/s)")
        return grew

    @task
    def check_ward_snapshot() -> dict[str, Any]:
        """Fail unless the ward snapshot has a current score for nearly every bed.

        The snapshot rows expire after 120 real seconds (TTL), so if the stream stops
        the snapshot EMPTIES - this check turns that into a failed run.
        """
        from ward.store.dao import WardStoreDAO
        from ward.store.session import create_cluster, get_session

        cluster = create_cluster()
        try:
            dao = WardStoreDAO(get_session("ward", cluster=cluster))
            rows = dao.get_ward_risk_snapshot("WARD-A", "v1")
        finally:
            cluster.shutdown()
        if len(rows) < MIN_BEDS_REPORTING:
            raise AirflowFailException(
                f"only {len(rows)} of 40 beds have a current score in the ward snapshot"
            )
        worst = rows[0]
        print(f"{len(rows)} beds scored; worst is {worst.patient_id} at {worst.risk_score}")
        return {"beds": len(rows), "worst": worst.patient_id, "worst_score": worst.risk_score}

    @task
    def check_stream_progress() -> float:
        """Fail unless the Spark query reported progress in the last 60 seconds."""
        import urllib.request

        from prometheus_client.parser import text_string_to_metric_families

        with urllib.request.urlopen(STREAM_METRICS_URL, timeout=5) as resp:
            text = resp.read().decode()
        stamps = [
            s.value
            for fam in text_string_to_metric_families(text)
            if fam.name == "stream_last_progress_timestamp_seconds"
            for s in fam.samples
        ]
        if not stamps:
            raise AirflowFailException("the stream has not reported any query progress")
        age = time.time() - max(stamps)
        if age > 60:
            raise AirflowFailException(f"last query progress was {age:.0f} s ago")
        print(f"last micro-batch progress {age:.1f} s ago")
        return age

    @task
    def report_health(readings: int, snapshot: dict[str, Any], age: float) -> None:
        print(
            f"HEALTHY: {readings} readings in 15 s, {snapshot['beds']} beds on screen, "
            f"query progress {age:.1f} s ago"
        )

    readings = check_vitals_flowing()
    snapshot = check_ward_snapshot()
    age = check_stream_progress()
    readings >> snapshot >> age
    report_health(readings, snapshot, age)


dag_instance = ward_healthcheck()
