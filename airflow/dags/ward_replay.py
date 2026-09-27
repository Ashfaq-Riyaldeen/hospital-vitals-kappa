"""ward_replay: Orchestrated 30-day log replay and clinical scoring rule change.

Triggered manually when a new clinical scoring rule version (e.g. v2) is introduced.
Under Kappa architecture, Kafka retention IS the historical store: reprocessing
means running a second instance of the stream processor from offset 0, computing
a version difference, and pausing at a mandatory human governance gate.

Workflow:
    preflight_check          <- record the log's start and end offsets per partition
           |
    confirm_replay_stream    <- ward-stream-v2 (started by `make replay`) is answering
           |
    wait_for_replay          <- its per-partition offsets pass the recorded end offsets
           |
    compute_version_diff     <- compare every reading scored under both versions
           |
    check_expected_result    <- only COPD patients changed; up only on oxygen at 93 %+
           |
    await_human_approval     <- DELIBERATE STOPPING POINT: clinical sign-off
           |
    cutover_active_version   <- manual, through the API (`make cutover`)

The first version of this DAG printed "replay complete" and returned a hard-coded
diff of "3 COPD reclassifications" without reading anything.

Under Kappa architecture, Airflow only orchestrates. Replaying is done by the streaming
processor itself reading the retained log, never by re-computing in Airflow.
"""

from __future__ import annotations

import os
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "ward-governance",
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=30),
}


@dag(
    dag_id="ward_replay",
    description="Orchestrate 30-day Kappa log replay for clinical scoring rule updates",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ward", "replay", "kappa", "governance"],
    params={"target_version": "v2", "from_offset": "earliest"},
)
def ward_replay() -> None:
    @task
    def preflight_check(params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Verify Kafka cluster accessibility and record high-watermark offsets."""
        from confluent_kafka import Consumer, TopicPartition
        from ward import settings

        kafka_cfg = settings.kafka()
        target_version = (params or {}).get("target_version") or "v2"

        consumer = Consumer(
            {
                "bootstrap.servers": kafka_cfg.bootstrap,
                "group.id": "ward-replay-preflight",
                "auto.offset.reset": "latest",
            }
        )
        topic = "vitals.readings.v1"
        try:
            metadata = consumer.list_topics(topic, timeout=10)
            partitions = metadata.topics[topic].partitions
            starts: dict[str, int] = {}
            ends: dict[str, int] = {}
            for p_id in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p_id), timeout=10)
                starts[str(p_id)], ends[str(p_id)] = low, high
            consumer.close()
            return {
                "target_version": target_version,
                "topic": topic,
                "start_offsets": starts,
                "end_offsets": ends,
                "partition_count": len(partitions),
            }
        except Exception as exc:
            consumer.close()
            raise AirflowFailException(f"Preflight check failed to query Kafka: {exc}") from exc

    @task(retries=3, retry_delay=pendulum.duration(seconds=20))
    def confirm_replay_stream(preflight_meta: dict[str, Any]) -> str:
        """The replay is ward-stream-v2 - the same pipeline, from offset 0, as v2.

        It is started by `make replay` (docker compose, profile "replay"). Airflow
        orchestrates; it does not run the stream. This step fails, and says why,
        if the replay stream is not up.
        """
        import urllib.request

        url = "http://ward-stream-v2:8105/health"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                resp.read()
        except OSError as exc:
            raise AirflowFailException(
                f"ward-stream-v2 is not answering at {url} ({exc}). Run `make replay`."
            ) from exc
        return str(preflight_meta["target_version"])

    @task(execution_timeout=pendulum.duration(minutes=45))
    def wait_for_replay(preflight_meta: dict[str, Any]) -> dict[str, Any]:
        """Poll the v2 stream's per-partition offsets until it passes the end offsets
        recorded at preflight. Progress is pushed to Prometheus as it goes."""
        from datetime import UTC, datetime

        from ward.replay.replay_runner import ReplayStatus, wait_for_replay

        status = ReplayStatus(
            target_version=preflight_meta["target_version"],
            started_at=datetime.now(UTC),
            end_offsets={int(k): v for k, v in preflight_meta["end_offsets"].items()},
            start_offsets={int(k): v for k, v in preflight_meta["start_offsets"].items()},
        )
        wait_for_replay(status, "http://ward-stream-v2:8105/metrics", timeout_seconds=2400)
        return {
            "events": status.total_events,
            "wait_seconds": status.duration_seconds,
        }

    @task
    def compute_version_diff(target_version: str, replay: dict[str, Any]) -> dict[str, Any]:
        """Compare every reading scored under both versions, from Cassandra."""
        from ward.replay.compare_versions import extract_ward_differences, generate_diff_artifacts
        from ward.store.dao import WardStoreDAO
        from ward.store.session import create_cluster, get_session

        cluster = create_cluster()
        try:
            dao = WardStoreDAO(get_session("ward", cluster=cluster))
            report = extract_ward_differences(dao, "v1", target_version)
        finally:
            cluster.shutdown()
        path = generate_diff_artifacts(report, os.environ.get("REPORTS_DIR", "/reports"))
        summary = {
            "report_path": str(path),
            "evaluations": report.total_evaluations,
            "changed": report.total_changed,
            "copd_changed": report.copd_evaluations_changed,
            "control_changed": report.non_copd_evaluations_changed,
            "upward_changes": report.upward_changes,
            "unexpected_upward": report.unexpected_upward_changes,
            "replayed_events": replay["events"],
        }
        print(summary)
        return summary

    @task
    def check_expected_result(summary: dict[str, Any]) -> str:
        """The expected answer is known before the replay: only COPD patients change,
        downward for their usual low saturation and upward only when they are on
        oxygen at 93 % or more. Anything else stops the run before anyone is asked
        to approve a cutover."""
        if summary["evaluations"] == 0:
            raise AirflowFailException("no readings were scored under both versions")
        if summary["control_changed"] or summary["unexpected_upward"]:
            raise AirflowFailException(
                f"unexpected changes: {summary['control_changed']} in non-COPD patients, "
                f"{summary['unexpected_upward']} unexplained upward. "
                f"See {summary['report_path']}."
            )
        return str(summary["report_path"])

    @task
    def await_human_approval(report_path: str) -> None:
        """DELIBERATE STOPPING POINT. Halts execution awaiting clinician governance approval."""
        print(
            f"REPLAY COMPLETE. Clinical audit report ready at {report_path}.\n"
            "MANDATORY GOVERNANCE GATE: Airflow will NOT cut over automatically.\n"
            "Clinician review and sign-off are required prior to activating v2 in production."
        )

    cutover_gate = EmptyOperator(
        task_id="cutover_active_version",
        doc="Manual cutover action taken after clinician governance approval.",
    )

    preflight = preflight_check()
    target = confirm_replay_stream(preflight)
    replay = wait_for_replay(preflight)
    diff = compute_version_diff(target, replay)
    report = check_expected_result(diff)
    approval = await_human_approval(report)

    target >> replay
    approval >> cutover_gate


dag_instance = ward_replay()
