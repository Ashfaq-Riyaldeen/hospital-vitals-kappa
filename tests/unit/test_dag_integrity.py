"""test_dag_integrity: Asserts that all DAGs parse without errors and conform to operational standards.

Validates that:
1. Every DAG in airflow/dags parses cleanly with zero import errors.
2. catchup is disabled across all DAGs (simulated time does not backfill real wall-clock days).
3. max_active_runs is 1 to prevent race conditions on Cassandra partitions.
4. Schedules match the simulated day mapping (*/5 * * * * = once per simulated day).
5. All critical tasks and dependencies exist as specified in the architecture plan.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip(
    "airflow.models", reason="apache-airflow is required to validate DAG structures"
)

DAG_DIR = Path(os.environ.get("WARD_DAG_DIR", Path(__file__).resolve().parents[2] / "airflow/dags"))


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    assert DAG_DIR.is_dir(), f"DAG folder not found: {DAG_DIR}"
    bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    return bag


def test_no_import_errors(dagbag):
    """An import error in Airflow prevents the DAG from loading without failing loudly."""
    assert not dagbag.import_errors, f"DAG import errors detected: {dagbag.import_errors}"


@pytest.mark.parametrize(
    ("dag_id", "expected_schedule"),
    [
        ("ward_lab_ingest", "*/5 * * * *"),
        ("ward_daily_report", "*/5 * * * *"),
        ("ward_replay", None),
        ("ward_healthcheck", "*/2 * * * *"),
        ("ward_retention", "0 * * * *"),
    ],
)
def test_dag_schedule_and_presence(dagbag, dag_id: str, expected_schedule: str | None):
    assert dag_id in dagbag.dags, f"DAG {dag_id} not loaded into DagBag"
    dag = dagbag.dags[dag_id]
    assert dag.schedule_interval == expected_schedule, (
        f"Schedule mismatch for {dag_id}: expected {expected_schedule}, got {dag.schedule_interval}"
    )


@pytest.mark.parametrize(
    "dag_id",
    [
        "ward_lab_ingest",
        "ward_daily_report",
        "ward_replay",
        "ward_healthcheck",
        "ward_retention",
    ],
)
def test_catchup_is_disabled_and_runs_serialized(dagbag, dag_id: str):
    dag = dagbag.dags[dag_id]
    assert dag.catchup is False, f"catchup must be False for {dag_id}"
    assert dag.max_active_runs == 1, f"max_active_runs must be 1 for {dag_id}"


def test_lab_ingest_has_required_tasks(dagbag):
    expected_tasks = {
        "resolve_sim_date",
        "verify_checksum",
        "validate_lab_file",
        "quarantine_file",
        "publish_to_kafka",
        "verify_published",
        "run_dq_checks",
        "notify_success",
        "notify_failure",
    }
    dag = dagbag.dags["ward_lab_ingest"]
    assert expected_tasks <= set(dag.task_ids)


def test_replay_dag_stops_at_approval_gate(dagbag):
    dag = dagbag.dags["ward_replay"]
    assert "await_human_approval" in dag.task_ids
    assert "cutover_active_version" in dag.task_ids
    approval_task = dag.get_task("await_human_approval")
    assert "cutover_active_version" in approval_task.downstream_task_ids
