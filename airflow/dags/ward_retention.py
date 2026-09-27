"""ward_retention: check that data expires the way the design says it does.

Runs hourly in real time. Two tables rely on Cassandra TTL rather than DELETE, because
deletes write tombstones that every later read must skip:

- ward_risk_snapshot: TTL 120 s, so the ward screen empties if the stream stops.
- alerts_by_ward: TTL 3 hours.

Workflow:
    check_table_ttls        <- the TTLs in the live schema match the design
           |
    check_snapshot_bounded  <- the snapshot holds a bounded number of rows (TTL working)
           |
    report_retention        <- summary line in the task log

The first version audited seven tables, two of which do not exist, and reported every
one as HEALTHY without reading anything.

Under Kappa, Airflow only orchestrates; it never deletes data itself.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "ward-dba",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}

EXPECTED_TTL = {"ward_risk_snapshot": 120, "alerts_by_ward": 10800}
# 40 beds, one row per distinct score a patient had in the last 120 s: a few hundred
# at most. Thousands would mean the TTL is not expiring rows.
MAX_SNAPSHOT_ROWS = 2000


@dag(
    dag_id="ward_retention",
    description="Check Cassandra TTLs match the design and the snapshot stays bounded",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ward", "cassandra", "retention"],
)
def ward_retention() -> None:
    @task
    def check_table_ttls() -> dict[str, int]:
        from ward.store.session import create_cluster, get_session

        cluster = create_cluster()
        try:
            session = get_session("ward", cluster=cluster)
            rows = session.execute(
                "SELECT table_name, default_time_to_live FROM system_schema.tables "
                "WHERE keyspace_name = 'ward'"
            )
            ttls = {r.table_name: r.default_time_to_live for r in rows}
        finally:
            cluster.shutdown()
        wrong = {t: ttls.get(t) for t, want in EXPECTED_TTL.items() if ttls.get(t) != want}
        if wrong:
            raise AirflowFailException(f"TTL differs from the design: {wrong}")
        print(f"TTLs as designed: {EXPECTED_TTL}")
        return ttls

    @task
    def check_snapshot_bounded() -> int:
        from ward.store.session import create_cluster, get_session

        cluster = create_cluster()
        try:
            session = get_session("ward", cluster=cluster)
            count = session.execute(
                "SELECT count(*) FROM ward_risk_snapshot "
                "WHERE ward_id = 'WARD-A' AND scorer_version = 'v1'"
            ).one()[0]
        finally:
            cluster.shutdown()
        if count > MAX_SNAPSHOT_ROWS:
            raise AirflowFailException(f"{count} snapshot rows: the TTL is not expiring rows")
        print(f"{count} rows in the v1 ward snapshot partition")
        return int(count)

    @task
    def report_retention(ttls: dict[str, int], snapshot_rows: int) -> dict[str, Any]:
        report = {"ttls": ttls, "snapshot_rows": snapshot_rows}
        print(f"retention OK: {report}")
        return report

    ttls = check_table_ttls()
    rows = check_snapshot_bounded()
    ttls >> rows
    report_retention(ttls, rows)


dag_instance = ward_retention()
