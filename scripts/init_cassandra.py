"""Initialize the Cassandra keyspace and query-first tables.

Executes ward/store/schema.cql with connection retry backoff.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from cassandra import DriverException
from cassandra.cluster import NoHostAvailable
from ward import settings
from ward.obs.log import get_logger
from ward.store.session import create_cluster, execute_cql_file

log = get_logger()

SCHEMA_PATH = Path(__file__).parents[1] / "ward" / "store" / "schema.cql"


def main(max_retries: int = 30, retry_delay: float = 3.0) -> int:
    store_cfg = settings.storage()
    log.info(
        "cassandra_init_starting",
        hosts=store_cfg.cassandra_hosts,
        port=store_cfg.cassandra_port,
        keyspace=store_cfg.keyspace,
        schema_path=str(SCHEMA_PATH),
    )

    cluster = None
    session = None
    for attempt in range(1, max_retries + 1):
        try:
            cluster = create_cluster()
            session = cluster.connect()
            log.info("cassandra_connected", attempt=attempt)
            break
        except (NoHostAvailable, DriverException, Exception) as exc:
            if attempt == max_retries:
                log.error("cassandra_connect_failed_terminal", attempt=attempt, error=str(exc))
                return 1
            log.warning(
                "cassandra_connect_retry",
                attempt=attempt,
                error=str(exc),
                retry_in=retry_delay,
            )
            time.sleep(retry_delay)

    try:
        assert session is not None
        execute_cql_file(session, SCHEMA_PATH)
        log.info("cassandra_schema_applied", schema=str(SCHEMA_PATH))

        # Verify tables in keyspace
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            (store_cfg.keyspace,),
        )
        tables = [r.table_name for r in rows]
        log.info("cassandra_tables_verified", keyspace=store_cfg.keyspace, tables=tables)

        expected = [
            "vitals_by_patient",
            "risk_scores_by_patient",
            "ward_risk_snapshot",
            "alerts_by_ward",
            "labs_by_patient",
            "daily_patient_summary",
            "sim_state",
        ]
        missing = set(expected) - set(tables)
        if missing:
            log.error("cassandra_missing_tables", missing=list(missing))
            return 1

        log.info("cassandra_init_complete", count=len(tables))
        return 0
    except Exception as exc:
        log.error("cassandra_schema_execution_failed", error=str(exc))
        return 1
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())
