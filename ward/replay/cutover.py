"""Dynamic cutover and rollback management for Kappa scorer versions.

Under pure Kappa architecture, both v1 and v2 co-exist simultaneously in Cassandra.
Cutover from v1 to v2 (or instant rollback to v1) is an atomic configuration
or state update that instructs the API and dashboards which partition key to serve.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from ward.obs.log import configure, get_logger
from ward.store.session import create_cluster, get_session

log = get_logger(component="cutover")

STATE_KEY = "ACTIVE_SCORER_VERSION"


class CutoverManager:
    """Manages active serving version state in Cassandra and orchestrates switch."""

    def __init__(self, session: Any | None = None) -> None:
        self.session = session

    def _ensure_session(self) -> Any:
        if self.session is None:
            cluster = create_cluster()
            self.session = get_session("ward", cluster=cluster)
        return self.session

    def query_active_version(self) -> str:
        """Fetch current active serving version from Cassandra sim_state."""
        session = self._ensure_session()
        stmt = session.prepare("SELECT value FROM ward.sim_state WHERE key = ?")
        row = session.execute(stmt.bind((STATE_KEY,))).one()
        if row and row.value:
            return row.value
        return "v1"

    def apply_active_version(self, target_version: str) -> str:
        """Set the active serving version in Cassandra sim_state atomically."""
        session = self._ensure_session()
        stmt = session.prepare("INSERT INTO ward.sim_state (key, value) VALUES (?, ?)")
        session.execute(stmt.bind((STATE_KEY, target_version)))
        log.info("cutover_applied", active_version=target_version)
        return target_version


def execute_cutover(target_version: str = "v2") -> str:
    """Execute cutover to a target version."""
    mgr = CutoverManager()
    current = mgr.query_active_version()
    if current == target_version:
        log.info("cutover_noop", version=target_version)
        return target_version
    mgr.apply_active_version(target_version)
    log.info("cutover_completed", previous=current, current=target_version)
    return target_version


def execute_rollback(target_version: str = "v1") -> str:
    """Instantly roll back to previous version."""
    return execute_cutover(target_version=target_version)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ward Kappa Version Cutover Manager")
    parser.add_argument("--version", default="v2", help="Target version to activate (default: v2)")
    parser.add_argument("--rollback", action="store_true", help="Roll back to v1")
    parser.add_argument("--status", action="store_true", help="Print active version")
    args = parser.parse_args()

    configure(service="ward-replay-cutover", stage="serve")
    mgr = CutoverManager()

    if args.status:
        v = mgr.query_active_version()
        print(f"Active Serving Scorer Version: {v}")
        return

    target = "v1" if args.rollback else args.version
    try:
        active = mgr.apply_active_version(target)
        print(f"Cutover complete. Active Serving Scorer Version is now: {active}")
    except Exception as exc:
        print(f"Cutover failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
