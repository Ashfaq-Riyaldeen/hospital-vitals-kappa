"""Cassandra session and connection management.

Configures connection pooling, token-aware routing, execution profiles,
and schema initialization.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    Cluster,
    DCAwareRoundRobinPolicy,
    ExecutionProfile,
    Session,
    TokenAwarePolicy,
)
from cassandra.policies import ExponentialReconnectionPolicy, RetryPolicy

from ward import settings

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_DEFAULT_PROFILE = "ward_default"


def create_cluster(
    hosts: list[str] | str | None = None,
    port: int | None = None,
) -> Cluster:
    """Create a configured Cassandra Cluster instance."""
    store_cfg = settings.storage()
    if hosts is None:
        raw_hosts = store_cfg.cassandra_hosts
        contact_points = [h.strip() for h in raw_hosts.split(",") if h.strip()]
    elif isinstance(hosts, str):
        contact_points = [h.strip() for h in hosts.split(",") if h.strip()]
    else:
        contact_points = hosts

    c_port = port if port is not None else store_cfg.cassandra_port

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
        retry_policy=RetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_ONE,
        request_timeout=15.0,
    )

    cluster = Cluster(
        contact_points=contact_points,
        port=c_port,
        execution_profiles={_DEFAULT_PROFILE: profile},
        default_execution_profile=_DEFAULT_PROFILE,
        reconnection_policy=ExponentialReconnectionPolicy(1.0, 10.0),
        protocol_version=4,
    )
    return cluster


def get_session(
    keyspace: str | None = None,
    cluster: Cluster | None = None,
) -> Session:
    """Obtain a connected Cassandra Session, setting the keyspace if provided."""
    target_keyspace = keyspace or settings.storage().keyspace
    c = cluster or create_cluster()
    session = c.connect()
    if target_keyspace:
        session.set_keyspace(target_keyspace)
    return session


def execute_cql_file(session: Session, cql_path: str | Path) -> None:
    """Execute all statements in a CQL script file idempotently."""
    path = Path(cql_path)
    if not path.is_file():
        raise FileNotFoundError(f"CQL script not found: {path}")

    raw_text = path.read_text(encoding="utf-8")
    statements: list[str] = []
    current: list[str] = []

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(current).strip()
            # remove trailing semicolon for execution
            if statement.endswith(";"):
                statement = statement[:-1].strip()
            if statement:
                statements.append(statement)
            current = []

    for stmt in statements:
        logger.debug("Executing CQL statement: %s", stmt[:60])
        session.execute(stmt)
