"""The Cassandra cluster object must be constructible without a mock.

Every other store test mocks the session, which is how `Cluster(...,
default_execution_profile=...)` - an argument the driver does not accept - shipped,
and no process in the stack could connect to Cassandra. Building the object touches
no network, so this runs without Docker.
"""

from __future__ import annotations

from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster
from ward.store.session import create_cluster


def test_create_cluster_builds_a_real_cluster_object() -> None:
    cluster = create_cluster(hosts="127.0.0.1", port=9042)  # resolved, never contacted
    assert isinstance(cluster, Cluster)
    profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
    assert profile.request_timeout == 15.0
