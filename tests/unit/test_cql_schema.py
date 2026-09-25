"""Static and architectural tests for the Cassandra CQL schema."""

from __future__ import annotations

import re
from pathlib import Path

SCHEMA_FILE = Path(__file__).parents[2] / "ward" / "store" / "schema.cql"
DAO_FILE = Path(__file__).parents[2] / "ward" / "store" / "dao.py"


def test_schema_file_exists() -> None:
    assert SCHEMA_FILE.is_file(), f"schema.cql not found at {SCHEMA_FILE}"


def test_no_allow_filtering_in_schema_or_dao() -> None:
    """★ Invariant: Zero ALLOW FILTERING across the codebase.

    Under Kappa, the query-first data model ensures all queries are single-partition
    reads. ALLOW FILTERING indicates a full-cluster scan and is an anti-pattern.
    """
    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    assert "ALLOW FILTERING" not in schema_text.upper()

    dao_text = DAO_FILE.read_text(encoding="utf-8")
    assert "ALLOW FILTERING" not in dao_text.upper()


def test_all_seven_query_tables_exist() -> None:
    expected_tables = {
        "vitals_by_patient",
        "risk_scores_by_patient",
        "ward_risk_snapshot",
        "alerts_by_ward",
        "labs_by_patient",
        "daily_patient_summary",
        "sim_state",
    }
    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    found_tables = set(
        re.findall(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?ward\.(\w+)", schema_text, re.IGNORECASE)
    )
    assert expected_tables == found_tables, (
        f"Missing or extra tables: {expected_tables ^ found_tables}"
    )


def test_snapshot_ttl_is_one_hundred_and_twenty_seconds() -> None:
    """★ Silence is not safety: snapshot table self-empties within 120 seconds if stream stops."""
    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    match = re.search(
        r"ward\.ward_risk_snapshot.*?default_time_to_live\s*=\s*(\d+)",
        schema_text,
        re.DOTALL | re.IGNORECASE,
    )
    assert match is not None, "default_time_to_live not found on ward_risk_snapshot"
    assert int(match.group(1)) == 120, "Snapshot TTL must be exactly 120 seconds"


def test_risk_score_in_clustering_key_for_free_worst_first_sort() -> None:
    """risk_score must be in the clustering key of ward_risk_snapshot so ordering is free."""
    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    snapshot_def = re.search(
        r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?ward\.ward_risk_snapshot\s*\((.*?)\)\s*WITH",
        schema_text,
        re.DOTALL | re.IGNORECASE,
    )
    assert snapshot_def is not None
    table_body = snapshot_def.group(1)
    pk_match = re.search(
        r"PRIMARY KEY\s*\(\s*\(\s*ward_id\s*,\s*scorer_version\s*\)\s*,\s*risk_score\s*,\s*patient_id\s*\)",
        table_body,
        re.IGNORECASE,
    )
    assert pk_match is not None, (
        "Primary key on ward_risk_snapshot must be ((ward_id, scorer_version), risk_score, patient_id)"
    )


def test_scorer_version_is_in_primary_keys_for_safe_replay() -> None:
    """scorer_version in primary keys ensures replay is non-destructive (v1 and v2 coexist)."""
    schema_text = SCHEMA_FILE.read_text(encoding="utf-8")
    assert "PRIMARY KEY ((patient_id), scorer_version, scored_at)" in schema_text
    assert "PRIMARY KEY ((ward_id, scorer_version), risk_score, patient_id)" in schema_text
    assert "PRIMARY KEY ((ward_id, sim_date), patient_id, scorer_version)" in schema_text
