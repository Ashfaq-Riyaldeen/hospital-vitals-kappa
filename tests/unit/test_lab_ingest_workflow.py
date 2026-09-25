"""test_lab_ingest_workflow: Unit tests for daily lab file validation, checksums, and quarantine branching.

Under Kappa architecture, pathology results arrive once daily in batch files.
Airflow validates the integrity and syntax of incoming files before publishing them
verbatim into the Kafka compacted log.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from airflow.exceptions import AirflowFailException


@pytest.fixture
def lab_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    inbox = tmp_path / "inbox"
    quarantine = tmp_path / "quarantine"
    inbox.mkdir()
    quarantine.mkdir()

    monkeypatch.setenv("LAB_INBOX", str(inbox))
    monkeypatch.setenv("LAB_QUARANTINE", str(quarantine))
    return inbox, quarantine


def _write_test_file(
    inbox: Path, sim_date: str, results: list[dict], corrupt_checksum: bool = False
) -> Path:
    payload = {
        "ward_id": "WARD-A",
        "sim_date": sim_date,
        "collected_at_sim": f"{sim_date}T06:00:00Z",
        "submitted_at_real": "2026-04-01T00:05:00Z",
        "lab_id": "PATH-LAB-1",
        "results": results,
    }
    body = json.dumps(payload, indent=2).encode()
    digest = hashlib.sha256(body).hexdigest()
    if corrupt_checksum:
        digest = "0000000000000000000000000000000000000000000000000000000000000000"

    lab_file = inbox / f"labs_{sim_date}.json"
    sidecar = inbox / f"labs_{sim_date}.json.sha256"
    lab_file.write_bytes(body)
    sidecar.write_text(f"{digest}  {lab_file.name}\n")
    return lab_file


def test_verify_checksum_valid_file(lab_env):
    inbox, _ = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    verify_task = dag.get_task("verify_checksum").python_callable

    sim_date = "2026-04-01"
    valid_results = [
        {
            "patient_id": "P001",
            "test_type": "lactate",
            "result_value": 1.2,
            "unit": "mmol/L",
            "reference_range": "0.5-2.2",
            "collected_at": "2026-04-01T05:40:00Z",
        }
    ]
    _write_test_file(inbox, sim_date, valid_results)

    result_path = verify_task(sim_date)
    assert Path(result_path).exists()


def test_verify_checksum_detects_tampering(lab_env):
    inbox, _ = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    verify_task = dag.get_task("verify_checksum").python_callable

    sim_date = "2026-04-01"
    valid_results = [
        {
            "patient_id": "P001",
            "test_type": "lactate",
            "result_value": 1.2,
            "unit": "mmol/L",
            "reference_range": "0.5-2.2",
            "collected_at": "2026-04-01T05:40:00Z",
        }
    ]
    _write_test_file(inbox, sim_date, valid_results, corrupt_checksum=True)

    with pytest.raises(AirflowFailException, match="Checksum mismatch"):
        verify_task(sim_date)


def test_validate_lab_file_routes_to_publish_when_valid(lab_env):
    inbox, _ = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    validate_task = dag.get_task("validate_lab_file").python_callable

    sim_date = "2026-04-01"
    valid_results = [
        {
            "patient_id": "P001",
            "test_type": "lactate",
            "result_value": 1.4,
            "unit": "mmol/L",
            "reference_range": "0.5-2.2",
            "collected_at": "2026-04-01T05:40:00Z",
        },
        {
            "patient_id": "P002",
            "test_type": "crp",
            "result_value": 3.2,
            "unit": "mg/L",
            "reference_range": "<5.0",
            "collected_at": "2026-04-01T05:40:00Z",
        },
    ]
    _write_test_file(inbox, sim_date, valid_results)

    destination = validate_task(sim_date)
    assert destination == "publish_to_kafka"


def test_validate_lab_file_routes_to_quarantine_on_corrupt_range(lab_env):
    inbox, _ = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    validate_task = dag.get_task("validate_lab_file").python_callable

    sim_date = "2026-04-04"
    # Malformed reference range simulated on day 4
    malformed_results = [
        {
            "patient_id": "P003",
            "test_type": "lactate",
            "result_value": 1.8,
            "unit": "mmol/L",
            "reference_range": "4.0--11.0",
            "collected_at": "2026-04-04T05:40:00Z",
        }
    ]
    _write_test_file(inbox, sim_date, malformed_results)

    destination = validate_task(sim_date)
    assert destination == "quarantine_file"


def test_validate_lab_file_routes_to_quarantine_on_duplicate_records(lab_env):
    inbox, _ = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    validate_task = dag.get_task("validate_lab_file").python_callable

    sim_date = "2026-04-01"
    # Duplicate patient and test_type in same batch
    duplicates = [
        {
            "patient_id": "P001",
            "test_type": "lactate",
            "result_value": 1.4,
            "unit": "mmol/L",
            "reference_range": "0.5-2.2",
            "collected_at": "2026-04-01T05:40:00Z",
        },
        {
            "patient_id": "P001",
            "test_type": "lactate",
            "result_value": 2.1,
            "unit": "mmol/L",
            "reference_range": "0.5-2.2",
            "collected_at": "2026-04-01T05:40:00Z",
        },
    ]
    _write_test_file(inbox, sim_date, duplicates)

    destination = validate_task(sim_date)
    assert destination == "quarantine_file"


def test_quarantine_file_moves_corrupt_payload(lab_env):
    inbox, quarantine = lab_env
    from airflow.dags.ward_lab_ingest import ward_lab_ingest

    dag = ward_lab_ingest()
    quarantine_task = dag.get_task("quarantine_file").python_callable

    sim_date = "2026-04-04"
    _write_test_file(inbox, sim_date, [])

    quarantine_task(sim_date)

    assert not (inbox / f"labs_{sim_date}.json").exists()
    assert (quarantine / f"labs_{sim_date}_invalid.json").exists()
    assert (quarantine / f"labs_{sim_date}_invalid.reason").exists()
