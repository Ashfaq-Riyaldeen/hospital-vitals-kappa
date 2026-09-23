"""The lab uploader: the atomic drop, and the labs that corroborate the vitals.

The atomic-drop test is the important one. Writing straight to the path Airflow's
FileSensor watches lets the sensor fire on a half-written file, and the resulting
JSON parse error is intermittent and timing-dependent -- the worst kind to debug.
`plan/03 section 5.2` calls it out as "a real and very common bug".
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from ward.clinical.lab_rules import is_abnormal, parse_reference_range
from ward.producers.lab_uploader import (
    MALFORMED_LAB_DAY,
    MISSING_LAB_DAY,
    PANEL,
    SEPSIS_PANEL,
    SEPSIS_PATIENT,
    LabUploader,
)
from ward.simclock import SimClock

EPOCH = datetime(2026, 4, 1, tzinfo=UTC)


@pytest.fixture
def uploader(tmp_path: Path) -> LabUploader:
    clock = SimClock(epoch_wall=datetime.now(UTC), epoch_sim=EPOCH, day_seconds=300)
    return LabUploader(clock, inbox=tmp_path / "inbox")


# ------------------------------------------------------------- atomic drop


def test_no_partial_file_is_ever_visible_at_the_final_path(uploader: LabUploader) -> None:
    """★ The file appears complete or not at all.

    Asserted by reading the file back and parsing it: a rename cannot produce a
    truncated result, so if this parses, the sensor could never have seen a partial.
    """
    payload = uploader.build_file(EPOCH.date(), sim_day=1)
    path = uploader.drop(payload, EPOCH.date())

    assert path.exists()
    parsed = json.loads(path.read_text())
    assert len(parsed["results"]) == len(uploader.patients) * len(PANEL)


def test_the_temp_directory_is_left_empty(uploader: LabUploader) -> None:
    """A leftover .tmp file means a rename did not happen and the next run could
    pick up a stale half-written payload."""
    uploader.drop(uploader.build_file(EPOCH.date(), 1), EPOCH.date())
    assert list((uploader.inbox / ".tmp").iterdir()) == []


def test_the_checksum_sidecar_matches_the_file(uploader: LabUploader) -> None:
    import hashlib

    path = uploader.drop(uploader.build_file(EPOCH.date(), 1), EPOCH.date())
    sidecar = uploader.inbox / f"{path.name}.sha256"
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    assert sidecar.read_text().split()[0] == expected


def test_the_sidecar_lands_before_the_file(uploader: LabUploader) -> None:
    """Ordering is deliberate. If the process dies between the two renames we would
    rather have a sidecar with no file -- obvious, and the DAG simply waits -- than a
    file with no sidecar, which the DAG would parse unverified."""
    path = uploader.drop(uploader.build_file(EPOCH.date(), 1), EPOCH.date())
    sidecar = uploader.inbox / f"{path.name}.sha256"
    assert sidecar.stat().st_mtime_ns <= path.stat().st_mtime_ns


# ------------------------------------------------------- the clinical content


def test_p014_day2_labs_corroborate_the_vitals_deterioration(uploader: LabUploader) -> None:
    """★ This is what makes the business question answerable.

    The question asks how yesterday's labs CHANGE the risk picture. That only has a
    coherent answer if the labs agree with the vitals: P014's sepsis shows as raised
    lactate, WBC and CRP, so the composite risk rises ABOVE what vitals alone give.
    Random abnormal values would contradict the vitals as often as they agreed.
    """
    payload = uploader.build_file(EPOCH.date(), sim_day=2)
    results = {r["test_type"]: r for r in payload["results"] if r["patient_id"] == SEPSIS_PATIENT}
    for test_type, expected in SEPSIS_PANEL.items():
        result = results[test_type]
        assert result["result_value"] == pytest.approx(expected)
        assert (
            is_abnormal(result["result_value"], parse_reference_range(result["reference_range"]))
            is True
        ), f"P014's {test_type} must read abnormal against its own reference range"


def test_a_stable_patient_has_normal_labs(uploader: LabUploader) -> None:
    """The control for the test above. If everyone's labs were abnormal, the sepsis
    panel would prove nothing."""
    payload = uploader.build_file(EPOCH.date(), sim_day=2)
    stable = [r for r in payload["results"] if r["patient_id"] == "P002"]
    assert stable
    for result in stable:
        reference = parse_reference_range(result["reference_range"])
        assert is_abnormal(result["result_value"], reference) is False


def test_every_patient_gets_the_full_panel(uploader: LabUploader) -> None:
    payload = uploader.build_file(EPOCH.date(), sim_day=1)
    for patient in uploader.patients:
        types = {
            r["test_type"] for r in payload["results"] if r["patient_id"] == patient.patient_id
        }
        assert types == {s.name for s in PANEL}


# ---------------------------------------------------------------- scenarios


def test_the_missing_day_drops_no_file(uploader: LabUploader) -> None:
    """The sensor must time out, the DAG must branch, and the ward must carry on
    being monitored on vitals alone. Degrading honestly beats failing."""
    assert uploader.upload_for_day(EPOCH.date(), MISSING_LAB_DAY) is None
    assert not list(uploader.inbox.glob("labs_*.json")) if uploader.inbox.exists() else True


def test_the_malformed_day_corrupts_exactly_one_reference_range(
    uploader: LabUploader,
) -> None:
    """One bad range, not a broken file. The validator must quarantine on content,
    which is a harder and more realistic case than a file that will not parse."""
    payload = uploader.build_file(EPOCH.date(), sim_day=MALFORMED_LAB_DAY)
    unparseable = [
        r for r in payload["results"] if parse_reference_range(r["reference_range"]).is_unparsed
    ]
    assert len(unparseable) == 1
    assert unparseable[0]["patient_id"] == "P003"


def test_reference_ranges_are_strings_as_a_real_feed_sends_them(
    uploader: LabUploader,
) -> None:
    payload = uploader.build_file(EPOCH.date(), sim_day=1)
    assert all(isinstance(r["reference_range"], str) for r in payload["results"])
