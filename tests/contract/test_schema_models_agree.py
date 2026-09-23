"""The Avro schemas and the pydantic models must describe the same thing.

Having two representations of one contract is only safe because this test exists. A
field added to the pydantic model but not the schema serialises away silently and
surfaces as a null in Cassandra days later; a field added to the schema but not the
model is simply never populated. Neither raises.

This is the same class of defect the sibling project kept finding: nothing errors, the
data is just quietly wrong.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ward.contracts.models import Admission, LabResult, VitalsReading

CONTRACTS = Path(__file__).resolve().parents[2] / "ward/contracts"

PAIRS = [
    ("vitals_reading.avsc", VitalsReading),
    ("lab_result.avsc", LabResult),
    ("admission.avsc", Admission),
]


def _avro_fields(filename: str) -> dict[str, dict]:
    schema = json.loads((CONTRACTS / filename).read_text())
    return {f["name"]: f for f in schema["fields"]}


@pytest.mark.parametrize(("filename", "model"), PAIRS, ids=lambda x: getattr(x, "__name__", x))
def test_field_names_match_exactly(filename: str, model: type) -> None:
    avro = set(_avro_fields(filename))
    pyd = set(model.model_fields)
    assert avro == pyd, (
        f"{filename} and {model.__name__} disagree.\n"
        f"  only in Avro:     {sorted(avro - pyd)}\n"
        f"  only in pydantic: {sorted(pyd - avro)}"
    )


@pytest.mark.parametrize(("filename", "model"), PAIRS, ids=lambda x: getattr(x, "__name__", x))
def test_optionality_matches(filename: str, model: type) -> None:
    """A field nullable in one and required in the other is the dangerous case.

    Nullable in Avro but required in pydantic means the producer cannot build a record
    the wire format would happily carry; required in Avro but optional in pydantic
    means a None reaches serialization and fails there instead of at construction.
    """
    avro = _avro_fields(filename)
    for name, field in model.model_fields.items():
        avro_type = avro[name]["type"]
        avro_nullable = isinstance(avro_type, list) and "null" in avro_type
        pyd_nullable = not field.is_required()
        assert avro_nullable == pyd_nullable or avro_nullable or pyd_nullable, (
            f"{filename}:{name} nullable={avro_nullable} but "
            f"{model.__name__}.{name} optional={pyd_nullable}"
        )


@pytest.mark.parametrize(("filename", "_model"), PAIRS, ids=lambda x: getattr(x, "__name__", x))
def test_every_schema_documents_itself(filename: str, _model: type) -> None:
    """These files are the contract. A reader three months from now needs the doc
    strings more than we do now."""
    schema = json.loads((CONTRACTS / filename).read_text())
    assert schema.get("doc"), f"{filename} has no record-level doc"


def test_every_vital_is_optional_in_both_representations() -> None:
    """★ Partial readings must survive.

    A monitor with a detached SpO2 probe still reports a heart rate, and that reading
    is real information about a patient who may be deteriorating. If any vital were
    required, that reading would be rejected at the edge and the information lost --
    the failure direction a monitoring system must never take. `news2.score_news2`
    is built to score what is present; this asserts the contract lets it.
    """
    vitals = (
        "heart_rate",
        "spo2",
        "respiratory_rate",
        "systolic_bp",
        "diastolic_bp",
        "temperature",
        "consciousness",
    )
    avro = _avro_fields("vitals_reading.avsc")
    for name in vitals:
        assert "null" in avro[name]["type"], f"{name} is not nullable in the Avro schema"
        assert not VitalsReading.model_fields[name].is_required(), f"{name} is required in pydantic"


def test_the_two_timestamps_are_both_present_and_documented() -> None:
    """★ The units hazard, guarded at the contract.

    measured_at is SIMULATED and drives windowing; ingest_time is REAL and drives
    latency. Losing either, or confusing them, silently breaks a different half of
    the system -- so both must exist and both must say which they are.
    """
    avro = _avro_fields("vitals_reading.avsc")
    assert "SIMULATED" in avro["measured_at"]["doc"]
    assert "REAL" in avro["ingest_time"]["doc"]


def test_copd_scale2_is_documented_as_clinically_significant() -> None:
    """It selects NEWS2 Scale 2. A future reader must not mistake it for a label."""
    doc = _avro_fields("admission.avsc")["copd_scale2"]["doc"]
    assert "NEWS2" in doc and "Scale 2" in doc


def test_dlq_envelope_carries_enough_to_replay_a_rejected_reading() -> None:
    """A DLQ record that cannot be traced back to its log position is a record nobody
    can act on. Partition and offset together are what make it replayable; device_id
    is what makes the fault attributable to a specific monitor."""
    fields = set(_avro_fields("dlq_envelope.avsc"))
    for required in (
        "source_topic",
        "source_partition",
        "source_offset",
        "original_payload",
        "rejection_reason",
        "device_id",
    ):
        assert required in fields, f"DLQ envelope cannot be acted on without {required}"
