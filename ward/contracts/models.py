"""Pydantic models mirroring the Avro schemas.

Two representations of the same contract, kept in step by
`tests/contract/test_schema_models_agree.py`. That test is the reason having both is
safe rather than a duplication hazard.

Why both exist:

* **Avro** is the wire contract. It is what Schema Registry enforces, what Spark reads,
  and what makes a schema change a checked operation.
* **Pydantic** is the in-process contract. The producers construct readings in Python
  and want a type error at construction, not a serialization failure three frames
  later with a message about a union branch.

Neither is a substitute for the other, and the agreement test means a field added to
one but not the other fails the build instead of surfacing as a null in Cassandra.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class VitalsReading(BaseModel):
    """One observation from one bedside monitor.

    UNITS, because getting these wrong is the quiet hazard in this domain:
    heart_rate beats/min, spo2 percent, respiratory_rate breaths/min,
    systolic_bp and diastolic_bp mmHg, temperature degrees CELSIUS.

    Every vital is Optional. A monitor with a detached SpO2 probe still reports a
    heart rate, and that partial reading is real information about a patient who may
    be deteriorating. Requiring all fields would mean discarding it.
    """

    model_config = ConfigDict(frozen=True)

    reading_id: str
    patient_id: str
    bed_id: str
    device_id: str

    measured_at: datetime = Field(description="SIMULATED time — windowing, trends")
    ingest_time: datetime = Field(description="REAL time — latency, traces")

    heart_rate: int | None = None
    spo2: int | None = None
    respiratory_rate: int | None = None
    systolic_bp: int | None = None
    diastolic_bp: int | None = None
    temperature: float | None = None
    consciousness: str | None = None
    on_supplemental_oxygen: bool = False

    producer_id: str
    schema_version: int = 1


class LabResult(BaseModel):
    """One pathology result.

    `unit` is carried explicitly and is the second units hazard in this project: a
    lactate reported in mg/dL and read as mmol/L is wrong by roughly 9x, and the
    resulting risk tier would be confidently wrong rather than obviously wrong.
    """

    model_config = ConfigDict(frozen=True)

    patient_id: str
    test_type: str
    result_value: float
    unit: str
    reference_low: float | None = None
    reference_high: float | None = None
    collected_at: datetime
    reported_at: datetime
    schema_version: int = 1

    @property
    def is_abnormal(self) -> bool:
        """Outside the reference range. Returns False when no range is given rather
        than guessing — an unknown range is not a normal result, and the caller is
        told which it is by checking the range fields."""
        if self.reference_low is not None and self.result_value < self.reference_low:
            return True
        return self.reference_high is not None and self.result_value > self.reference_high


class Admission(BaseModel):
    """Ward reference data.

    `copd_scale2` changes a clinical result rather than decorating it: it selects
    NEWS2 SpO2 Scale 2, without which this patient is persistently over-scored.
    """

    model_config = ConfigDict(frozen=True)

    patient_id: str
    bed_id: str
    age: int
    sex: str
    primary_condition: str
    copd_scale2: bool = False
    admitted_at: datetime
    discharged_at: datetime | None = None
    schema_version: int = 1
