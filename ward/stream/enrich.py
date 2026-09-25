"""Enrichment stage: join vital readings with admissions reference data.

Brings in clinical context, specifically:
- copd_scale2: selects NEWS2 SpO2 Scale 2 for hypercapnic respiratory failure
- admitting_condition, age, bed_id, ward_id
"""

from __future__ import annotations

from dataclasses import dataclass

from ward.contracts.models import Admission, VitalsReading


@dataclass(frozen=True, slots=True)
class EnrichedReading:
    """A vital reading decorated with patient admission context."""

    reading: VitalsReading
    age: int
    admitting_condition: str
    copd_scale2: bool
    ward_id: str = "WARD-A"


def enrich_reading(
    reading: VitalsReading,
    admissions_map: dict[str, Admission],
    default_ward_id: str = "WARD-A",
) -> tuple[EnrichedReading, bool]:
    """Enrich a single vital reading against the admissions reference map.

    Returns:
        (EnrichedReading, admission_found)
    """
    adm = admissions_map.get(reading.patient_id)
    if adm is None:
        enriched = EnrichedReading(
            reading=reading,
            age=65,
            admitting_condition="Unknown",
            copd_scale2=False,
            ward_id=default_ward_id,
        )
        return enriched, False

    enriched = EnrichedReading(
        reading=reading,
        age=adm.age,
        admitting_condition=adm.primary_condition,
        copd_scale2=adm.copd_scale2,
        ward_id=default_ward_id,
    )
    return enriched, True
