"""Ward reference data, published to a COMPACTED topic.

Runs once at start-up, publishes one record per bed, and exits. It does not loop,
because admissions change on the timescale of days and compaction means a republished
key simply replaces the old value.

WHY COMPACTED RATHER THAN RETAINED. The streaming job broadcasts this data to enrich
every reading, and it must be able to rebuild that broadcast in full at any time --
on restart, on rescale, and during a replay from offset 0. A delete-retained topic
cannot promise that: after the retention window the earliest admissions are gone and
the broadcast would be missing patients. Compaction guarantees the latest value for
every key survives indefinitely, which is exactly "the current state of the ward".

`copd_scale2` IS THE FIELD THAT MATTERS. It selects NEWS2 SpO2 Scale 2, so it changes
a clinical result rather than labelling one. A patient who should be on Scale 2 but is
not flagged here is persistently over-scored, and the replay in plan/06 exists to
correct precisely that population.

Run:
    python -m ward.producers.admissions
"""

from __future__ import annotations

import sys
from pathlib import Path

from ward import settings
from ward.obs import metrics
from ward.obs.kafka_client import build_producer
from ward.obs.log import configure, get_logger
from ward.producers.ward_model import build_ward
from ward.simclock import read_anchor


def main(argv: list[str] | None = None) -> int:
    obs = settings.observability()
    configure(service="admissions", stage="ingest", level=obs.log_level, json_output=obs.log_json)
    log = get_logger()

    clock = read_anchor(Path(settings.sim().state_path))
    cfg = settings.ward()
    kafka_cfg = settings.kafka()
    patients = build_ward(cfg.beds, clock.epoch_sim, cfg.random_seed)

    from ward.contracts.serialization import serialize_key, serialize_value

    producer = build_producer("admissions")
    topic = kafka_cfg.admissions_topic
    failures = 0

    def report(err: object, _msg: object) -> None:
        nonlocal failures
        if err is not None:
            failures += 1
            log.error("delivery_failed", error=str(err), stage="ingest")

    for patient in patients:
        payload = {
            "patient_id": patient.patient_id,
            "bed_id": patient.bed_id,
            "age": patient.age,
            "sex": patient.sex,
            "primary_condition": patient.condition,
            "copd_scale2": patient.copd_scale2,
            "admitted_at": patient.admitted_at,
            "discharged_at": None,
            "schema_version": 1,
        }
        producer.produce(
            topic=topic,
            key=serialize_key(patient.patient_id, topic),
            value=serialize_value(payload, topic, kafka_cfg.schema_registry_url, "admission.avsc"),
            on_delivery=report,
        )
        producer.poll(0)

    undelivered = producer.flush(timeout=30)
    metrics.admissions_refresh_total.inc()

    scale2 = sum(1 for p in patients if p.copd_scale2)
    log.info(
        "admissions_published",
        patients=len(patients),
        copd_scale2=scale2,
        undelivered=undelivered,
        failures=failures,
        stage="ingest",
    )

    if undelivered or failures:
        # An incomplete broadcast means some readings would be enriched with no
        # admission record, and a patient with no `copd_scale2` flag silently falls
        # back to Scale 1 - the exact over-scoring the replay exists to fix.
        log.error(
            "admissions_incomplete",
            detail="a missing admission silently downgrades that patient to SpO2 Scale 1",
            stage="ingest",
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
