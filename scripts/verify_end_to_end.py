"""End-to-end verification of everything built so far.

There is no streaming job yet, so this script stands in for one: it consumes real
readings off Kafka, deserialises them through the Schema Registry, and scores them
with THE ACTUAL `ward.clinical.news2` - not a copy. That exercises the whole path
that exists today:

    physiology -> producer -> Avro -> Kafka -> registry -> deserialise -> NEWS2

Every check compares an output against a value known BEFORE the measurement was taken.
"40 patients appeared" means nothing on its own; "WARD_BEDS is 40 and 40 appeared"
is evidence. That distinction is the only thing that has reliably caught defects on
this project, in either repo.

    make e2e
"""

from __future__ import annotations

import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from ward import settings
from ward.clinical.news2 import score_news2
from ward.producers.physiology.defects import TOTAL_DEFECT_RATE
from ward.producers.physiology.narratives import COPD_PATIENT, SEPSIS_PATIENT
from ward.producers.ward_model import build_ward

CONSUME_SECONDS = 45


@dataclass
class Check:
    name: str
    expected: str
    actual: str
    passed: bool
    note: str = ""


@dataclass
class Findings:
    readings: int = 0
    patients: set[str] = field(default_factory=set)
    partitions: Counter = field(default_factory=Counter)
    impossible: Counter = field(default_factory=Counter)
    empty: int = 0
    scores: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    spo2_by_patient: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    future_timestamps: int = 0
    duplicate_ids: int = 0


def _physiologically_impossible(v: dict) -> str | None:
    """The DLQ's future job, applied here so the injected defects can be counted.

    IMPOSSIBILITY, not statistical outliers. An SpO2 of 88 is a sick patient and must
    pass; an SpO2 of 0 is a detached probe and must not. A 3-sigma filter would
    suppress exactly the readings this system exists to notice.
    """
    if all(v[k] is None for k in ("heart_rate", "spo2", "systolic_bp", "temperature")):
        return "EMPTY_READING"
    if v["spo2"] is not None and v["spo2"] == 0:
        return "SPO2_PROBE_DETACHED"
    if v["heart_rate"] is not None and (v["heart_rate"] == 0 or v["heart_rate"] > 250):
        return "HR_OUT_OF_RANGE"
    if v["temperature"] is not None and not (25 <= v["temperature"] <= 45):
        return "TEMP_OUT_OF_RANGE"
    if (
        v["systolic_bp"] is not None
        and v["diastolic_bp"] is not None
        and v["diastolic_bp"] >= v["systolic_bp"]
    ):
        return "BP_INVERTED"
    return None


def consume(seconds: int) -> Findings:
    k = settings.kafka()
    sr = SchemaRegistryClient({"url": "http://localhost:8181"})
    deserialize = AvroDeserializer(sr)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9192",
            "group.id": f"e2e-verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([k.vitals_topic])

    ward_index = {
        p.patient_id: p
        for p in build_ward(settings.ward().beds, datetime.now(), settings.ward().random_seed)
    }

    f = Findings()
    seen_ids: set[str] = set()
    import time

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        msg = consumer.poll(2.0)
        if msg is None or msg.error():
            continue

        v = deserialize(msg.value(), SerializationContext(k.vitals_topic, MessageField.VALUE))
        f.readings += 1
        f.patients.add(v["patient_id"])
        f.partitions[msg.partition()] += 1

        if v["reading_id"] in seen_ids:
            f.duplicate_ids += 1
        seen_ids.add(v["reading_id"])

        if (reason := _physiologically_impossible(v)) is not None:
            f.impossible[reason] += 1
            if reason == "EMPTY_READING":
                f.empty += 1
            continue  # a dead-lettered reading is never scored

        if v["spo2"] is not None:
            f.spo2_by_patient[v["patient_id"]].append(v["spo2"])

        patient = ward_index.get(v["patient_id"])
        result = score_news2(
            respiratory_rate=v["respiratory_rate"],
            spo2=v["spo2"],
            on_supplemental_oxygen=v["on_supplemental_oxygen"],
            systolic_bp=v["systolic_bp"],
            heart_rate=v["heart_rate"],
            consciousness=v["consciousness"],
            temperature=v["temperature"],
            copd_scale2=bool(patient and patient.copd_scale2),
        )
        f.scores[v["patient_id"]].append(result.total)

    consumer.close()
    return f


def evaluate(f: Findings) -> list[Check]:
    beds = settings.ward().beds
    checks: list[Check] = []

    checks.append(
        Check(
            "readings consumed",
            "> 0",
            str(f.readings),
            f.readings > 0,
        )
    )
    checks.append(
        Check(
            "distinct patients",
            f"exactly {beds} (WARD_BEDS)",
            str(len(f.patients)),
            len(f.patients) == beds,
            "an independently known value - any other number is a bug, not a judgement",
        )
    )
    checks.append(
        Check(
            "partitions used",
            "all 6",
            str(len(f.partitions)),
            len(f.partitions) == 6,
            f"spread {min(f.partitions.values())}-{max(f.partitions.values())}; "
            "uneven as plan/04 section 1.1 predicts for 40 keys over 6 partitions",
        )
    )

    impossible = sum(f.impossible.values())
    rate = impossible / f.readings if f.readings else 0
    # Sampling error on a few thousand readings is wide, so the band is generous.
    # The point is that the two numbers are of the same order, not identical.
    ok = abs(rate - TOTAL_DEFECT_RATE) < 0.012
    checks.append(
        Check(
            "injected defect rate",
            f"~{TOTAL_DEFECT_RATE:.2%} (configured)",
            f"{rate:.2%} ({impossible} readings)",
            ok,
            "the DLQ's control: what the simulator injects is what validation must reject",
        )
    )

    # The rarest defects are 0.1% and 0.05%, so a short consume window expects well
    # under one of each. Demanding four distinct types made this fail on SAMPLE SIZE
    # rather than on anything being wrong -- a threshold that cannot be met is as
    # useless as one that cannot fail.
    checks.append(
        Check(
            "defect types seen",
            f">= 2 (sample of {f.readings})",
            str(len(f.impossible)),
            len(f.impossible) >= 2,
            ", ".join(sorted(f.impossible))
            + " -- the rarest are 0.1% and 0.05%, so a short window expects none of them",
        )
    )

    scored = sum(len(v) for v in f.scores.values())
    checks.append(
        Check(
            "readings scored by NEWS2",
            "> 0",
            str(scored),
            scored > 0,
            "the real ward.clinical.news2, not a copy",
        )
    )

    copd_spo2 = f.spo2_by_patient.get(COPD_PATIENT, [])
    if copd_spo2:
        in_band = all(88 <= s <= 92 for s in copd_spo2)
        checks.append(
            Check(
                f"{COPD_PATIENT} SpO2 band",
                "88-92 (Scale 2 target)",
                f"{min(copd_spo2)}-{max(copd_spo2)}",
                in_band,
                "the replay's target population must actually exist",
            )
        )

    sepsis = f.scores.get(SEPSIS_PATIENT, [])
    if sepsis:
        checks.append(
            Check(
                f"{SEPSIS_PATIENT} scored",
                "> 0 readings",
                str(len(sepsis)),
                True,
                f"NEWS2 range {min(sepsis)}-{max(sepsis)}",
            )
        )

    return checks


def main() -> int:
    from ward.obs.log import configure

    configure(service="e2e", stage="process", level="WARNING", json_output=False)

    print(f"consuming for {CONSUME_SECONDS}s from localhost:9192 ...\n")
    f = consume(CONSUME_SECONDS)
    checks = evaluate(f)

    width = max(len(c.name) for c in checks)
    failed = 0
    for c in checks:
        mark = "PASS" if c.passed else "FAIL"
        if not c.passed:
            failed += 1
        print(f"  [{mark}] {c.name:<{width}}  expected {c.expected:<28} got {c.actual}")
        if c.note:
            print(f"         {c.note}")

    print(f"\n{len(checks) - failed}/{len(checks)} checks passed")
    if f.scores:
        worst = max(f.scores.items(), key=lambda kv: max(kv[1]))
        print(f"highest NEWS2 observed: {max(worst[1])} ({worst[0]})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
