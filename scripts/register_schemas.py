"""Register the Avro schemas under BACKWARD compatibility.

Registering up front, rather than letting the first producer do it implicitly, means
an incompatible schema fails HERE - at stack start-up, with a clear error - instead of
at 3am when a monitor with new firmware connects.

BACKWARD means a new consumer can read old data. That is the right direction for this
system: the streaming job is redeployed far more often than the bedside monitors are,
and it is what makes the plan/06 replay possible at all - a v2 scorer reads v1 records
straight off the retained log with no migration step.
"""

from __future__ import annotations

import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from ward import settings
from ward.obs.log import configure, get_logger

log = get_logger()

CONTRACTS = Path(__file__).resolve().parents[1] / "ward/contracts"

# subject -> schema file. The `-value` suffix is Confluent's TopicNameStrategy: the
# subject for a topic's value schema is `<topic>-value`. Getting this wrong means the
# schema registers fine and the consumer never finds it.
SUBJECTS: dict[str, str] = {
    "vitals.readings.v1-value": "vitals_reading.avsc",
    "labs.results.v1-value": "lab_result.avsc",
    "ward.admissions.v1-value": "admission.avsc",
    "vitals.readings.dlq-value": "dlq_envelope.avsc",
}


def main() -> int:
    obs = settings.observability()
    configure(service="init", stage="ingest", level=obs.log_level, json_output=obs.log_json)

    client = SchemaRegistryClient({"url": settings.kafka().schema_registry_url})

    for subject, filename in SUBJECTS.items():
        body = (CONTRACTS / filename).read_text()
        schema_id = client.register_schema(subject, Schema(body, schema_type="AVRO"))
        log.info("schema_registered", subject=subject, schema_id=schema_id, stage="ingest")

        # Set compatibility AFTER registering: setting it on a subject that does not
        # exist yet is accepted and then silently lost when the subject is created.
        client.set_compatibility(subject_name=subject, level="BACKWARD")

    log.info("schemas_ready", count=len(SUBJECTS), stage="ingest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
