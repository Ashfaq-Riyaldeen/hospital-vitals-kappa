"""Avro serialization in the Confluent wire format.

Every message on every topic is Avro with a Schema Registry id, not JSON. Three
reasons, in the order they matter here:

* **The schema will change.** plan/06's replay scenario is a scorer version change,
  but a monitor firmware update adding a field is the ordinary case. A registry with
  BACKWARD compatibility makes that a checked operation instead of a hope.
* **Size.** At 40 beds this is irrelevant; the point is that the design does not stop
  working at ward-network scale, where 40-60% smaller payloads matter.
* **It is a contract.** A JSON payload is whatever the producer felt like sending. An
  Avro schema is something the consumer can refuse.

THE WIRE FORMAT: a magic byte 0x00, then a 4-byte big-endian schema id, then the Avro
body. Consumers that skip the 5-byte prefix and hand the rest to a plain Avro reader
get silent corruption rather than an error, which is why decoding goes through this
module rather than being open-coded at each call site.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, StringSerializer

# Schemas live beside this module rather than in a subdirectory: the contracts ARE
# the package, and a nested folder implies there is something else in here.
SCHEMA_DIR = Path(__file__).parent

_string_serializer = StringSerializer("utf_8")


@lru_cache(maxsize=8)
def load_schema(filename: str) -> str:
    """Read a schema file. Cached - these never change at runtime."""
    return json.dumps(json.loads((SCHEMA_DIR / filename).read_text()))


@lru_cache(maxsize=1)
def registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


@lru_cache(maxsize=8)
def avro_serializer(url: str, schema_file: str) -> AvroSerializer:
    return AvroSerializer(registry_client(url), load_schema(schema_file))


def serialize_key(key: str, topic: str) -> bytes:
    return _string_serializer(key, SerializationContext(topic, "key"))


def serialize_value(value: dict[str, Any], topic: str, url: str, schema_file: str) -> bytes:
    return avro_serializer(url, schema_file)(value, SerializationContext(topic, "value"))


@lru_cache(maxsize=8)
def avro_deserializer(url: str, schema_file: str | None = None) -> Any:
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    schema_str = load_schema(schema_file) if schema_file else None
    return AvroDeserializer(registry_client(url), schema_str)


def deserialize(
    value_bytes: bytes,
    topic: str,
    url: str | None = None,
    schema_file: str | None = None,
) -> dict[str, Any]:
    """Deserialize Confluent-framed Avro bytes into a dictionary.

    If url is given, uses confluent_kafka AvroDeserializer with schema registry.
    If schema_file is provided, strips the 5-byte Confluent header and decodes
    via fastavro locally without requiring network access to the registry.
    """
    if len(value_bytes) > 5 and value_bytes[0] == 0 and schema_file is not None:
        import io

        import fastavro

        payload = value_bytes[5:]
        reader_schema = json.loads(load_schema(schema_file))
        return dict(fastavro.schemaless_reader(io.BytesIO(payload), reader_schema))

    if url is not None:
        from confluent_kafka.serialization import MessageField, SerializationContext

        deser = avro_deserializer(url, schema_file)
        result = deser(value_bytes, SerializationContext(topic, MessageField.VALUE))
        return dict(result) if result is not None else {}

    raise ValueError("Either url or schema_file must be specified for deserialization")
