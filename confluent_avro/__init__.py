"""
An Avro SerDe implementation that integrates with the confluent
schema registry and serializes and deserializes data according
to the defined confluent wire format
"""


from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.serde import AvroKeySerde, AvroKeyValueSerde, AvroValueSerde

__all__ = ["AvroKeySerde", "AvroValueSerde", "AvroKeyValueSerde", "SchemaRegistry"]

__version__ = "1.8.0"
