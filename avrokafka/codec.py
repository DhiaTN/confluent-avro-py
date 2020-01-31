import struct
from io import BytesIO

import avro.io as AvroIO
import avro.schema as AvroSchema

from avrokafka.schemaregistry import SchemaRegistry

MAGIC_BYTE = 0


class AvroSerde(object):
    def __init__(self, registry_client: SchemaRegistry, topic: str):
        self.sr = registry_client
        self.subject = topic
        self.data_offset = self.sr.schema_id_size + 1

    def deserialize(self, data):
        payload = BytesIO(data)
        magic, schema_id = struct.unpack(">bI", payload.read(self.data_offset))
        schema_str = self.sr.get_schema(schema_id)
        avro_schema = AvroSchema.Parse(schema_str)
        decoder = AvroIO.BinaryDecoder(payload)
        reader = AvroIO.DatumReader(avro_schema)
        return reader.read(decoder)

    def serialize(self, data, schema_str):
        schema_info = self.sr.get_schema_info(self.subject, schema_str)
        schema_id = schema_info.get("id")
        avro_schema = AvroSchema.Parse(schema_str)
        writer = AvroIO.DatumWriter(avro_schema)
        outf = BytesIO()
        outf.write(struct.pack("b", MAGIC_BYTE))
        outf.write(struct.pack(">I", schema_id))
        encoder = AvroIO.BinaryEncoder(outf)
        writer.write(data, encoder)
        return outf.getvalue()


class AvroKeySerde(AvroSerde):
    def __init__(self, registry_client: SchemaRegistry, topic: str):
        self.sr = registry_client
        self.subject = "-".join([topic, "key"])


class AvroValueSerde(AvroSerde):
    def __init__(self, registry_client: SchemaRegistry, topic: str):
        self.sr = registry_client
        self.subject = "-".join([topic, "value"])


class AvroKeyValueSerde(object):
    def __init__(self, registry_client: SchemaRegistry, topic: str):
        self.key = AvroKeySerde(registry_client, topic)
        self.value = AvroValueSerde(registry_client, topic)
