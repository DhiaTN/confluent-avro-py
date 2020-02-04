import struct
from io import BytesIO

from avrokafka import avrolib2 as avrolib
from avrokafka.exceptions import SerializerError
from avrokafka.schemaregistry import SchemaRegistry

MAGIC_BYTE = 0


class AvroSerde(object):
    """
    A class that can perform avro serlialization and deserialization 
    using the confluent schema registry.

    :param SchemaRegistry: registry_client
    :param str: topic
    :param func: naming_strategy
    """

    def __init__(
        self, registry_client, topic, naming_strategy=None,
    ):

        self.sr = registry_client
        self.subject = topic if not naming_strategy else naming_strategy(topic)
        self.data_offset = self.sr.schema_id_size + 1

    def deserialize(self, data):
        """
        Decode a message from kafka that has been avro-encoded
        and follows the schema registry wire format.
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format

        :param str|bytes or None message: message key or value to be decoded
        :returns: Decoded message contents.
        :rtype dict:
        """
        if data is None:
            return SerializerError("message can't be None Type")

        input_stream = BytesIO(data)
        magic, schema_id = struct.unpack(">bI", input_stream.read(self.data_offset))

        if magic != MAGIC_BYTE:
            raise SerializerError("message does not start with magic byte")

        schema_str = self.sr.get_schema(schema_id)
        decoder = avrolib.Decoder(schema_str)
        return decoder.decode(input_stream)

    def serialize(self, data: dict, avro_schema: str):
        """
        Encode `data` with the given avro schema `avro_schema`.
        If the avro_schema is rejected by the schema registry, 
        the serialization fails. The data encoding follows the schema registry wire format.
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
        
        :param dict data: An object to serialize
        :param str avro_schema: An avro parsed schema

        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """
        schema_info = self.sr.get_schema_info(self.subject, avro_schema)
        schema_id = schema_info.get("id")

        out_stream = BytesIO()
        out_stream.write(struct.pack("b", MAGIC_BYTE))
        out_stream.write(struct.pack(">I", schema_id))

        encoder = avrolib.Encoder(avro_schema)
        return encoder.encode(data, out_stream)


class AvroKeySerde(AvroSerde):
    """The schema is registered with the subject of 'topic-key'
    """

    def __init__(self, registry_client: SchemaRegistry, topic: str):
        super(AvroKeySerde, self).__init__(
            registry_client, topic, lambda x: "-".join([x, "key"])
        )


class AvroValueSerde(AvroSerde):
    """The schema is registered with the subject of 'topic-value'
    """

    def __init__(self, registry_client: SchemaRegistry, topic: str):
        super(AvroValueSerde, self).__init__(
            registry_client, topic, lambda x: "-".join([x, "value"])
        )


class AvroKeyValueSerde(object):
    def __init__(self, registry_client: SchemaRegistry, topic: str):
        self.key = AvroKeySerde(registry_client, topic)
        self.value = AvroValueSerde(registry_client, topic)
