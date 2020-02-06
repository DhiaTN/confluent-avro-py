import struct
from io import BytesIO
from typing import Callable

from avrokafka import avrolib
from avrokafka.exceptions import SerializerError
from avrokafka.schema_registry import SchemaRegistry

MAGIC_BYTE = 0


class AvroSerde(object):
    """
    A class that can perform avro serialization and deserialization
    using the confluent schema registry.

    :param SchemaRegistry: registry_client
    :param str: topic
    :param func: naming_strategy
    """

    def __init__(
        self,
        registry_client: SchemaRegistry,
        topic: str,
        naming_strategy: Callable = lambda x: x,
    ):

        self.sr = registry_client
        self.subject = naming_strategy(topic)
        self.data_offset = self.sr.schema_id_size + 1
        self._encoder_map = {}
        self._decoder_map = {}

    def deserialize(self, data: bytes) -> dict:
        """
        Decode a message from kafka that has been avro-encoded
        and follows the schema registry wire format.
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format

        :param bytes: data to be decoded
        :returns: Decoded message content
        :rtype dict:
        """

        if data is None:
            raise SerializerError("message can't be None Type")

        with BytesIO(data) as input_stream:
            magic, schema_id = struct.unpack(">bI", input_stream.read(self.data_offset))

            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            decoder = self._get_decoder(schema_id)
            return decoder.decode(input_stream)

    def serialize(self, data: dict, avro_schema: str) -> bytes:
        """
        Encode `data` with the given avro schema `avro_schema`.
        If the `avro_schema` is rejected by the schema registry, 
        the serialization fails. The data encoding follows the schema registry wire format.
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
        
        :param dict data: An object to serialize
        :param str avro_schema: An avro parsed schema

        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """

        schema_id = self.sr.register_schema(self.subject, avro_schema)
        with BytesIO() as out_stream:
            out_stream.write(struct.pack("b", MAGIC_BYTE))
            out_stream.write(struct.pack(">I", schema_id))
            encoder = self._get_encoder(schema_id, avro_schema)  # cache
            return encoder.encode(data, out_stream)

    def _get_decoder(self, schema_id: int) -> avrolib.Decoder:
        if schema_id in self._decoder_map:
            return self._decoder_map[schema_id]
        schema_str = self.sr.get_schema(schema_id)
        self._decoder_map[schema_id] = avrolib.Decoder(schema_str)
        return self._decoder_map[schema_id]

    def _get_encoder(self, schema_id, schema_str) -> avrolib.Encoder:
        if schema_id in self._encoder_map:
            return self._encoder_map[schema_id]
        self._encoder_map[schema_id] = avrolib.Encoder(schema_str)
        return self._encoder_map[schema_id]


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
