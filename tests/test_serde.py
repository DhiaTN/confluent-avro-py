from unittest.mock import patch

import pytest

from confluent_avro.exceptions import MessageParsingError
from confluent_avro.schema_registry.errors import IncompatibleSchemaVersion
from confluent_avro.serde import AvroKeyValueSerde

TOPIC = "confluent_avro-test-employee"
SCHEMA_REGISTRY_CLS = "confluent_avro.schema_registry.SchemaRegistry"


def _schema_registry_mock(cls_mock, schema_id=None, schema_str=None):
    sr = cls_mock.return_value
    sr.schema_id_size = 4
    sr.get_schema.return_value = schema_str
    sr.register_schema.return_value = schema_id
    return sr


def test_value_deserialize_missing_magicbyte(employee_schema, employee_avro_data):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        sr = _schema_registry_mock(mock)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        with pytest.raises(MessageParsingError):
            avro_serde.value.deserialize(employee_avro_data)


def test_value_deserialize_none(employee_schema, employee_avro_data):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        sr = _schema_registry_mock(mock)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        with pytest.raises(MessageParsingError):
            avro_serde.value.deserialize(None)


def test_value_deserialize_success(
    employee_schema, employee_avro_wire_format, employee_json_data
):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        schema_id = 23
        sr = _schema_registry_mock(mock, schema_id, employee_schema)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        employee_avro_data = employee_avro_wire_format(schema_id)
        decoded_data = avro_serde.value.deserialize(employee_avro_data)
        assert decoded_data == employee_json_data


def test_value_serialize_incompatible_change(
    employee_schema, employee_avro_wire_format, employee_json_data
):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        sr = _schema_registry_mock(mock)
        sr.register_schema.side_effect = IncompatibleSchemaVersion({})
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        with pytest.raises(IncompatibleSchemaVersion):
            avro_serde.value.serialize(employee_json_data, employee_schema)


def test_value_serialize_success(
    employee_schema, employee_avro_wire_format, employee_json_data
):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        schema_id = 45
        sr = _schema_registry_mock(mock, schema_id, employee_schema)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        employee_avro = employee_avro_wire_format(schema_id)
        encoded_data = avro_serde.value.serialize(employee_json_data, employee_schema)
        assert encoded_data == employee_avro
        decoded_data = avro_serde.value.deserialize(encoded_data)
        assert decoded_data == employee_json_data


@patch("confluent_avro.serde.avrolib.Decoder")
def test_cached_decoder(
    decoder_mock, employee_schema, employee_avro_wire_format, employee_json_data
):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        schema_id = 45
        sr = _schema_registry_mock(mock, schema_id, employee_schema)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        encoded_data = avro_serde.value.serialize(employee_json_data, employee_schema)
        avro_serde.value.deserialize(encoded_data)
        avro_serde.value.deserialize(encoded_data)
        decoder_mock.assert_called_once()


@patch("confluent_avro.serde.avrolib.Encoder")
def test_cached_encoder_success(
    encoder_mock, employee_schema, employee_avro_wire_format, employee_json_data
):
    with patch(SCHEMA_REGISTRY_CLS) as mock:
        schema_id = 45
        sr = _schema_registry_mock(mock, schema_id, employee_schema)
        avro_serde = AvroKeyValueSerde(sr, TOPIC)
        avro_serde.value.serialize(employee_json_data, employee_schema)
        avro_serde.value.serialize(employee_json_data, employee_schema)
        encoder_mock.assert_called_once()
