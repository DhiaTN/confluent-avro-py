from io import BytesIO, StringIO
from pathlib import Path

import pytest

from avrokafka import avrolib

SCHEMA_FILE = Path(__file__).parent / "fixutres/employee.avsc"


def test_loads_success(employee_schema):
    schema = avrolib.loads(employee_schema)
    assert type(schema) == dict
    assert schema["name"] == "avrokafka.tests.Employee"


def test_loads_type_error():
    with pytest.raises(avrolib.SchemaParsingError):
        avrolib.loads("invalid_schema")


def test_loads_invalid_schema():
    invalid_schema = '{"type": "record", "fileds": {"type": "boolean"}}'
    with pytest.raises(avrolib.SchemaParsingError):
        avrolib.loads(invalid_schema)


def test_load_success():
    schema = avrolib.load(SCHEMA_FILE)
    assert type(schema) == dict
    assert schema["name"] == "avrokafka.tests.Employee"


def test_load_invalid_file_path():
    with pytest.raises(avrolib.SchemaParsingError):
        avrolib.load("invalid_schema")


def test_encoder(employee_schema, employee_melissa_avro, employee_melissa):
    encoder = avrolib.Encoder(employee_schema)
    melissa_encoded = encoder.encode(employee_melissa, BytesIO())
    assert melissa_encoded == employee_melissa_avro


def test_encoder_invalid_writer(employee_schema, employee_melissa):
    encoder = avrolib.Encoder(employee_schema)
    with pytest.raises(avrolib.InvalidWriterStream):
        encoder.encode(employee_melissa, StringIO())


def test_encoder_invalid_data(employee_schema):
    encoder = avrolib.Encoder(employee_schema)
    with pytest.raises(avrolib.EncodingError):
        encoder.encode({"name": "Mario Bros"}, BytesIO())


def test_decoder_success(employee_schema, employee_melissa_avro, employee_melissa):
    decoder = avrolib.Decoder(employee_schema)
    melissa_decoded = decoder.decode(BytesIO(employee_melissa_avro))
    assert melissa_decoded == employee_melissa


def test_decoder_schema_not_compatible(employee_melissa_avro):
    employee_schema = (
        '{"name": "random","type": "record", "fileds": {"type": "boolean"}}'
    )
    decoder = avrolib.Decoder(employee_schema)
    melissa_decoded = decoder.decode(BytesIO(employee_melissa_avro))
    assert melissa_decoded == {}


def test_decoder_invalid_reader(employee_schema, employee_melissa_avro):
    decoder = avrolib.Decoder(employee_schema)
    with pytest.raises(avrolib.DecodingError):
        decoder.decode(employee_melissa_avro)