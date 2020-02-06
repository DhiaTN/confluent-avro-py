import json
import struct
from io import BytesIO
from pathlib import Path

import fastavro
import pytest

FIXTURE_DIR = Path(__file__).parent / "fixutres"


@pytest.fixture(scope="session")
def employee_schema():
    return (FIXTURE_DIR / "employee.avsc").read_text()


@pytest.fixture(scope="session")
def employee_parsed_schema(employee_schema):
    return fastavro.parse_schema(json.loads(employee_schema))


@pytest.fixture(scope="session")
def employee_json_data():
    employees = json.loads((FIXTURE_DIR / "employees.json").read_text())
    return employees[0]


@pytest.fixture(scope="session")
def employee_avro_data(employee_json_data, employee_parsed_schema):
    with BytesIO() as out_stream:
        fastavro.schemaless_writer(
            out_stream, employee_parsed_schema, employee_json_data
        )
        return out_stream.getvalue()


@pytest.fixture
def employee_avro_wire_format(employee_json_data, employee_parsed_schema):
    def encode(schema_id):
        with BytesIO() as out_stream:
            out_stream.write(struct.pack("b", 0))
            out_stream.write(struct.pack(">I", schema_id))
            fastavro.schemaless_writer(
                out_stream, employee_parsed_schema, employee_json_data
            )
            return out_stream.getvalue()

    return encode
