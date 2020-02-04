import json
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
def employee_melissa():
    employees = json.loads((FIXTURE_DIR / "employees.json").read_text())
    return employees[0]


@pytest.fixture(scope="session")
def employee_melissa_avro(employee_melissa, employee_parsed_schema):
    out_stream = BytesIO()
    fastavro.schemaless_writer(out_stream, employee_parsed_schema, employee_melissa)
    return out_stream.getvalue()
