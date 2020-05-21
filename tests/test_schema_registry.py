import pytest
import responses
import status

from confluent_avro.schema_registry import CompatibilityLevel, SchemaRegistry
from confluent_avro.schema_registry.errors import (
    DataProcessingError,
    IncompatibleSchemaVersion,
    NotFoundError,
    SchemaRegistryError,
    SchemaRegistryNetworkError,
    SchemaRegistryUnavailable,
    UnauthorizedAccess,
)

SCHEMA_REGISTRY_URL = "https://test.registry.io"
SUBJECT = "confluent_avro-test-employee-value"


@pytest.fixture
def registry_client():
    return SchemaRegistry(SCHEMA_REGISTRY_URL)


def test_registry_client_invalid_url():
    with pytest.raises(ValueError):
        SchemaRegistry("test.registry.io")


@responses.activate
def test_get_schema_unauthorized(registry_client):
    schema_id = 1
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}",
        body="401 Authorization Required",
        status=status.HTTP_401_UNAUTHORIZED,
    )

    with pytest.raises(UnauthorizedAccess) as e:
        registry_client.get_schema(schema_id)
    assert e.value.client_error.get("message") == "401 Authorization Required"
    assert e.value.status_code == status.HTTP_401_UNAUTHORIZED


@responses.activate
def test_get_schema_invalid_id(registry_client):
    invalid_schema_id = 345623
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{invalid_schema_id}",
        json={"message": "Invalid ID"},
        status=status.HTTP_404_NOT_FOUND,
    )

    with pytest.raises(NotFoundError) as e:
        registry_client.get_schema(invalid_schema_id)
    assert e.value.status_code == status.HTTP_404_NOT_FOUND
    assert e.value.client_error.get("message") == "Invalid ID"


@responses.activate
def test_get_schema_unknown_error(registry_client):
    schema_id = 1
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}",
        body="504 Gateway Timeout",
        status=status.HTTP_504_GATEWAY_TIMEOUT,
    )

    with pytest.raises(SchemaRegistryUnavailable) as e:
        registry_client.get_schema(schema_id)
    assert e.value.client_error.get("message") == "504 Gateway Timeout"
    assert e.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


@responses.activate
def test_get_schema_invalid_url(registry_client):
    schema_id = 1
    with pytest.raises(SchemaRegistryNetworkError) as e:
        registry_client.get_schema(schema_id)
    assert e.value.client_error.get("message") == "A Connection error occurred."
    assert e.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


@responses.activate
def test_get_schema_success(registry_client, employee_schema):
    schema_id = 13
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}",
        json={"schema": employee_schema},
        status=status.HTTP_200_OK,
    )
    schema = registry_client.get_schema(schema_id)
    assert hash(schema) == hash(employee_schema)


@responses.activate
def test_get_schema_id_invalid_subject(registry_client, employee_schema):
    invalid_subject = "invalid-subject"
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{invalid_subject}",
        json={"message": "Subject not found"},
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(NotFoundError) as e:
        registry_client.get_schema_id(invalid_subject, employee_schema)
    assert e.value.status_code == status.HTTP_404_NOT_FOUND
    assert e.value.client_error.get("message") == "Subject not found"


@responses.activate
def test_get_schema_id_schema_not_registred(registry_client, employee_schema):
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}",
        json={"message": "Schema not found"},
        status=status.HTTP_404_NOT_FOUND,
    )
    with pytest.raises(NotFoundError) as e:
        registry_client.get_schema_id(SUBJECT, employee_schema)
    assert e.value.status_code == status.HTTP_404_NOT_FOUND
    assert e.value.client_error.get("message") == "Schema not found"


@responses.activate
def test_get_schema_id_success(registry_client, employee_schema):
    response_mock = {
        "subject": SUBJECT,
        "version": 1,
        "id": 88,
        "schema": employee_schema,
    }
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}",
        json=response_mock,
        status=status.HTTP_200_OK,
    )
    schema_id = registry_client.get_schema_id(SUBJECT, employee_schema)
    assert schema_id == response_mock["id"]


@responses.activate
def test_register_schema_version_incompatible(registry_client, employee_schema):
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"message": "Schema version incompatible"},
        status=status.HTTP_409_CONFLICT,
    )

    with pytest.raises(IncompatibleSchemaVersion) as e:
        registry_client.register_schema(SUBJECT, employee_schema)

    assert e.value.status_code == status.HTTP_409_CONFLICT
    assert e.value.client_error.get("message") == "Schema version incompatible"


@responses.activate
def test_register_schema_schema_invalid(registry_client, employee_schema):
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"message": "schema invalid"},
        status=422,
    )

    with pytest.raises(DataProcessingError) as e:
        registry_client.register_schema(SUBJECT, "invalid_avro_schema")

    assert e.value.status_code == status.HTTP_400_BAD_REQUEST
    assert e.value.client_error.get("message") == "schema invalid"


@responses.activate
def test_register_schema_success(registry_client, employee_schema):
    new_schema_id = 156
    responses.add(
        responses.POST,
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"id": new_schema_id},
        status=status.HTTP_200_OK,
    )
    schema_id = registry_client.register_schema(SUBJECT, employee_schema)
    assert schema_id == new_schema_id


@responses.activate
def test_get_default_compatibility_success(registry_client):
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/config",
        json={"compatibilityLevel": "forward"},
        status=status.HTTP_200_OK,
    )
    compatibility = registry_client.get_default_compatibility()
    assert compatibility == CompatibilityLevel.FORWARD


@responses.activate
def test_get_subject_compatibility_success(registry_client):
    subject_name = "subject_x"
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/config/{subject_name}",
        json={"compatibilityLevel": "full"},
        status=status.HTTP_200_OK,
    )
    compatibility = registry_client.get_subject_compatibility(subject_name)
    assert compatibility == CompatibilityLevel.FULL


@responses.activate
def test_get_subject_compatibility_broken_api(registry_client):
    subject_name = "subject_x"
    responses.add(
        responses.GET,
        f"{SCHEMA_REGISTRY_URL}/config/{subject_name}",
        json={"compatibility": "full"},
        status=status.HTTP_200_OK,
    )
    with pytest.raises(SchemaRegistryError) as e:
        registry_client.get_subject_compatibility(subject_name)
        assert e.message == "Something went unexpectedly wrong"


@responses.activate
def test_set_default_compatibility_success(registry_client):
    responses.add(
        responses.PUT,
        f"{SCHEMA_REGISTRY_URL}/config",
        json={"compatibility": "forward_transitive"},
        status=status.HTTP_200_OK,
    )
    registry_client.set_default_compatibility(CompatibilityLevel.FORWARD_TRANSITIVE)


@responses.activate
def test_set_default_compatibility_invalid_level(registry_client):
    responses.add(
        responses.PUT,
        f"{SCHEMA_REGISTRY_URL}/config",
        json={"error_code": 42203},
        status=422,
    )
    with pytest.raises(DataProcessingError):
        registry_client.set_default_compatibility("random_value")


@responses.activate
def test_set_subject_compatibility_success(registry_client):
    subject_name = "subject_x"
    responses.add(
        responses.PUT,
        f"{SCHEMA_REGISTRY_URL}/config/{subject_name}",
        json={"compatibility": "full_transitive"},
        status=status.HTTP_200_OK,
    )
    registry_client.set_subject_compatibility(
        subject_name, CompatibilityLevel.FULL_TRANSITIVE
    )


@responses.activate
def test_set_subject_compatibility_invalid_level(registry_client):
    subject_name = "subject_x"
    responses.add(
        responses.PUT,
        f"{SCHEMA_REGISTRY_URL}/config/{subject_name}",
        json={"error_code": 42203},
        status=422,
    )
    with pytest.raises(DataProcessingError):
        registry_client.set_subject_compatibility(subject_name, "random_value")
