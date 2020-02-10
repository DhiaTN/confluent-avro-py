from confluent_avro.schema_registry import client_http

SchemaRegistry = client_http.SchemaRegistry
SchemaRegistryRetry = client_http.SchemaRegistryRetry

__all__ = ["SchemaRegistry", "SchemaRegistryRetry"]
