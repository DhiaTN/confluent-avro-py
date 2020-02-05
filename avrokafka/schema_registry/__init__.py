from avrokafka.schema_registry import client_http, errors

SchemaRegistry = client_http.SchemaRegistry

__all__ = ["SchemaRegistry", "errors"]
