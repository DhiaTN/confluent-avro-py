from confluent_avro.schema_registry import auth, client_http

SchemaRegistry = client_http.SchemaRegistry
SchemaRegistryRetry = client_http.SchemaRegistryRetry
CompatibilityLevel = client_http.CompatibilityLevel
HTTPBasicAuth = auth.HTTPBasicAuth
HTTPDigestAuth = auth.RegistryHTTPDigestAuth

__all__ = [
    "SchemaRegistry",
    "SchemaRegistryRetry",
    "CompatibilityLevel",
    "HTTPBasicAuth",
    "HTTPDigestAuth",
]
