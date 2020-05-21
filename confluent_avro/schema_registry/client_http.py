import enum
import json
from functools import lru_cache
from urllib.parse import urljoin

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from confluent_avro.schema_registry.auth import RegistryAuthBase
from confluent_avro.schema_registry.errors import handle_client_error


class SchemaRegistryRetry(Retry):
    pass


class SchemaRegistrySession(Session):
    def __init__(self, base_url: str):
        self.base_url = base_url
        super(SchemaRegistrySession, self).__init__()

    def request(self, method, url, *args, **kwargs):
        url = urljoin(self.base_url, url)
        return super(SchemaRegistrySession, self).request(method, url, *args, **kwargs)


HEADERS = {"Content-Type": "application/json"}
RETRY_POLICY = SchemaRegistryRetry(
    total=5,
    backoff_factor=0.3,
    method_whitelist=False,
    raise_on_status=(500, 502, 503, 504),
)


class CompatibilityLevel(enum.Enum):
    """An enumeration of different compatibility
    levels supported by the schema registry
    """

    BACKWARD = "backward"
    BACKWARD_TRANSITIVE = "backward_transitive"
    FORWARD = "forward"
    FORWARD_TRANSITIVE = "forward_transitive"
    FULL = "full"
    FULL_TRANSITIVE = "full_transitive"
    NONE = "none"

    def __str__(self):
        return self.name


class SchemaRegistry(object):
    """HTTP client to connect to interact with schema registry"""

    def __init__(
        self,
        url: str,
        auth: RegistryAuthBase = None,
        schema_id_size: int = 4,
        retry_policy: SchemaRegistryRetry = RETRY_POLICY,
        headers: dict = HEADERS,
    ):
        if not url.startswith("http"):
            raise ValueError(
                f"Invalid URL '{url}': No schema supplied. "
                f"Perhaps you meant http(s)://{url}?"
            )

        self.url = url
        self.schema_id_size = schema_id_size
        self._session = self._build_session(auth, retry_policy, headers)

    def _build_session(
        self,
        auth: RegistryAuthBase = None,
        retry_policy: SchemaRegistryRetry = None,
        headers: dict = None,
    ) -> SchemaRegistrySession:
        """
        Provides persistent connection to the schema registry 
        to be reused for all client interaction. It supports multiple auth types
        and implements retry mechanism.

        :param RegistryAuthBase auth: class handling authorization with schema registry
        :param SchemaRegistryRetry retry_policy: class that implements retry policy
        :param dict headers: headers to provide custom request headers
        :return: connection session to the schema registry
        :rtype: SchemaRegistrySession
        """

        session = SchemaRegistrySession(self.url)
        session.auth = auth
        if headers:
            session.headers.update(headers)
        if retry_policy:
            adapter = HTTPAdapter(max_retries=retry_policy)
            session.mount(self.url, adapter)
        return session

    @handle_client_error
    @lru_cache(maxsize=None)
    def get_schema(self, schema_id: int) -> str:
        """
        GET /schemas/ids/(in: id)
        
        Retrieves the schema descriptor for the given `id`.
        
        :param int schema_id: unique ID of the registered schema
        :returns: schema
        :rtype: str
        """

        response = self._session.get(url=f"/schemas/ids/{schema_id}")
        response.raise_for_status()
        return response.json().get("schema")

    @handle_client_error
    @lru_cache(maxsize=None)
    def get_schema_id(self, subject, schema: str) -> int:
        """
        POST /subjects/(string: subject)
        
        Check if the given `schema` is registered under the given `subject`
        and returns it's schema ID. If schema doesn't exist it raises an error.
        
        :param str subject: subject name
        :param str schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url=f"/subjects/{subject}", data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")

    @handle_client_error
    @lru_cache(maxsize=None)
    def register_schema(self, subject, schema: str) -> int:
        """
        POST /subjects/(string: subject)/versions

        Register a schema with the registry under the given subject
        and returns the schema ID. Registering the same schema twice is idempotent, 
        so if the schema is already registered it just returns its schema ID. 
        
        :param str subject: subject name
        :param str schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url=f"/subjects/{subject}/versions", data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")

    def __get_compatibility(self, subject: str = None) -> CompatibilityLevel:
        config_api = f"/config"
        if subject:
            config_api = f"/config/{subject}"
        response = self._session.get(url=config_api)
        response.raise_for_status()
        level = response.json().get("compatibilityLevel", "")
        return CompatibilityLevel(level.lower())

    def __set_compatibility(
        self, compatibility: CompatibilityLevel, subject: str = None
    ):
        config_api = f"/config"
        if subject:
            config_api = f"/config/{subject}"
        response = self._session.put(
            url=config_api, data=json.dumps({"compatibility": str(compatibility)}),
        )
        response.raise_for_status()

    @handle_client_error
    def get_default_compatibility(self) -> CompatibilityLevel:
        return self.__get_compatibility()

    @handle_client_error
    def get_subject_compatibility(self, subject: str) -> CompatibilityLevel:
        return self.__get_compatibility(subject)

    @handle_client_error
    def set_default_compatibility(self, compatibility: CompatibilityLevel):
        return self.__set_compatibility(compatibility)

    @handle_client_error
    def set_subject_compatibility(
        self, subject: str, compatibility: CompatibilityLevel
    ):
        return self.__set_compatibility(compatibility, subject)
