import json

from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from avrokafka.schema_registry.errors import handle_client_error
from avrokafka.schema_registry.auth import RegistryAuthBase


class SchemaRegistryRetry(Retry):
    pass


HEADERS = {"Content-Type": "application/json"}
RETRY_POLICY = SchemaRegistryRetry(
    total=5,
    backoff_factor=0.3,
    method_whitelist=False,
    raise_on_status=(500, 502, 503, 504),
)


class SchemaRegistry(object):
    def __init__(
        self,
        url: str,
        auth: RegistryAuthBase = None,
        schema_id_size: int = 4,
        retry_policy: SchemaRegistryRetry = RETRY_POLICY,
        headers: dict = HEADERS,
    ):
        self.url = url
        self.schema_id_size = schema_id_size
        self._session = self._build_session(auth, retry_policy, headers)

    def _build_session(
        self,
        auth: RegistryAuthBase = None,
        retry_policy: SchemaRegistryRetry = None,
        headers: dict = {},
    ) -> requests.Session:
        """
        Provides persistent connection to the schema registry 
        to be reused for all client interaction. It supports multiple auth types
        and implements retry mechanism.
        return: connection session to the schema registry
        rtype: requests.Session
        """

        session = requests.Session()
        session.auth = auth
        session.headers.update(headers)
        if retry_policy:
            adapter = HTTPAdapter(max_retries=retry_policy)
            session.mount(self.url, adapter)
        return session

    @handle_client_error
    @lru_cache(maxsize=None)
    def get_schema(self, id: int) -> str:
        """
        GET /schemas/ids/(in: id)
        
        Retrives the schema descriptor for the given `id`.
        
        :param str subject: subject name
        :param dict schema: Avro schema to be registered
        :returns: schema
        :rtype: str
        """

        response = self._session.get(url=f"{self.url}/schemas/ids/{id}")
        response.raise_for_status()
        return response.json().get("schema")

    @handle_client_error
    @lru_cache(maxsize=None)
    def get_schema_id(self, subject, schema: str) -> int:
        """
        POST /subjects/(string: subject)
        
        Check existence of the given `schema` registered under the given `subject`
        and returns the schema ID. If schema doesn't exist it raises an error.
        
        :param str subject: subject name
        :param str schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url=f"{self.url}/subjects/{subject}", data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")

    @handle_client_error
    @lru_cache(maxsize=None)
    def register_schema(self, subject, schema: str) -> int:
        """
        POST /subjects/(string: subject)/versions

        Register a schema with the registry under the given subject
        and returns the schema ID. If the schema is already registered
        it just returns its schema ID. 
        
        :param str subject: subject name
        :param str schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url=f"{self.url}/subjects/{subject}/versions",
            data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")
