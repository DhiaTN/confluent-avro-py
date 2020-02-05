import json

import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from avrokafka.schema_registry.errors import handle_client_error

HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
RETRIE_POLICY = Retry(
    total=5,
    backoff_factor=0.3,
    method_whitelist=False,
    raise_on_status=(500, 502, 503, 504),
)


class SchemaRegistry(object):
    def __init__(
        self,
        url: str,
        auth: AuthBase = None,
        schema_id_size: int = 4,
        retry_policy: Retry = RETRIE_POLICY,
        headers: dict = HEADERS,
    ):
        self.url = url
        self.schema_id_size = schema_id_size
        self._session = self._build_session(auth, retry_policy, headers)

    def _build_session(
        self, auth: AuthBase, retry_policy: Retry = None, headers: dict = {}
    ):
        """
        Provides persistent connection to the schema registry 
        to be reused for all client interaction. It supports mutiple auth types 
        and implements rety mechanism.
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
    def get_schema(self, id: int):
        """
        GET /schemas/ids/(in: id)
        
        Retrives the schema descriptor for the given `id`.
        
        :param str subject: subject name
        :param dict schema: Avro schema to be registered
        :returns: schema
        :rtype: str
        """

        response = self._session.get(
            url="{url}/schemas/ids/{id}".format(url=self.url, id=id),
        )
        response.raise_for_status()
        return response.json().get("schema")

    @handle_client_error
    def get_schema_id(self, subject: str, schema: dict):
        """
        POST /subjects/(string: subject)
        
        Check existence of the given `schema` registred under the given `subject`
        and returns the schema ID. If schema doesn't exist it raises an error.
        
        :param str subject: subject name
        :param dict schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url="{url}/subjects/{subject}".format(url=self.url, subject=subject),
            data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")

    @handle_client_error
    def register_schema(self, subject: str, schema: dict):
        """
        POST /subjects/(string: subject)/versions

        Register a schema with the registry under the given subject
        and returns the schema ID. If the schema is already registred 
        it just returns its schema ID. 
        
        :param str subject: subject name
        :param dict schema: Avro schema to be registered
        :returns: schema_id
        :rtype: int
        """

        response = self._session.post(
            url="{url}/subjects/{subject}/versions".format(
                url=self.url, subject=subject
            ),
            data=json.dumps({"schema": schema}),
        )
        response.raise_for_status()
        return response.json().get("id")
