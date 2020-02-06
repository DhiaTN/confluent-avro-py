from functools import wraps

import status
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, ReadTimeout, Timeout


class SchemaRegistryError(Exception):  # pragma: no cover
    """Base Schema Registry error"""

    def __init__(self, error: dict, message: str, status_code: int = None):
        self.client_error = error
        self.message = message
        self.status_code = status_code

    def __repr__(self):
        return "<{} msg={}, status={}>".format(
            self.__class__.name, self.message, self.status_code
        )


class SchemaRegistryNetworkError(SchemaRegistryError):
    def __init__(self, error):
        self.message = "Schema registry is not reachable"
        self.client_error = error
        self.status_code = status.HTTP_503_SERVICE_UNAVAILABLE


class SchemaRegistryUnavailable(SchemaRegistryError):
    def __init__(self, error):
        self.message = "Unexpected SchemaRegistry Error"
        self.client_error = error
        self.status_code = status.HTTP_503_SERVICE_UNAVAILABLE


class UnauthorizedAccess(SchemaRegistryError):
    def __init__(self, error):
        self.message = "Unauthorized Access"
        self.client_error = error
        self.status_code = status.HTTP_401_UNAUTHORIZED


class IncompatibleSchemaVersion(SchemaRegistryError):
    def __init__(self, error):
        self.message = "Schema being registered is incompatible with earlier version"
        self.client_error = error
        self.status_code = status.HTTP_409_CONFLICT


class InvalidAvroSchema(SchemaRegistryError):
    def __init__(self, error):
        self.message = "Input schema is an invalid Avro schema"
        self.client_error = error
        self.status_code = status.HTTP_400_BAD_REQUEST


class SchemaNotFoundError(SchemaRegistryError):
    def __init__(self, error):
        self.message = error.get("message", "Schema not found")
        self.client_error = error
        self.status_code = status.HTTP_404_NOT_FOUND


class ClientHTTPError(object):
    def __init__(self, error):
        self.http_error = error

    @property
    def status_code(self):
        return self.http_error.response.status_code

    @property
    def details(self):
        try:
            error = self.http_error.response.json()
        except ValueError:
            error = {"message": self.http_error.response.text}
        return {**error, **{"status_code": self.status_code}}


def handle_client_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            print(func.cache_info())
        except (Timeout, ReadTimeout, RequestsConnectionError) as e:
            raise SchemaRegistryNetworkError(error={"message": e.__doc__})
        except HTTPError as e:
            client_error = ClientHTTPError(e)
            if client_error.status_code in [
                status.HTTP_401_UNAUTHORIZED,
                status.HTTP_403_FORBIDDEN,
            ]:
                raise UnauthorizedAccess(client_error.details)
            if client_error.status_code == status.HTTP_404_NOT_FOUND:
                raise SchemaNotFoundError(client_error.details)
            elif client_error.status_code == status.HTTP_409_CONFLICT:
                raise IncompatibleSchemaVersion(client_error.details)
            elif client_error.status_code == 422:
                raise InvalidAvroSchema(client_error.details)
            else:
                raise SchemaRegistryUnavailable(client_error.details)
        return response

    return wrapper
