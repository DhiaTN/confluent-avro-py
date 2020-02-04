import json


class SerializerError(Exception):
    def __init__(self, error, message):
        self.error = str(self.error)
        self.message = message

    def __str__(self):
        return self.message


class SchemaRegistryError(Exception):
    """Base Schema Registry error"""

    status_code = None

    def __init__(self, error, message):
        self.error = str(self.error)
        self.message = message

    def __str__(self):
        return self.message


class SchemaRegistryUnkownError(Exception):
    status_code = None

    def __init__(self, error):
        self.error = str(self.error)
        self.message = "Unexpected SchemaRegistry Error"


class SchemaNotFoundError(SchemaRegistryError):
    status_code = 404

    def __init__(self, error):
        self.message = "Schema not found"
        self.error = str(error)


class SchemaNotRegistredError(SchemaRegistryError):
    status_code = 404

    def __init__(self, error):
        self.message = "Schema not registred"
        self.error = str(error)
