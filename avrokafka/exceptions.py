class SerializerError(Exception):
    def __init__(self, message: str, error=None):
        self.error = error
        self.message = message

    def __repr__(self):
        return self.message


class SchemaParsingError(Exception):
    """Error while parsing a JSON schema descriptor."""

    pass


class InvalidWriterStream(Exception):
    """Error while writing to the stream buffer."""

    pass


class DecodingError(Exception):
    """Error while decoding the avro binary."""

    pass


class EncodingError(Exception):
    """Error while encoding data in to avro binary."""

    pass
