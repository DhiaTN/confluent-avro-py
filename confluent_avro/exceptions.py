class MessageParsingError(Exception):
    """Message format is not compliant with the wire format"""

    def __init__(self, message: str, error: Exception = None):
        self.error = error
        self.message = message


class SchemaParsingError(Exception):
    """Error while parsing a JSON schema descriptor."""


class InvalidWriterStream(Exception):
    """Error while writing to the stream buffer."""


class DecodingError(Exception):
    """Error while decoding the avro binary."""


class EncodingError(Exception):
    """Error while encoding data in to avro binary."""
