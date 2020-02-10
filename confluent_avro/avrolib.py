import json
from io import BytesIO, StringIO
from typing import Union

import fastavro
from fastavro.schema import SchemaParseException as FastavroParseException

from confluent_avro.exceptions import (
    DecodingError,
    EncodingError,
    InvalidWriterStream,
    SchemaParsingError,
)


def loads(avro_schema: Union[str, bytes]) -> dict:
    """Deserialize ``avro_schema`` (a ``str`` or ``bytes`` instance
    containing a avro schema) to a Python dict
    :param str avro_schema: An avro schema descriptor (text-like)
    :returns: Parsed avro schema
    :rtype: dict
    """

    try:
        schema_json = json.loads(avro_schema)
        return fastavro.parse_schema(schema_json)
    except (ValueError, TypeError, FastavroParseException) as e:
        raise SchemaParsingError(f"Schema parse failed: {e}")


def load(avro_fp: Union[StringIO, BytesIO]) -> dict:
    """Parse an avro schema from a file-like object
    :param str|bytes avro_fp: a file-like object containing avro schema
    :returns: Parsed avro schema
    :rtype: dict
    """

    try:
        return loads(avro_fp.read())
    except AttributeError as e:
        raise SchemaParsingError(f"Invalid file descriptor: {e}")


class Decoder(object):
    """Avro decoder wrapper"""

    def __init__(self, avro_schema: str):
        self.schema_str = avro_schema
        self.schema = loads(avro_schema)

    def decode(self, input_stream: BytesIO):
        try:
            return fastavro.schemaless_reader(input_stream, self.schema)
        except (TypeError, AttributeError) as e:
            raise DecodingError(
                f"Expected BytesIO as input, fround {input_stream}: {e}"
            )


class Encoder(object):
    """Avro encoder wrapper"""

    def __init__(self, avro_schema: str):
        self.schema_str = avro_schema
        self.schema = loads(avro_schema)

    def encode(self, data: dict, output_stream: BytesIO):
        try:
            fastavro.schemaless_writer(output_stream, self.schema, data)
            return output_stream.getvalue()
        except ValueError as e:
            raise EncodingError(f"Data is not valid: {data}\n{e}")
        except (TypeError, AttributeError) as e:
            raise InvalidWriterStream(
                f"Expected BytesIO type, fround {output_stream}: {e}"
            )
