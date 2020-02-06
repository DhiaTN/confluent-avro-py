import json
from io import BytesIO

import fastavro
from fastavro.schema import SchemaParseException as FastavroParseException


from avrokafka.exceptions import (
    SchemaParsingError,
    InvalidWriterStream,
    DecodingError,
    EncodingError,
)


def loads(avro_schema: str):
    try:
        schema_json = json.loads(avro_schema)
        return fastavro.parse_schema(schema_json)
    except (ValueError, TypeError, FastavroParseException) as e:
        raise SchemaParsingError("Schema parse failed: {}".format(str(e)))


def load(avro_fp: str):
    try:
        with open(avro_fp, "r") as f:
            return loads(f.read())
    except FileNotFoundError as e:
        raise SchemaParsingError("Schema parse failed: {}".format(str(e)))


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
                "Expected BytesIO as input, fround {}: {}".format(input_stream, str(e))
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
            raise EncodingError("Data is not valid: {}\n{}".format(data, str(e)))
        except (TypeError, AttributeError) as e:
            raise InvalidWriterStream(
                "Expected BytesIO type, fround {}: {}".format(output_stream, str(e))
            )
