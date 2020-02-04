from io import BytesIO

import avro.io
import avro.schema as AvroSchema


class SchemaParsingError(Exception):
    """Error while parsing a JSON schema descriptor."""

    pass


def loads(schema_str):
    """ Parse a schema given a schema string """
    try:
        return AvroSchema.Parse(schema_str)
    except AvroSchema.SchemaParseException as e:
        raise SchemaParsingError("Schema parse failed: {}".format(str(e)))


def load(fp):
    """ Parse a schema from a file path """
    with open(fp) as f:
        return loads(f.read())


class Decoder(object):
    """Avro decoder wrapper"""

    def __init__(self, avro_schema: str):
        self.schema_str = avro_schema
        self.schema = loads(avro_schema)
        self.reader = avro.io.DatumReader(self.schema)

    def decode(self, reader_stream: BytesIO):
        bin_decoder = avro.io.BinaryDecoder(reader_stream)
        return self.reader.read(bin_decoder)


class Encoder(object):
    """Avro encoder wrapper"""

    def __init__(self, avro_schema: str):
        self.schema_str = avro_schema
        self.schema = loads(avro_schema)
        self.writer = avro.io.DatumWriter(self.schema)

    def encode(self, data, writer_stream: BytesIO):
        encoder = avro.io.BinaryEncoder(writer_stream)
        self.writer.write(data, encoder)
        return writer_stream.getvalue()
