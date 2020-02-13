[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/confluent_avro?label=Python)](https://pypi.org/project/confluent-avro/)
[![Build Status](https://travis-ci.com/DhiaTN/confluent-avro-py.svg?branch=master)](https://travis-ci.com/DhiaTN/avrokafka-py)
[![Maintainability](https://api.codeclimate.com/v1/badges/fd596527a28dcaea1a2d/maintainability)](https://codeclimate.com/github/DhiaTN/confluent-avro-py/maintainability)
[![codecov](https://codecov.io/gh/DhiaTN/confluent-avro-py/branch/master/graph/badge.svg)](https://codecov.io/gh/DhiaTN/confluent-avro-py)
[![PyPI version](https://badge.fury.io/py/confluent_avro.svg)](https://badge.fury.io/py/confluent_avro)
[![PyPI - License](https://img.shields.io/pypi/l/confluent_avro?color=ff69b4&label=License)](https://opensource.org/licenses/Apache-2.0)

<br />
<p align="center">
  <h1 align="center">ConfluentAvro</h1>

  <p align="center">
    An Avro SerDe implementation that integrates with the <a href="https://www.confluent.io/confluent-schema-registry/">confluent schema registry</a> and serializes and deserializes data according to the defined <a href="http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format">confluent wire format</a>.
    <br />
    <br />
    <a href="examples">View Demo</a>
    ·
    <a href="https://github.com/DhiaTN/confluent-avro-py/issues">Report Bug</a>
    ·
    <a href="https://github.com/DhiaTN/confluent-avro-py/issues">Request Feature</a>
  </p>
</p>

## Getting Started

### Background

To solve [schema management](https://docs.confluent.io/current/schema-registry/index.html) issues and ensure compatibility in the development of Kafka-based applications, the confluent team introduced the schema registry to store and share the schema between the different apps and apply compatibility checks on each newly registered schema. To make the schema sharing easy, they extend the Avro binary format by prepending the schema id before the actual record instead of including the full schema.

-» You can find more about Confluent and Schema Registry in [Confluent documentation](https://docs.confluent.io/current/schema-registry/index.html).

### Implementation 

ConfluentAvro implemented according to the above specification. Before publishing to Kafka topic, the library prepends the schema id to the generated Avro binary and when consuming from Kafka, it retrieves the schema id and fetches the schema from the registry before deserializing the actual data.

The underline API will automatically register new schemas used for the data serialization and will fetch the corresponding schema when deserializing it. Newly registered schemas and fetched schemas are both cached locally to speed up the process for future records.

» The ConfluentAvro's bullet points:

- Supports the confluent wire format
- Integrates with the confluent schema registry
- Retries with exponential backoff if connection to registry failed
- Implements caching at the schema registry level
- The underline decoder/encoder is built once for the same schema and reused for all upcoming records 
- Can be integrated with different Kafka clients


### Built With

- [fastavro](https://fastavro.readthedocs.io/en/latest/) (check [fastavro benchmark](https://github.com/DhiaTN/avro-benchmarking-py3))
- [requests](https://requests.readthedocs.io)

### Installation

```shell script
» pip install confluent-avro
```

### Usage

> Check [examples](examples) for a fully working demo.

##### Consumer App Example:

```python
from kafka import KafkaConsumer

from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

KAFKA_TOPIC = "confluent_avro-example-topic"

registry_client = SchemaRegistry(
    "https://myschemaregistry.com",
    HTTPBasicAuth("username", "password"),
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
)
avroSerde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    group_id="random_group_id",
    bootstrap_servers=["localhost:9092",]
)

for msg in consumer:
    v = avroSerde.value.deserialize(msg.value)
    k = avroSerde.key.deserialize(msg.key)
    print(msg.offset, msg.partition, k, v)
```

##### Producer App Example:

```python
from kafka import KafkaProducer

from confluent_avro import AvroKeyValueSerde, SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth

KAFKA_TOPIC = "confluent_avro-example-topic"

registry_client = SchemaRegistry(
    "https://myschemaregistry.com",
    HTTPBasicAuth("username", "password"),
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
)

avroSerde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
producer.send(
    KAFKA_TOPIC,
    key=avroSerde.key.serialize({...}, key_schema),
    value=avroSerde.value.serialize({...}, value_schema),
)
```