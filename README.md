# confluent-avro

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/confluent_avro?label=Python)](https://pypi.org/project/confluent-avro/)
[![Build Status](https://travis-ci.com/DhiaTN/avrokafka-py.svg?branch=master)](https://travis-ci.com/DhiaTN/avrokafka-py)
[![Maintainability](https://api.codeclimate.com/v1/badges/cc863ec33bb0cdb7f515/maintainability)](https://codeclimate.com/github/DhiaTN/avrokafka-py/maintainability)
[![codecov](https://codecov.io/gh/DhiaTN/avrokafka-py/branch/master/graph/badge.svg)](https://codecov.io/gh/DhiaTN/avrokafka-py)
[![PyPI version](https://badge.fury.io/py/confluent_avro.svg)](https://badge.fury.io/py/confluent_avro)
[![PyPI - License](https://img.shields.io/pypi/l/confluent_avro?color=ff69b4&label=License)](https://opensource.org/licenses/Apache-2.0)
-----------

An Avro SerDe implementation that integrates with the confluent schema registry and serializes and deserializes data according to the defined confluent wire format

## Installation

```shell script
pip install confluent-avro
```

## Usage:

> Check [examples](examples) for a full working demo.

##### Consumer App Example:

```python
from kafka import KafkaConsumer

from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.schema_registry.auth import RegistryHTTPBasicAuth
from confluent_avro.serde import AvroKeyValueSerde

KAFKA_TOPIC = "confluent_avro-example-topic"

registry_client = SchemaRegistry(
    "https://myschemaregistry.com",
    RegistryHTTPBasicAuth("username", "password"),
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

from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.schema_registry.auth import RegistryHTTPBasicAuth
from confluent_avro.serde import AvroKeyValueSerde

KAFKA_TOPIC = "confluent_avro-example-topic"

registry_client = SchemaRegistry(
    "https://myschemaregistry.com",
    RegistryHTTPBasicAuth("username", "password"),
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