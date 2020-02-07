# avrokafka

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/avrokafka?color=blue&label=Python)](https://pypi.org/project/avrokafka/)
[![Build Status](https://travis-ci.com/DhiaTN/avrokafka-py.svg?branch=master)](https://travis-ci.com/DhiaTN/avrokafka-py)
[![Maintainability](https://api.codeclimate.com/v1/badges/cc863ec33bb0cdb7f515/maintainability)](https://codeclimate.com/github/DhiaTN/avrokafka-py/maintainability)
[![codecov](https://codecov.io/gh/DhiaTN/avrokafka-py/branch/master/graph/badge.svg)](https://codecov.io/gh/DhiaTN/avrokafka-py)
[![PyPI version](https://badge.fury.io/py/avrokafka.svg)](https://badge.fury.io/py/avrokafka)
[![PyPI - License](https://img.shields.io/pypi/l/avrokafka?color=ff69b4&label=License)](https://opensource.org/licenses/Apache-2.0)
-----------
A schema-registry aware avro serde (serializer/deserializer) to work with Apache Kafka

## Installation

```shell script
pip install avrokafka
```

## Usage:

> Check [examples](examples) for a full working demo.

##### Consumer App Example:

```python
from kafka import KafkaConsumer

from avrokafka.schema_registry import SchemaRegistry
from avrokafka.schema_registry.auth import RegistryHTTPBasicAuth
from avrokafka.serde import AvroKeyValueSerde

KAFKA_TOPIC = "avrokafka-example-topic"

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

from avrokafka.schema_registry import SchemaRegistry
from avrokafka.schema_registry.auth import RegistryHTTPBasicAuth
from avrokafka.serde import AvroKeyValueSerde

KAFKA_TOPIC = "avrokafka-example-topic"

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