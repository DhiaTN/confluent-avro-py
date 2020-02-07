# avrokafka

[![Build Status](https://travis-ci.com/DhiaTN/avrokafka-py.svg?token=okpqLNGkmg8xM5NJasss&branch=master)](https://travis-ci.com/DhiaTN/avrokafka-py)

A schema-registry aware avro serde (serializer/deserializer) to work with Apache Kafka

## Installation

```shell script
pip install avrokafka
```

## Usage:

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