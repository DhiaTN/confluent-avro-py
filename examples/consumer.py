import sys

from kafka import KafkaConsumer

from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.serde import AvroKeyValueSerde

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_TOPIC = "telecom_italia_data"

registry_client = SchemaRegistry(SCHEMA_REGISTRY_URL)
avro_serde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)

if __name__ == "__main__":
    group_id = ".".join(sys.argv)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        group_id=group_id,
        bootstrap_servers=["localhost:9092"],
    )

    print("{} start consuming!...".format(group_id))

    for msg in consumer:
        print("\nmessage recieved!" + "=" * 50 + "\n")
        k = avro_serde.key.deserialize(msg.key)
        v = avro_serde.value.deserialize(msg.value)
        print(msg.offset, msg.partition, k, v)
