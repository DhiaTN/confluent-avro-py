import sys

from kafka import KafkaConsumer

from avrokafka.schema_registry import SchemaRegistry
from avrokafka.serde import AvroSerde

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_TOPIC = "telecom_italia_data"

registry_client = SchemaRegistry(SCHEMA_REGISTRY_URL)
avro_serde = AvroSerde(registry_client, KAFKA_TOPIC)

if __name__ == "__main__":
    group_id = ".".join(sys.argv)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        group_id=group_id,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
    )

    print("{} start consuming!...".format(group_id))

    for msg in consumer:
        print("message recieved!")
        print("\n" + "=" * 50 + "\n")
        v = avro_serde.deserialize(msg.value)
        k = avro_serde.deserialize(msg.key)
        print(msg.offset, msg.partition, k, v)
