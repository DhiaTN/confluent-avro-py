from pathlib import Path
import time
import random

from kafka import KafkaProducer

from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.serde import AvroKeyValueSerde

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_TOPIC = "telecom_italia_data"
BASE_DIR = Path(__file__).parent

registry_client = SchemaRegistry(SCHEMA_REGISTRY_URL)
avro_serde = AvroKeyValueSerde(registry_client, KAFKA_TOPIC)
kafka_producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

records = [
    {
        "key": {
            "SquareId": i
        },
        "value": {
            "SquareId": i,
            "TimeInterval": int(time.time()),
            "CountryCode": random.randint(1, 50),
            "SmsInActivity": random.uniform(0.001, 0.09),
            "SmsOutActivity": random.uniform(0.001, 0.09),
            "CallInActivity": None,
            "CallOutActivity": None,
            "InternetTrafficActivity": None
        }
    } for i in range(5000, 5013)
]

if __name__ == "__main__":
    key_schema = (BASE_DIR / "telecom_italia_data-key.json").read_text()
    value_schema = (BASE_DIR / "telecom_italia_data-value.json").read_text()

    for record in records:
        f = kafka_producer.send(
            KAFKA_TOPIC,
            key=avro_serde.key.serialize(record['key'], key_schema),
            value=avro_serde.value.serialize(record['value'], value_schema),
        )
        print(f.get(timeout=60))
