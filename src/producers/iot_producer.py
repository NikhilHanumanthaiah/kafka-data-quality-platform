import random
import time
import uuid
from datetime import datetime
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Kafka and Schema Registry configs
KAFKA_BROKERS = "localhost:19091,localhost:19092,localhost:19093"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "raw_iot_data"

# Load Avro schema
value_schema = avro.load("schemas/iot_sensor.avsc")

def generate_sensor_data():
    """Simulate one sensor event."""
    return {
        "device_id": str(uuid.uuid4())[:8],
        "temperature": round(random.uniform(18.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "battery_level": round(random.uniform(20.0, 100.0), 2),
        "timestamp": int(datetime.utcnow().timestamp() * 1000)
    }

def main():
    # Configure Avro Producer
    avro_producer = AvroProducer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            "acks": "all",
            "retries": 3,
            "enable.idempotence": True
        },
        default_value_schema=value_schema
    )

    print("Producing IoT sensor data to Kafka topic:", TOPIC)
    while True:
        record = generate_sensor_data()
        try:
            avro_producer.produce(topic=TOPIC, value=record)
            avro_producer.flush()
            print(f"Produced record: {record}")
        except Exception as e:
            print("Failed to send record:", e)
        time.sleep(1)

if __name__ == "__main__":
    main()
