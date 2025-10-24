import faust
import statistics
import asyncio
from datetime import datetime
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafka + Schema Registry configs
BOOTSTRAP_SERVERS = "localhost:19091"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

RAW_TOPIC = "raw_iot_data"
VALID_TOPIC = "validated_iot_data"
ANOMALY_TOPIC = "anomalies_iot_data"
METRICS_TOPIC = "dq_metrics_iot"

# Faust app (used for output publishing)
app = faust.App(
    "iot-validator",
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    store="memory://",
)

# Faust topics (output only)
valid_stream = app.topic(VALID_TOPIC, value_type=dict)
anomaly_stream = app.topic(ANOMALY_TOPIC, value_type=dict)
metrics_stream = app.topic(METRICS_TOPIC, value_type=dict)

# In-memory store for stats
window_temperatures = []

# --- Schema Registry + Avro setup ---
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# The deserializer automatically fetches the schema ID from Schema Registry
avro_deserializer = AvroDeserializer(schema_registry_client)

# --- Kafka Consumer to read Avro messages ---
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "iot-validator-group",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe([RAW_TOPIC])


def is_valid(record: dict) -> bool:
    """Basic schema + value validation rules."""
    if not record.get("device_id"):
        return False
    temp = record.get("temperature")
    hum = record.get("humidity")
    batt = record.get("battery_level")
    if temp is None or temp < -20 or temp > 80:
        return False
    if hum is None or hum < 0 or hum > 100:
        return False
    if batt is not None and (batt < 0 or batt > 100):
        return False
    return True


def is_anomaly(record: dict) -> bool:
    """Simple 3σ outlier detection on temperature."""
    if len(window_temperatures) < 30:
        return False
    mean = statistics.mean(window_temperatures)
    stdev = statistics.stdev(window_temperatures)
    return abs(record["temperature"] - mean) > 3 * stdev


async def process_record(record: dict):
    """Process and route a single record."""
    window_temperatures.append(record["temperature"])
    if len(window_temperatures) > 1000:
        window_temperatures.pop(0)

    # Validate and classify
    if not is_valid(record) or is_anomaly(record):
        await anomaly_stream.send(value=record)
    else:
        await valid_stream.send(value=record)

    # Publish metrics periodically
    now = datetime.utcnow().isoformat()
    metrics = {
        "timestamp": now,
        "total_records": len(window_temperatures),
        "avg_temp": round(statistics.mean(window_temperatures), 2),
        "temp_stdev": round(statistics.stdev(window_temperatures), 2) if len(window_temperatures) > 2 else 0.0,
    }
    await metrics_stream.send(value=metrics)


@app.timer(interval=2.0)
async def poll_avro_records():
    """Poll Avro messages from Kafka, decode, and feed to Faust streams."""
    try:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            return
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            return

        record = avro_deserializer(
            msg.value(),
            SerializationContext(RAW_TOPIC, MessageField.VALUE)
        )

        if record is not None:
            await process_record(record)
    except Exception as e:
        print("Error decoding Avro message:", e)


@app.task
async def on_started():
    print("IoT Validator is running with Avro + Schema Registry support...")
    while True:
        await asyncio.sleep(1)
