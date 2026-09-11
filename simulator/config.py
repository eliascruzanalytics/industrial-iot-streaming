import os
from dotenv import load_dotenv

# Load environment variables from .env file if available
load_dotenv()

# MQTT Settings
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_TOPIC_PATTERN = os.getenv("MQTT_TOPIC_PATTERN", "industrial/machines/{device_id}/telemetry")

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")

# Simulator Settings
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "5.0"))

DEVICES = [
    {"device_id": "MTR-001", "machine_type": "Motor", "location": "Factory_Floor_A"},
    {"device_id": "MTR-002", "machine_type": "Motor", "location": "Factory_Floor_A"},
    {"device_id": "MTR-003", "machine_type": "Motor", "location": "Factory_Floor_B"},
    {"device_id": "MTR-004", "machine_type": "Motor", "location": "Factory_Floor_B"},
    {"device_id": "MTR-005", "machine_type": "Motor", "location": "Factory_Floor_C"},
]
