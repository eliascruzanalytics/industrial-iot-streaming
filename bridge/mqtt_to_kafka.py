import json
import logging
import os
import sys
import time
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MQTTKafkaBridge")

# Environment configurations
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "industrial/machines/+/telemetry")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")


def validate_json_payload(raw_payload: str) -> Optional[Dict[str, Any]]:
    """Validates if raw_payload is valid JSON and extracts object."""
    try:
        data = json.loads(raw_payload)
        if isinstance(data, dict):
            return data
        logger.error("[BRIDGE][ERROR] Invalid JSON payload: Root element is not a JSON Object.")
        return None
    except Exception as ex:
        logger.error(f"[BRIDGE][ERROR] Invalid JSON payload parse failure: {ex}")
        return None


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Creates Kafka producer for forwarding MQTT messages."""
    logger.info(f"[KAFKA] Initializing Kafka Producer to {bootstrap_servers}")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all"
    )


class MqttToKafkaBridge:
    """MQTT-to-Kafka Bridge forwarding MQTT telemetry to Kafka event bus."""

    def __init__(
        self,
        mqtt_host: str = MQTT_HOST,
        mqtt_port: int = MQTT_PORT,
        mqtt_topic: str = MQTT_TOPIC,
        mqtt_qos: int = MQTT_QOS,
        kafka_bootstrap: str = KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic: str = KAFKA_TOPIC,
        kafka_producer: Optional[KafkaProducer] = None
    ):
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.mqtt_qos = mqtt_qos
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        
        self.kafka_producer = kafka_producer
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt_to_kafka_bridge")
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.is_connected = False

    def init_kafka(self):
        """Lazy initialization of Kafka producer if not injected."""
        if not self.kafka_producer:
            self.kafka_producer = create_kafka_producer(self.kafka_bootstrap)

    def on_mqtt_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.is_connected = True
            logger.info(f"[MQTT] Connected to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")
            logger.info(f"[MQTT] Subscribing to wildcard topic '{self.mqtt_topic}' (QoS {self.mqtt_qos})...")
            client.subscribe(self.mqtt_topic, qos=self.mqtt_qos)
            logger.info(f"[MQTT] Subscribed to {self.mqtt_topic}")
        else:
            logger.error(f"[MQTT][ERROR] Failed connection with return code {rc}")

    def on_mqtt_disconnect(self, client, userdata, flags, rc, properties=None):
        self.is_connected = False
        logger.warning(f"[MQTT] Disconnected from broker (rc={rc}). Will attempt reconnection.")

    def process_message(self, raw_topic: str, raw_payload: str) -> bool:
        """
        Core payload validation and Kafka publishing logic.
        Returns True if successfully validated and sent to Kafka, False otherwise.
        """
        logger.info(f"[MQTT] Message received on topic '{raw_topic}'")
        event = validate_json_payload(raw_payload)
        
        if not event:
            logger.error(f"[BRIDGE][ERROR] Dropped invalid message from topic '{raw_topic}'")
            return False

        device_id = event.get("device_id")
        event_id = event.get("event_id")

        if not device_id:
            logger.warning("[BRIDGE][WARNING] Message payload missing 'device_id'. Using None as Kafka key.")

        try:
            self.init_kafka()
            future = self.kafka_producer.send(
                topic=self.kafka_topic,
                key=device_id,
                value=event
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                f"[KAFKA] Event published -> topic={record_metadata.topic} "
                f"device_id={device_id} event_id={event_id} partition={record_metadata.partition} offset={record_metadata.offset}"
            )
            return True
        except KafkaError as ke:
            logger.error(f"[KAFKA][ERROR] Publish failed for event {event_id}: {ke}")
            return False
        except Exception as ex:
            logger.error(f"[BRIDGE][ERROR] Unexpected error processing event {event_id}: {ex}")
            return False

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode("utf-8")
            self.process_message(msg.topic, payload_str)
        except Exception as ex:
            logger.error(f"[BRIDGE][ERROR] Error decoding MQTT message payload: {ex}")

    def start(self, retry_interval: int = 5):
        """Starts bridge loop with reconnection retry handling."""
        self.init_kafka()
        
        while True:
            try:
                logger.info(f"[BRIDGE] Connecting MQTT client to {self.mqtt_host}:{self.mqtt_port}...")
                self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
                self.mqtt_client.loop_forever()
            except Exception as ex:
                logger.error(f"[BRIDGE][ERROR] Connection attempt failed: {ex}. Retrying in {retry_interval}s...")
                time.sleep(retry_interval)


if __name__ == "__main__":
    bridge = MqttToKafkaBridge()
    bridge.start()
