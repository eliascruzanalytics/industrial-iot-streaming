import json
import logging
import time
import sys
from typing import Dict, Any
import paho.mqtt.client as mqtt

from simulator.config import MQTT_HOST, MQTT_PORT, MQTT_QOS, MQTT_TOPIC_PATTERN, EVENT_INTERVAL_SECONDS, DEVICES
from simulator.sensor import SensorSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MQTTProducer")


class IoTMqttProducer:
    """MQTT Producer client for sending industrial telemetry events to Mosquitto broker."""

    def __init__(self, host: str = MQTT_HOST, port: int = MQTT_PORT, qos: int = MQTT_QOS):
        self.host = host
        self.port = port
        self.qos = qos
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="iot_sensor_producer")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.connected = False

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.connected = True
            logger.info(f"[MQTT] Connected successfully to Mosquitto broker at {self.host}:{self.port}")
        else:
            logger.error(f"[MQTT][ERROR] Connection failed with return code {rc}")

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        self.connected = False
        logger.warning(f"[MQTT] Disconnected from Mosquitto broker (rc={rc}). Reconnecting...")

    def connect(self, max_retries: int = 5, retry_interval: int = 2):
        """Connects to Mosquitto MQTT Broker with retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[MQTT] Connecting to broker {self.host}:{self.port} (Attempt {attempt}/{max_retries})...")
                self.client.connect(self.host, self.port, keepalive=60)
                self.client.loop_start()
                
                # Wait briefly for connection callback
                start_time = time.time()
                while not self.connected and (time.time() - start_time) < 3.0:
                    time.sleep(0.1)
                
                if self.connected:
                    return
            except Exception as e:
                logger.error(f"[MQTT][ERROR] Failed connection attempt {attempt}: {e}")
                time.sleep(retry_interval)

        logger.warning(f"[MQTT][WARNING] Could not establish connection after {max_retries} attempts.")

    def publish_event(self, event: Dict[str, Any]):
        """Publishes JSON event payload to device-specific MQTT topic."""
        device_id = event["device_id"]
        topic = MQTT_TOPIC_PATTERN.format(device_id=device_id)
        payload_str = json.dumps(event)

        info = self.client.publish(topic, payload_str, qos=self.qos)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"[MQTT] Event {event['event_id']} published to topic '{topic}' (QoS {self.qos})")
        else:
            logger.error(f"[MQTT][ERROR] Failed to publish event {event['event_id']} (rc={info.rc})")

    def disconnect(self):
        """Disconnects client cleanly."""
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("[MQTT] Disconnected cleanly from Mosquitto broker.")


def run_mqtt_simulation(max_events: int = None, inject_bad_data: bool = False):
    """Main simulation execution loop publishing to MQTT."""
    producer = IoTMqttProducer()
    producer.connect()

    simulators = [SensorSimulator(**dev) for dev in DEVICES]
    logger.info(f"[MQTT] Started IoT Producer emitting to pattern '{MQTT_TOPIC_PATTERN}' for {len(simulators)} devices.")

    round_count = 0
    try:
        while True:
            round_count += 1
            logger.info(f"--- Emission Round {round_count} ---")
            
            for sim in simulators:
                event = sim.step()
                
                if inject_bad_data and round_count % 5 == 0 and sim.device_id == "MTR-003":
                    event["temperature"] = -999.0
                    logger.warning(f"[TEST INJECTION] Injected negative temperature event: {event['event_id']}")
                
                producer.publish_event(event)

            if max_events and round_count >= max_events:
                logger.info(f"[MQTT] Reached maximum event rounds limit ({max_events}). Stopping.")
                break

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("[MQTT] Simulation interrupted by user.")
    finally:
        producer.disconnect()


if __name__ == "__main__":
    run_mqtt_simulation()
