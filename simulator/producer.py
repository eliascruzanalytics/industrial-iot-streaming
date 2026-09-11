"""
IoT Producer Entrypoint.

Delegates telemetry publishing exclusively to the MQTT layer (Mosquitto).
IoT devices communicate via MQTT and remain completely decoupled from Kafka details.
"""

from simulator.mqtt_producer import run_mqtt_simulation

if __name__ == "__main__":
    run_mqtt_simulation()
