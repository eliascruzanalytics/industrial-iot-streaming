# MQTT Communication Layer Specification

## Overview

The MQTT layer acts as the lightweight edge messaging protocol for industrial IoT device telemetry communication.

```text
[IoT Device Simulators]
       │
       │ MQTT (QoS 1, topic: industrial/machines/{device_id}/telemetry)
       ▼
┌──────────────┐
│  Mosquitto   │  Port 1883
│ MQTT Broker  │
└──────┬───────┘
       │
       │ Subscribe (industrial/machines/+/telemetry)
       ▼
┌──────────────┐
│ MQTT-Kafka   │  bridge/mqtt_to_kafka.py
│    Bridge    │
└──────┬───────┘
       │
       │ Kafka Producer (Key: device_id)
       ▼
┌──────────────┐
│    Kafka     │  Topic: iot-machine-events
└──────────────┘
```

## Broker Configuration

- **Engine**: Eclipse Mosquitto v2 (`eclipse-mosquitto:2`)
- **Port**: `1883` (Default MQTT listener), `9001` (WebSockets)
- **Configuration File**: `mqtt/mosquitto.conf`
  - `listener 1883`
  - `allow_anonymous true` (Local lab mode)
  - `persistence true`

---

## Topic Design & Routing

- **Device Topic Pattern**: `industrial/machines/{device_id}/telemetry`
- **Wildcard Subscription**: `industrial/machines/+/telemetry`
- **Device Identifiers**: `MTR-001`, `MTR-002`, `MTR-003`, `MTR-004`, `MTR-005`

---

## Quality of Service (QoS 1)

Telemetry events are published using **MQTT QoS 1 (At least once delivery)**.

```text
QoS 1 (At least once) ──> Potential Message Duplication ──> PySpark event_id Deduplication
```

This guarantees message delivery from edge devices while demonstrating downstream idempotency in PySpark Structured Streaming using `event_id` deduplication.

---

## MQTT → Kafka Bridge (`bridge/mqtt_to_kafka.py`)

- **Subscribes**: `industrial/machines/+/telemetry`
- **Validation**: Validates raw JSON payload structure before routing.
- **Kafka Message Key**: Extracts `device_id` as Kafka partition message key.
- **Target Topic**: `iot-machine-events`
- **Fault-Tolerance**: Automatic MQTT broker reconnect and retry loops.
