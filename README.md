# Industrial IoT Streaming Data Pipeline

>  **Guia de Execução em Português**: Para ver o tutorial completo em português passo a passo de como rodar o projeto, acesse [docs/execution_guide.md](file:///c:/Users/elias.cruz/Documents/PYTHON_PROJECT/io_stream/docs/execution_guide.md).

## Context

In modern smart manufacturing, industrial machinery (such as heavy-duty electric motors) is equipped with multi-sensor telemetry nodes that emit high-frequency operational metrics (temperature, pressure, vibration, RPM). 

This project implements a complete near real-time streaming data pipeline on local infrastructure. It simulates industrial edge telemetry emitted via **MQTT (QoS 1)**, ingests messages through **Eclipse Mosquitto**, bridges streams into **Apache Kafka**, processes streaming data in **PySpark Structured Streaming**, applies explicit schema validation and Data Quality filtering, isolates invalid events to **Quarantine (DLQ)**, handles event deduplication and event-time watermarking, enriches data with multi-level anomaly detection (`NORMAL`, `WARNING`, `CRITICAL`), and persists state to **PostgreSQL**.

---

## Architecture Flow

```text
                         INDUSTRIAL IoT
                              │
                              ▼
                     Python Sensor Simulator
                              │
                              │ MQTT / QoS 1
                              ▼
                     ┌──────────────────┐
                     │    Mosquitto     │
                     │   MQTT Broker    │
                     └────────┬─────────┘
                              │
                              │ Subscribe
                              ▼
                     ┌──────────────────┐
                     │ MQTT → Kafka     │
                     │ Bridge           │
                     └────────┬─────────┘
                              │
                              │ Kafka
                              ▼
                     ┌──────────────────┐
                     │      Kafka       │
                     │ iot-machine-     │
                     │ events           │
                     └────────┬─────────┘
                              │
                              ▼
                  ┌─────────────────────────┐
                  │ PySpark Structured      │
                  │ Streaming               │
                  └───────────┬─────────────┘
                              │
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
           BRONZE        DATA QUALITY    QUARANTINE
               │              │
               └──────────────┤
                              ▼
                    Deduplication
                              │
                              ▼
                         Watermark
                              │
                              ▼
                    Anomaly Detection
                              │
                              ▼
                         REFINED
                              │
                              ▼
                       PostgreSQL
                              │
                              ▼
                      Operational BI
```

---

## IoT Communication Layer

Industrial hardware on the factory floor publishes telemetry over **MQTT**, a lightweight messaging protocol optimized for IoT edge networks. 

- **Mosquitto** acts as the local edge MQTT broker listening on port `1883`.
- Devices publish to topic pattern `industrial/machines/{device_id}/telemetry`.
- A Python **MQTT-to-Kafka Bridge** (`bridge/mqtt_to_kafka.py`) subscribes to `industrial/machines/+/telemetry` and forwards validated JSON payloads into Kafka using `device_id` as the partition message key.
- **Kafka** acts as the distributed streaming backbone consumed downstream by **PySpark Structured Streaming**.

---

## Why MQTT + Kafka?

MQTT and Kafka serve different complementary purposes in modern data architectures:

- **MQTT** is designed for lightweight, low-overhead communication between edge IoT devices and local brokers over unreliable networks.
- **Kafka** is designed for high-throughput, distributed event streaming, long-term buffering, and horizontal scalability in data engineering workloads.
- The **MQTT-to-Kafka Bridge** decouples the operational edge IoT layer from the analytical data platform. Sensors emit metrics without needing Kafka client libraries or knowledge of data warehouse topology.

---

## Stack & Technologies

- **Python 3.12+** — Telemetry simulation, bridge worker, and test suite
- **Eclipse Mosquitto** — Edge MQTT broker
- **Paho MQTT** — Lightweight Python MQTT client
- **Apache Kafka** — Distributed event stream broker
- **PySpark 3.5+ (Structured Streaming)** — Micro-batch stream processing engine
- **PostgreSQL 15** — Operational analytical data warehouse
- **Docker & Docker Compose** — Container orchestration
- **pytest** — Automated unit test suite
- **Kafka UI** — Visual topic & event stream monitoring

---

## Streaming Engineering Concepts

1. **Event Time vs Processing Time**: Pipeline operations evaluate sensor timestamps (`event_timestamp`) emitted by hardware rather than ingestion time (`processing_timestamp`).
2. **Watermarking**: Window operations use PySpark watermarking (`withWatermark("event_timestamp", "10 minutes")`) to handle out-of-order and late-arriving events.
3. **Idempotency & Deduplication**: Real-time message streaming ensures at-least-once delivery; duplicate events are eliminated using `event_id` keys.
4. **Data Quality & Quarantine (DLQ)**: Invalid payloads (out-of-bound measurements, missing identifiers) are isolated to `data/quarantine/` with `error_reason` flags for auditing.
5. **IoT Messaging**: MQTT simulates lightweight device-to-platform edge communication.
6. **QoS 1 and At-Least-Once Semantics**: MQTT QoS 1 provides at-least-once delivery semantics, creating duplicate event possibilities downstream that reinforce the necessity of `event_id` deduplication.
7. **Protocol Decoupling**: IoT sensors publish over MQTT without dependency on Kafka or Spark binaries.

---

## Data Quality Rules

| Metric | Validation Rule | Invalid Routing |
| :--- | :--- | :--- |
| `event_id` | Must be non-empty string | Quarantined (`event_id_null`) |
| `device_id` | Must be non-empty string | Quarantined (`device_id_null`) |
| `temperature` | `0.0 <= temperature <= 200.0` | Quarantined (`temperature_out_of_range`) |
| `pressure` | `pressure >= 0.0` | Quarantined (`pressure_negative`) |
| `vibration` | `vibration >= 0.0` | Quarantined (`vibration_negative`) |
| `rpm` | `rpm >= 0` | Quarantined (`rpm_negative`) |
| `status` | `RUNNING`, `STOPPED`, `MAINTENANCE`, `FAILURE` | Quarantined (`invalid_status`) |

---

## Anomaly Detection Rules

- **CRITICAL**: `temperature > 90.0` OR `vibration > 15.0` OR `pressure > 7.0`
- **WARNING**: `temperature > 75.0` OR `vibration > 6.0` OR `pressure > 5.0`
- **NORMAL**: All metrics within standard operating parameters.

---

## Execution Guide

Detailed execution instructions are documented in [docs/execution_guide.md](file:///c:/Users/elias.cruz/Documents/PYTHON_PROJECT/io_stream/docs/execution_guide.md).

### Quick Summary across Terminals:

1. **Infrastructure**: `docker compose up -d`
2. **IoT Simulator**: `python simulator/producer.py`
3. **MQTT Bridge**: `python bridge/mqtt_to_kafka.py`
4. **PySpark Engine**: `python streaming/refined.py`
5. **PostgreSQL Queries**: `docker exec -it iot_postgres psql -U iot_user -d iot_db -f /database/queries.sql`
6. **Unit Tests**: `pytest`

---

## Future Backlog

- [ ] Prometheus & Grafana operational dashboards
- [ ] Schema Registry & Avro serialization
- [ ] Automated Dead Letter Queue re-ingestion worker
- [ ] CI/CD GitHub Actions pipeline
- [ ] Predictive maintenance ML model integration
- [ ] MQTT authentication
- [ ] MQTT TLS encryption
- [ ] MQTT retained messages analysis
- [ ] MQTT topic partitioning strategy
- [ ] Kafka Connect MQTT connector alternative
