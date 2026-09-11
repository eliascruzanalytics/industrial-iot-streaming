# System Architecture — Industrial IoT Streaming Data Pipeline

## Overview

This repository implements an end-to-end near real-time Industrial IoT streaming pipeline. It simulates 5 industrial motors (`MTR-001` to `MTR-005`) producing continuous sensor telemetry, publishes events over **MQTT (QoS 1)** to **Eclipse Mosquitto**, bridges messages to **Apache Kafka**, processes events with **PySpark Structured Streaming**, segregates raw data into **Bronze (Parquet)** and invalid data into **Quarantine (DLQ)**, applies **Data Quality**, **Watermarking**, and **Deduplication**, enriches data with **Anomaly Classification**, and persists state to **PostgreSQL**.

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
```

## Architectural Design Decisions

1. **Protocol Decoupling (MQTT + Kafka)**: Decouples edge IoT telemetry devices (Mosquitto) from the analytical streaming engine (Kafka + PySpark). IoT devices emit metrics without needing Kafka libraries.
2. **QoS 1 & At-Least-Once Delivery**: Demonstrates real-world edge networking semantics where network re-transmissions may introduce duplicates, resolved downstream by PySpark deduplication on `event_id`.
3. **Explicit Schema Enforcement**: Prevents schema drift and runtime type assertion errors during JSON parsing from Kafka stream.
4. **Quarantine / DLQ Routing**: Events violating business validation rules (negative pressure, missing identifiers, out-of-range temperatures) are preserved in `data/quarantine/` for audit and manual investigation rather than silently dropped.
5. **Event-Time Processing & Watermarking**: Window operations and late-arriving event bounds are strictly bound to `event_timestamp` rather than ingestion time.
