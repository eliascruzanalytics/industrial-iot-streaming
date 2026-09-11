# Data Dictionary — Industrial IoT Streaming Pipeline

## 1. Bronze Layer (`data/bronze/`)
Stores raw streaming JSON events ingested directly from Kafka.

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `event_id` | STRING | Unique event UUID string |
| `device_id` | STRING | Unique identifier of industrial machine (e.g. `MTR-001`) |
| `event_timestamp` | STRING | ISO 8601 UTC timestamp emitted by sensor |
| `temperature` | DOUBLE | Engine temperature reading in °C |
| `pressure` | DOUBLE | Operating pressure reading in bar |
| `vibration` | DOUBLE | Vibration level reading in mm/s |
| `rpm` | INTEGER | Rotations per minute |
| `status` | STRING | Machine operating state (`RUNNING`, `STOPPED`, `MAINTENANCE`, `FAILURE`) |
| `machine_type` | STRING | Classification of industrial equipment (e.g. `Motor`) |
| `location` | STRING | Factory zone location |
| `kafka_timestamp` | TIMESTAMP | Kafka broker append timestamp |
| `ingestion_timestamp` | TIMESTAMP | Spark pipeline local ingestion timestamp |

---

## 2. Quarantine / DLQ Layer (`data/quarantine/`)
Stores malformed, out-of-range, or corrupted events rejected by Data Quality rules.

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `event_id` | STRING | Event ID if present, otherwise null |
| `device_id` | STRING | Device ID if present, otherwise null |
| `error_reason` | STRING | Comma-separated list of failed validation rules |
| `original_payload` | STRING | Complete JSON string payload as received from Kafka |
| `quarantine_timestamp` | TIMESTAMP | Timestamp record was routed to quarantine |

---

## 3. Refined Layer (`refined_machine_measurements`)
Clean, deduplicated, enriched data model stored in PostgreSQL and local Parquet.

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `event_id` | STRING (PK) | Unique event primary key |
| `device_id` | STRING | Machine identifier |
| `event_timestamp` | TIMESTAMP | Event timestamp |
| `ingestion_timestamp` | TIMESTAMP | Ingestion timestamp |
| `temperature` | DOUBLE | Sensor temperature (°C) |
| `pressure` | DOUBLE | Sensor pressure (bar) |
| `vibration` | DOUBLE | Sensor vibration (mm/s) |
| `rpm` | INTEGER | Engine RPM |
| `machine_status` | STRING | Operating status |
| `anomaly_level` | STRING | Evaluated severity (`NORMAL`, `WARNING`, `CRITICAL`) |
| `temperature_alert` | BOOLEAN | `True` if temperature exceeds warning threshold |
| `pressure_alert` | BOOLEAN | `True` if pressure exceeds warning threshold |
| `vibration_alert` | BOOLEAN | `True` if vibration exceeds warning threshold |
| `processing_timestamp` | TIMESTAMP | Time of PySpark refined processing |
