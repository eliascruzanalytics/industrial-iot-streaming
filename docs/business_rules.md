# Business Rules & Data Quality Specification

## Data Quality Rules (Quarantine Filtering)

A incoming event is evaluated against mandatory quality constraints. If ANY constraint fails, the event is marked as `INVALID` and routed to `data/quarantine/`.

| Field | Rule Constraint | Error Reason |
| :--- | :--- | :--- |
| `event_id` | `NOT NULL AND != ''` | `event_id_null` |
| `device_id` | `NOT NULL AND != ''` | `device_id_null` |
| `event_timestamp` | `NOT NULL` | `event_timestamp_null` |
| `temperature` | `NOT NULL AND >= 0.0 AND <= 200.0` | `temperature_out_of_range` |
| `pressure` | `NOT NULL AND >= 0.0` | `pressure_negative` |
| `vibration` | `NOT NULL AND >= 0.0` | `vibration_negative` |
| `rpm` | `NOT NULL AND >= 0` | `rpm_negative` |
| `status` | `IN ('RUNNING', 'STOPPED', 'MAINTENANCE', 'FAILURE')` | `invalid_status` |

---

## Anomaly Level Classification

Events passing quality checks are categorized into hierarchy levels:

### 1. CRITICAL
Triggered if ANY of the following conditions are met:
- `temperature > 90.0` °C
- `vibration > 15.0` mm/s
- `pressure > 7.0` bar

### 2. WARNING
Triggered if NOT CRITICAL and ANY of the following conditions are met:
- `temperature > 75.0` °C
- `vibration > 6.0` mm/s
- `pressure > 5.0` bar

### 3. NORMAL
Default status if no threshold limits are breached.
