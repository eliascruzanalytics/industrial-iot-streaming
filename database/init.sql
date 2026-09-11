-- Database initialization script for PostgreSQL

CREATE TABLE IF NOT EXISTS refined_machine_measurements (
    event_id VARCHAR(100) PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    rpm INTEGER NOT NULL,
    machine_status VARCHAR(20) NOT NULL,
    anomaly_level VARCHAR(20) NOT NULL,
    temperature_alert BOOLEAN NOT NULL,
    pressure_alert BOOLEAN NOT NULL,
    vibration_alert BOOLEAN NOT NULL,
    processing_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_measurements_device_id ON refined_machine_measurements(device_id);
CREATE INDEX IF NOT EXISTS idx_measurements_event_timestamp ON refined_machine_measurements(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_measurements_anomaly_level ON refined_machine_measurements(anomaly_level);

CREATE TABLE IF NOT EXISTS refined_machine_current_status (
    device_id VARCHAR(50) PRIMARY KEY,
    last_event_id VARCHAR(100) NOT NULL,
    last_event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    rpm INTEGER NOT NULL,
    machine_status VARCHAR(20) NOT NULL,
    anomaly_level VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
