-- Analytical Queries for Industrial IoT Refined Layer

-- Query 1: Which machines are currently in CRITICAL status?
SELECT device_id, last_event_timestamp, temperature, pressure, vibration, anomaly_level
FROM refined_machine_current_status
WHERE anomaly_level = 'CRITICAL'
ORDER BY last_event_timestamp DESC;

-- Query 2: Which machine has the highest temperature?
SELECT device_id, event_id, event_timestamp, temperature, anomaly_level
FROM refined_machine_measurements
ORDER BY temperature DESC
LIMIT 5;

-- Query 3: Which machine has the highest vibration?
SELECT device_id, event_id, event_timestamp, vibration, anomaly_level
FROM refined_machine_measurements
ORDER BY vibration DESC
LIMIT 5;

-- Query 4: How many total events were processed?
SELECT COUNT(*) AS total_processed_events
FROM refined_machine_measurements;

-- Query 5: Count of events by machine status (RUNNING, STOPPED, MAINTENANCE, FAILURE)
SELECT machine_status, COUNT(*) AS event_count
FROM refined_machine_measurements
GROUP BY machine_status
ORDER BY event_count DESC;

-- Query 6: Count of events by anomaly level (NORMAL, WARNING, CRITICAL)
SELECT anomaly_level, COUNT(*) AS event_count
FROM refined_machine_measurements
GROUP BY anomaly_level
ORDER BY event_count DESC;

-- Query 7: Average temperature, pressure, and vibration per machine
SELECT 
    device_id,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    ROUND(AVG(pressure)::numeric, 2) AS avg_pressure,
    ROUND(AVG(vibration)::numeric, 2) AS avg_vibration,
    COUNT(*) AS total_measurements
FROM refined_machine_measurements
GROUP BY device_id
ORDER BY device_id;

-- Query 8: Which machine presented the highest number of alerts?
SELECT 
    device_id,
    SUM(CASE WHEN temperature_alert THEN 1 ELSE 0 END) AS temp_alerts,
    SUM(CASE WHEN pressure_alert THEN 1 ELSE 0 END) AS press_alerts,
    SUM(CASE WHEN vibration_alert THEN 1 ELSE 0 END) AS vib_alerts,
    COUNT(*) FILTER (WHERE temperature_alert OR pressure_alert OR vibration_alert) AS total_alerts
FROM refined_machine_measurements
GROUP BY device_id
ORDER BY total_alerts DESC;

-- Query 9: Summary of anomaly distribution by device
SELECT 
    device_id,
    COUNT(*) FILTER (WHERE anomaly_level = 'NORMAL') AS normal_count,
    COUNT(*) FILTER (WHERE anomaly_level = 'WARNING') AS warning_count,
    COUNT(*) FILTER (WHERE anomaly_level = 'CRITICAL') AS critical_count
FROM refined_machine_measurements
GROUP BY device_id
ORDER BY device_id;
