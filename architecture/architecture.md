# Architectural Reference & Data Flow

For full architectural documentation, please see [docs/architecture.md](file:///c:/Users/elias.cruz/Documents/PYTHON_PROJECT/io_stream/docs/architecture.md) and [docs/mqtt.md](file:///c:/Users/elias.cruz/Documents/PYTHON_PROJECT/io_stream/docs/mqtt.md).

```text
[Sensor Simulators] -> [Mosquitto MQTT] -> [MQTT-to-Kafka Bridge] -> [Kafka: iot-machine-events] -> [PySpark Streaming]
                                                                                                            │
                                                                   +----------------------------------------+----------------------------------------+
                                                                   |                                        |                                        |
                                                                   v                                        v                                        v
                                                            [Bronze Layer]                           [Quarantine DLQ]                         [Refined Layer]
                                                            (data/bronze)                            (data/quarantine)                        (PostgreSQL)
```
