import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MetricsConsumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")
METRICS_GROUP_ID = "metrics-consumer-group"


class MetricsConsumer:
    """
    Consumer 2: Real-Time Operational Metrics & Running Window Aggregator.
    
    Consumes streaming events and computes real-time rolling statistics
    per industrial device (moving average temperature, pressure, vibration,
    min/max ranges, message counts, operating status breakdown).
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = METRICS_GROUP_ID,
        output_dir: str = os.path.join("data", "metrics")
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_dir = output_dir
        self.device_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "count": 0,
            "temp_sum": 0.0,
            "temp_min": float("inf"),
            "temp_max": float("-inf"),
            "pressure_sum": 0.0,
            "pressure_min": float("inf"),
            "pressure_max": float("-inf"),
            "vibration_sum": 0.0,
            "vibration_min": float("inf"),
            "vibration_max": float("-inf"),
            "statuses": defaultdict(int),
            "last_event_timestamp": None
        })
        self.total_processed = 0
        os.makedirs(self.output_dir, exist_ok=True)

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Updates device metrics state with incoming telemetry event."""
        device_id = event.get("device_id", "UNKNOWN")
        temp = float(event.get("temperature", 0.0) or 0.0)
        press = float(event.get("pressure", 0.0) or 0.0)
        vib = float(event.get("vibration", 0.0) or 0.0)
        status = str(event.get("status", "UNKNOWN")).upper()
        ts = event.get("event_timestamp")

        stats = self.device_metrics[device_id]
        stats["count"] += 1
        stats["temp_sum"] += temp
        stats["temp_min"] = min(stats["temp_min"], temp)
        stats["temp_max"] = max(stats["temp_max"], temp)

        stats["pressure_sum"] += press
        stats["pressure_min"] = min(stats["pressure_min"], press)
        stats["pressure_max"] = max(stats["pressure_max"], press)

        stats["vibration_sum"] += vib
        stats["vibration_min"] = min(stats["vibration_min"], vib)
        stats["vibration_max"] = max(stats["vibration_max"], vib)

        stats["statuses"][status] += 1
        stats["last_event_timestamp"] = ts

        self.total_processed += 1

        summary = self.get_device_summary(device_id)
        logger.info(
            f"📊 [METRICS] {device_id} | Total Events: {summary['total_events']} | "
            f"Avg Temp: {summary['avg_temperature']}°C | Avg Vib: {summary['avg_vibration']} mm/s"
        )
        self._save_summary_to_disk()
        return summary

    def get_device_summary(self, device_id: str) -> Dict[str, Any]:
        """Calculates current running average and metric snapshot for a device."""
        stats = self.device_metrics[device_id]
        count = stats["count"]
        if count == 0:
            return {"device_id": device_id, "total_events": 0}

        return {
            "device_id": device_id,
            "total_events": count,
            "avg_temperature": round(stats["temp_sum"] / count, 2),
            "min_temperature": round(stats["temp_min"], 2),
            "max_temperature": round(stats["temp_max"], 2),
            "avg_pressure": round(stats["pressure_sum"] / count, 2),
            "min_pressure": round(stats["pressure_min"], 2),
            "max_pressure": round(stats["pressure_max"], 2),
            "avg_vibration": round(stats["vibration_sum"] / count, 2),
            "min_vibration": round(stats["vibration_min"], 2),
            "max_vibration": round(stats["vibration_max"], 2),
            "status_distribution": dict(stats["statuses"]),
            "last_event_timestamp": stats["last_event_timestamp"],
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

    def get_all_summaries(self) -> Dict[str, Any]:
        """Returns overall fleet operational summary."""
        summaries = {}
        for dev_id in self.device_metrics:
            summaries[dev_id] = self.get_device_summary(dev_id)
        return {
            "total_processed_events": self.total_processed,
            "total_active_devices": len(self.device_metrics),
            "devices": summaries,
            "generated_at": datetime.now(timezone.utc).isoformat()
        }

    def _save_summary_to_disk(self):
        """Persists current aggregated state to disk for dashboard consumption."""
        out_file = os.path.join(self.output_dir, "operational_metrics.json")
        try:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(self.get_all_summaries(), f, indent=2)
        except Exception as ex:
            logger.error(f"Failed to write metrics summary to disk: {ex}")

    def start(self, max_messages: Optional[int] = None):
        """Starts Kafka consumer loop for Metrics Consumer."""
        from kafka import KafkaConsumer
        logger.info(f"Starting MetricsConsumer listening to topic '{self.topic}' (group: '{self.group_id}')")

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            group_id=self.group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True
        )

        msg_count = 0
        try:
            for message in consumer:
                event = message.value
                if isinstance(event, dict):
                    self.process_event(event)
                msg_count += 1
                if max_messages and msg_count >= max_messages:
                    break
        except KeyboardInterrupt:
            logger.info("MetricsConsumer stopped by user.")
        finally:
            consumer.close()


if __name__ == "__main__":
    consumer = MetricsConsumer()
    consumer.start()
