import json
import logging
import os
import sys
import uuid
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
logger = logging.getLogger("AlertConsumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")
ALERT_GROUP_ID = "alert-consumer-group"


class AlertConsumer:
    """
    Consumer 1: Real-Time Critical Alert & Emergency Notification Consumer.
    
    Consumes Kafka streaming telemetry, detects high-risk threshold breaches
    (critical temperature, extreme vibration, pressure over-limit, machine failure),
    and dispatches immediate emergency notifications and audit logs.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = ALERT_GROUP_ID,
        output_dir: str = os.path.join("data", "alerts")
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_dir = output_dir
        self.alerts_history: List[Dict[str, Any]] = []
        os.makedirs(self.output_dir, exist_ok=True)

    def is_critical_event(self, event: Dict[str, Any]) -> bool:
        """Evaluates whether event qualifies for emergency alert."""
        temp = float(event.get("temperature", 0.0) or 0.0)
        press = float(event.get("pressure", 0.0) or 0.0)
        vib = float(event.get("vibration", 0.0) or 0.0)
        status = str(event.get("status", "")).upper()
        anomaly = str(event.get("anomaly_level", "")).upper()

        is_critical = (
            temp > 90.0
            or vib > 15.0
            or press > 7.0
            or status == "FAILURE"
            or anomaly == "CRITICAL"
        )
        return is_critical

    def create_alert_payload(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Formats structured alert notification record."""
        temp = float(event.get("temperature", 0.0) or 0.0)
        press = float(event.get("pressure", 0.0) or 0.0)
        vib = float(event.get("vibration", 0.0) or 0.0)
        status = str(event.get("status", "UNKNOWN"))

        reasons = []
        if temp > 90.0:
            reasons.append(f"Temperature CRITICAL ({temp}°C > 90.0°C)")
        if vib > 15.0:
            reasons.append(f"Vibration CRITICAL ({vib} mm/s > 15.0 mm/s)")
        if press > 7.0:
            reasons.append(f"Pressure CRITICAL ({press} bar > 7.0 bar)")
        if status == "FAILURE":
            reasons.append("Machine Status FAILURE")

        if not reasons:
            reasons.append("Anomaly Level CRITICAL Flagged")

        alert_record = {
            "alert_id": f"alt-{uuid.uuid4().hex[:8]}",
            "device_id": event.get("device_id", "UNKNOWN"),
            "event_id": event.get("event_id", "N/A"),
            "severity": "CRITICAL",
            "reasons": reasons,
            "metrics": {
                "temperature": temp,
                "pressure": press,
                "vibration": vib,
                "status": status
            },
            "event_timestamp": event.get("event_timestamp"),
            "alert_timestamp": datetime.now(timezone.utc).isoformat()
        }
        return alert_record

    def process_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Processes a single telemetry event dictionary."""
        if self.is_critical_event(event):
            alert = self.create_alert_payload(event)
            self.alerts_history.append(alert)
            
            logger.warning(
                f"🚨 [EMERGENCY ALERT] Device {alert['device_id']} | "
                f"Event {alert['event_id']} | Reasons: {', '.join(alert['reasons'])}"
            )
            self._save_alert_to_disk(alert)
            return alert
        return None

    def _save_alert_to_disk(self, alert: Dict[str, Any]):
        """Persists alert record into local alerts log file."""
        log_file = os.path.join(self.output_dir, "alerts.jsonl")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert) + "\n")
        except Exception as ex:
            logger.error(f"Failed to write alert to log file: {ex}")

    def start(self, poll_timeout_ms: int = 1000, max_messages: Optional[int] = None):
        """Starts Kafka consumer loop for Alert Consumer."""
        from kafka import KafkaConsumer
        logger.info(f"Starting AlertConsumer listening to topic '{self.topic}' (group: '{self.group_id}')")

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
            logger.info("AlertConsumer stopped by user.")
        finally:
            consumer.close()


if __name__ == "__main__":
    consumer = AlertConsumer()
    consumer.start()
