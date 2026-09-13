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
logger = logging.getLogger("MaintenanceConsumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")
MAINTENANCE_GROUP_ID = "maintenance-consumer-group"


class MaintenanceConsumer:
    """
    Consumer 5: Equipment Maintenance & Lifecycle Consumer.
    
    Tracks industrial hardware operating cycles, calculates cumulative mechanical stress
    (vibration fatigue, high-temperature operating cycles, continuous running hours),
    and emits predictive maintenance recommendations before hardware breakdown occurs.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = MAINTENANCE_GROUP_ID,
        output_dir: str = os.path.join("data", "maintenance"),
        max_accumulated_vibration: float = 100.0,
        max_high_temp_cycles: int = 20
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_dir = output_dir
        self.max_accumulated_vibration = max_accumulated_vibration
        self.max_high_temp_cycles = max_high_temp_cycles

        self.equipment_state: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_cycles": 0,
            "running_cycles": 0,
            "accumulated_vibration": 0.0,
            "high_temp_cycles": 0,
            "high_pressure_cycles": 0,
            "last_status": None,
            "maintenance_required": False,
            "maintenance_reasons": [],
            "health_score": 100.0
        })
        self.work_orders: List[Dict[str, Any]] = []
        os.makedirs(self.output_dir, exist_ok=True)

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluates machine lifecycle metrics and updates maintenance state."""
        device_id = event.get("device_id", "UNKNOWN")
        temp = float(event.get("temperature", 0.0) or 0.0)
        press = float(event.get("pressure", 0.0) or 0.0)
        vib = float(event.get("vibration", 0.0) or 0.0)
        status = str(event.get("status", "UNKNOWN")).upper()
        event_id = event.get("event_id", "N/A")

        state = self.equipment_state[device_id]
        state["total_cycles"] += 1

        if status == "RUNNING":
            state["running_cycles"] += 1

        state["accumulated_vibration"] = round(state["accumulated_vibration"] + vib, 2)

        if temp > 75.0:
            state["high_temp_cycles"] += 1
        if press > 5.0:
            state["high_pressure_cycles"] += 1

        # Calculate dynamic health score (100% -> 0%)
        vib_penalty = min(50.0, (state["accumulated_vibration"] / self.max_accumulated_vibration) * 50.0)
        temp_penalty = min(50.0, (state["high_temp_cycles"] / self.max_high_temp_cycles) * 50.0)
        health_score = max(0.0, round(100.0 - vib_penalty - temp_penalty, 1))
        state["health_score"] = health_score

        # Check maintenance trigger criteria
        reasons = []
        if state["accumulated_vibration"] >= self.max_accumulated_vibration:
            reasons.append(f"Excessive Accumulated Vibration ({state['accumulated_vibration']} >= {self.max_accumulated_vibration})")
        if state["high_temp_cycles"] >= self.max_high_temp_cycles:
            reasons.append(f"High Temperature Operating Limit ({state['high_temp_cycles']} >= {self.max_high_temp_cycles} cycles)")
        if status == "FAILURE":
            reasons.append("Catastrophic Machine Failure Detected")

        if reasons and not state["maintenance_required"]:
            state["maintenance_required"] = True
            state["maintenance_reasons"] = reasons
            
            work_order = {
                "work_order_id": f"WO-{device_id}-{state['total_cycles']}",
                "device_id": device_id,
                "trigger_event_id": event_id,
                "priority": "HIGH" if status == "FAILURE" else "MEDIUM",
                "health_score": health_score,
                "reasons": reasons,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            self.work_orders.append(work_order)
            logger.warning(
                f"🔧 [MAINTENANCE WORK ORDER GENERATED] Device {device_id} | "
                f"Health: {health_score}% | Reasons: {', '.join(reasons)}"
            )

        state["last_status"] = status
        logger.info(
            f"⚙️ [MAINTENANCE] {device_id} | Health Score: {health_score}% | "
            f"Acc. Vibration: {state['accumulated_vibration']} mm/s | Work Orders: {len(self.work_orders)}"
        )

        self._save_state_to_disk()
        return {
            "device_id": device_id,
            "health_score": health_score,
            "maintenance_required": state["maintenance_required"],
            "reasons": state["maintenance_reasons"]
        }

    def get_lifecycle_summary(self) -> Dict[str, Any]:
        """Returns fleet-wide equipment lifecycle summary."""
        return {
            "monitored_devices_count": len(self.equipment_state),
            "open_work_orders_count": len(self.work_orders),
            "equipment_states": dict(self.equipment_state),
            "work_orders": self.work_orders,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

    def _save_state_to_disk(self):
        """Persists equipment lifecycle state to disk."""
        out_file = os.path.join(self.output_dir, "equipment_lifecycle.json")
        try:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(self.get_lifecycle_summary(), f, indent=2)
        except Exception as ex:
            logger.error(f"Failed to write maintenance state to disk: {ex}")

    def start(self, max_messages: Optional[int] = None):
        """Starts Kafka consumer loop for Maintenance Consumer."""
        from kafka import KafkaConsumer
        logger.info(f"Starting MaintenanceConsumer listening to topic '{self.topic}' (group: '{self.group_id}')")

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
            logger.info("MaintenanceConsumer stopped by user.")
        finally:
            consumer.close()


if __name__ == "__main__":
    consumer = MaintenanceConsumer()
    consumer.start()
