import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any


class SensorSimulator:
    """Simulates realistic industrial sensor telemetry state and event generation."""

    def __init__(self, device_id: str, machine_type: str = "Motor", location: str = "Factory_Floor_A"):
        self.device_id = device_id
        self.machine_type = machine_type
        self.location = location
        self.state = "NORMAL"
        self.sequence_counter = 0

    def step(self) -> Dict[str, Any]:
        """Advances state probabilistic transition and generates next telemetry event payload."""
        self.sequence_counter += 1
        
        # State transition logic
        roll = random.random()
        if self.state == "NORMAL":
            if roll < 0.15:
                self.state = "WARNING"
        elif self.state == "WARNING":
            if roll < 0.10:
                self.state = "CRITICAL"
            elif roll > 0.85:
                self.state = "NORMAL"
        elif self.state == "CRITICAL":
            if roll > 0.70:
                self.state = "WARNING"

        # Generate metrics based on state
        if self.state == "NORMAL":
            temperature = round(random.uniform(50.0, 74.9), 2)
            pressure = round(random.uniform(3.0, 4.9), 2)
            vibration = round(random.uniform(1.0, 5.9), 2)
            rpm = random.randint(1400, 1800)
            status = "RUNNING"
        elif self.state == "WARNING":
            temperature = round(random.uniform(75.0, 89.9), 2)
            pressure = round(random.uniform(5.0, 6.9), 2)
            vibration = round(random.uniform(6.0, 14.9), 2)
            rpm = random.randint(1200, 1900)
            status = "RUNNING" if random.random() > 0.2 else "MAINTENANCE"
        else:  # CRITICAL
            temperature = round(random.uniform(90.1, 105.0), 2)
            pressure = round(random.uniform(7.1, 9.5), 2)
            vibration = round(random.uniform(15.1, 25.0), 2)
            rpm = random.randint(1000, 2100)
            status = "FAILURE" if random.random() > 0.4 else "RUNNING"

        event = {
            "event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "device_id": self.device_id,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": temperature,
            "pressure": pressure,
            "vibration": vibration,
            "rpm": rpm,
            "status": status,
            "machine_type": self.machine_type,
            "location": self.location,
        }
        return event
