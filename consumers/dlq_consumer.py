import json
import logging
import glob
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
logger = logging.getLogger("DlqConsumer")


class DlqConsumer:
    """
    Consumer 4: Data Quality & Quarantine (DLQ) Monitor Consumer.
    
    Monitors, parses, and analyzes invalid streaming events routed to Quarantine/DLQ.
    Calculates error reason distributions, tracks error counts per device, and computes
    data quality metrics for operational reliability monitoring.
    """

    def __init__(
        self,
        quarantine_dir: str = os.path.join("data", "quarantine"),
        error_threshold_alert: int = 10
    ):
        self.quarantine_dir = quarantine_dir
        self.error_threshold_alert = error_threshold_alert
        self.error_counts_by_reason: Dict[str, int] = defaultdict(int)
        self.error_counts_by_device: Dict[str, int] = defaultdict(int)
        self.total_quarantined = 0
        self.quarantine_history: List[Dict[str, Any]] = []
        os.makedirs(self.quarantine_dir, exist_ok=True)

    def process_quarantine_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Processes a single quarantined payload record."""
        device_id = str(record.get("device_id") or "UNKNOWN_DEVICE")
        error_reason = str(record.get("error_reason") or "unspecified_error")
        event_id = record.get("event_id", "N/A")

        self.error_counts_by_reason[error_reason] += 1
        self.error_counts_by_device[device_id] += 1
        self.total_quarantined += 1

        processed_entry = {
            "quarantine_id": f"dlq-{self.total_quarantined}",
            "event_id": event_id,
            "device_id": device_id,
            "error_reason": error_reason,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        self.quarantine_history.append(processed_entry)

        logger.warning(
            f"⚠️ [DLQ MONITOR] Quarantined Event {event_id} | Device: {device_id} | Reason: {error_reason}"
        )

        if self.error_counts_by_reason[error_reason] >= self.error_threshold_alert:
            logger.error(
                f"🚨 [DQ ALERT THRESHOLD EXCEEDED] Reason '{error_reason}' reached {self.error_counts_by_reason[error_reason]} occurrences!"
            )

        self._save_summary_to_disk()
        return processed_entry

    def scan_quarantine_directory(self) -> int:
        """Scans local quarantine storage directory for unparsed Parquet/JSON logs."""
        count = 0
        json_files = glob.glob(os.path.join(self.quarantine_dir, "*.json*")) + \
                     glob.glob(os.path.join(self.quarantine_dir, "**", "*.json*"), recursive=True)

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            record = json.loads(line)
                            self.process_quarantine_record(record)
                            count += 1
            except Exception as ex:
                logger.error(f"Error reading quarantine file {file_path}: {ex}")
        return count

    def get_dlq_summary(self) -> Dict[str, Any]:
        """Returns aggregated quarantine metrics summary."""
        return {
            "total_quarantined_events": self.total_quarantined,
            "errors_by_reason": dict(self.error_counts_by_reason),
            "errors_by_device": dict(self.error_counts_by_device),
            "last_processed_at": datetime.now(timezone.utc).isoformat()
        }

    def _save_summary_to_disk(self):
        """Persists DLQ metrics summary to file."""
        out_file = os.path.join(self.quarantine_dir, "dlq_summary.json")
        try:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(self.get_dlq_summary(), f, indent=2)
        except Exception as ex:
            logger.error(f"Failed to write DLQ summary to disk: {ex}")

    def start(self):
        """Runs quarantine directory scan monitor."""
        logger.info(f"Starting DlqConsumer scanning directory '{self.quarantine_dir}'")
        found = self.scan_quarantine_directory()
        logger.info(f"DlqConsumer scan complete. Total quarantined events evaluated: {self.total_quarantined}")


if __name__ == "__main__":
    consumer = DlqConsumer()
    consumer.start()
