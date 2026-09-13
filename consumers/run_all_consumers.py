import argparse
import logging
import os
import sys
import time
from threading import Thread
from typing import List, Dict, Any

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from consumers.alert_consumer import AlertConsumer
from consumers.metrics_consumer import MetricsConsumer
from consumers.archive_consumer import ArchiveConsumer
from consumers.dlq_consumer import DlqConsumer
from consumers.maintenance_consumer import MaintenanceConsumer

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
logger = logging.getLogger("ConsumerRunner")

SAMPLE_EVENTS = [
    {
        "event_id": "evt-test-001",
        "device_id": "MTR-001",
        "event_timestamp": "2026-09-13T18:00:00Z",
        "temperature": 95.5,
        "pressure": 5.2,
        "vibration": 16.2,
        "rpm": 1800,
        "status": "RUNNING",
        "anomaly_level": "CRITICAL"
    },
    {
        "event_id": "evt-test-002",
        "device_id": "MTR-002",
        "event_timestamp": "2026-09-13T18:01:00Z",
        "temperature": 65.0,
        "pressure": 4.1,
        "vibration": 3.2,
        "rpm": 1750,
        "status": "RUNNING",
        "anomaly_level": "NORMAL"
    },
    {
        "event_id": "evt-test-003",
        "device_id": "MTR-003",
        "event_timestamp": "2026-09-13T18:02:00Z",
        "temperature": 110.0,
        "pressure": 8.5,
        "vibration": 22.0,
        "rpm": 0,
        "status": "FAILURE",
        "anomaly_level": "CRITICAL"
    }
]


def run_sample_demo(consumer_name: str):
    """Executes sample event processing for local demonstration/validation."""
    logger.info(f"🚀 Running sample demonstration for consumer '{consumer_name}'...")
    
    if consumer_name in ("alert", "all"):
        alert_cons = AlertConsumer()
        logger.info("--- [1/5] Testing AlertConsumer ---")
        for evt in SAMPLE_EVENTS:
            alert_cons.process_event(evt)
            
    if consumer_name in ("metrics", "all"):
        metrics_cons = MetricsConsumer()
        logger.info("--- [2/5] Testing MetricsConsumer ---")
        for evt in SAMPLE_EVENTS:
            metrics_cons.process_event(evt)

    if consumer_name in ("archive", "all"):
        archive_cons = ArchiveConsumer()
        logger.info("--- [3/5] Testing ArchiveConsumer ---")
        for evt in SAMPLE_EVENTS:
            archive_cons.process_event(evt)

    if consumer_name in ("dlq", "all"):
        dlq_cons = DlqConsumer()
        logger.info("--- [4/5] Testing DlqConsumer ---")
        dlq_sample = {
            "event_id": "evt-quarantine-001",
            "device_id": "MTR-099",
            "error_reason": "temperature_out_of_range",
            "temperature": 250.0
        }
        dlq_cons.process_quarantine_record(dlq_sample)

    if consumer_name in ("maintenance", "all"):
        maint_cons = MaintenanceConsumer()
        logger.info("--- [5/5] Testing MaintenanceConsumer ---")
        for evt in SAMPLE_EVENTS:
            maint_cons.process_event(evt)

    logger.info("✅ Sample demonstration complete! Check files generated under data/ directory.")


def run_kafka_consumers(consumer_name: str):
    """Launches active Kafka consumer loops."""
    threads: List[Thread] = []

    def start_consumer_thread(target_cls, name):
        c = target_cls()
        logger.info(f"Starting background worker thread for {name}...")
        c.start()

    consumers_map = {
        "alert": (AlertConsumer, "AlertConsumer"),
        "metrics": (MetricsConsumer, "MetricsConsumer"),
        "archive": (ArchiveConsumer, "ArchiveConsumer"),
        "dlq": (DlqConsumer, "DlqConsumer"),
        "maintenance": (MaintenanceConsumer, "MaintenanceConsumer")
    }

    if consumer_name == "all":
        for name, (cls_obj, label) in consumers_map.items():
            t = Thread(target=start_consumer_thread, args=(cls_obj, label), daemon=True)
            threads.append(t)
            t.start()

        logger.info(f"⚡ All 5 consumers running concurrently across {len(threads)} background threads.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down all consumers.")
    else:
        if consumer_name in consumers_map:
            cls_obj, label = consumers_map[consumer_name]
            c = cls_obj()
            c.start()
        else:
            logger.error(f"Unknown consumer name '{consumer_name}'. Choose from: {list(consumers_map.keys())} or 'all'")


def main():
    parser = argparse.ArgumentParser(description="Industrial IoT Multi-Consumer Runner")
    parser.add_argument(
        "--consumer",
        type=str,
        default="all",
        choices=["alert", "metrics", "archive", "dlq", "maintenance", "all"],
        help="Specify which consumer to run (default: all)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run sample event processing once without connecting to Kafka broker (for offline test/demo)"
    )
    args = parser.parse_args()

    if args.once:
        run_sample_demo(args.consumer)
    else:
        run_kafka_consumers(args.consumer)


if __name__ == "__main__":
    main()
