import json
import logging
import os
import sys
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
logger = logging.getLogger("ArchiveConsumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot-machine-events")
ARCHIVE_GROUP_ID = "archiver-consumer-group"


class ArchiveConsumer:
    """
    Consumer 3: Data Lake Bronze/Raw Archiver Consumer.
    
    Consumes streaming raw telemetry payloads from Kafka and persists them
    into a partitioned Bronze Data Lake storage (`data/bronze/year=YYYY/month=MM/day=DD/`).
    Ensures long-term data retention, raw auditing, and batch reprocessing capability.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = ARCHIVE_GROUP_ID,
        base_dir: str = os.path.join("data", "bronze")
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.base_dir = base_dir
        self.archived_count = 0
        os.makedirs(self.base_dir, exist_ok=True)

    def get_partition_path(self, event_ts: Optional[str] = None) -> str:
        """Determines target directory path based on date partitioning."""
        dt = datetime.now(timezone.utc)
        if event_ts:
            try:
                dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
            except Exception:
                pass

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        partition_dir = os.path.join(
            self.base_dir,
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        os.makedirs(partition_dir, exist_ok=True)
        return partition_dir

    def process_event(self, event: Dict[str, Any]) -> str:
        """Archives a raw telemetry event into the target partitioned file."""
        event_ts = event.get("event_timestamp")
        partition_dir = self.get_partition_path(event_ts)
        file_path = os.path.join(partition_dir, "raw_telemetry.jsonl")

        enriched_archive_record = {
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
            "_source_topic": self.topic,
            "raw_payload": event
        }

        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(enriched_archive_record) + "\n")
            self.archived_count += 1
            logger.info(f"📁 [ARCHIVE] Archived event {event.get('event_id', 'N/A')} -> {file_path}")
            return file_path
        except Exception as ex:
            logger.error(f"Failed to archive event to disk: {ex}")
            raise

    def start(self, max_messages: Optional[int] = None):
        """Starts Kafka consumer loop for Archiver Consumer."""
        from kafka import KafkaConsumer
        logger.info(f"Starting ArchiveConsumer listening to topic '{self.topic}' (group: '{self.group_id}')")

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
            logger.info("ArchiveConsumer stopped by user.")
        finally:
            consumer.close()


if __name__ == "__main__":
    consumer = ArchiveConsumer()
    consumer.start()
