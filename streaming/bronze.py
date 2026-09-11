import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Windows PySpark HADOOP_HOME setup to prevent FileNotFoundException
if sys.platform.startswith("win"):
    hadoop_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".hadoop"))
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    winutils_file = os.path.join(bin_dir, "winutils.exe")
    if not os.path.exists(winutils_file):
        open(winutils_file, "a").close()
    os.environ["HADOOP_HOME"] = hadoop_dir

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from streaming.schemas import IOT_EVENT_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BronzePipeline")


def create_spark_session(app_name: str = "Industrial_IoT_Bronze") -> SparkSession:
    """Creates PySpark session with Kafka streaming support."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master("local[*]") \
        .getOrCreate()


def process_bronze_stream(bootstrap_servers: str = "localhost:9092", topic: str = "iot-machine-events", output_path: str = "data/bronze"):
    """Reads raw JSON stream from Kafka, adds ingestion timestamp, and saves to Bronze Parquet."""
    spark = create_spark_session()
    logger.info(f"Starting Bronze stream reading from Kafka topic '{topic}' at {bootstrap_servers}")

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload and add metadata
    bronze_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp") \
        .select(
            F.from_json(F.col("json_payload"), IOT_EVENT_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("ingestion_timestamp", F.current_timestamp())

    checkpoint_dir = os.path.join("checkpoint", "bronze")
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    logger.info(f"Writing Bronze stream to Parquet path: {output_path}")

    query = bronze_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    return query


if __name__ == "__main__":
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "iot-machine-events")
    streaming_query = process_bronze_stream(bootstrap_servers=kafka_host, topic=kafka_topic)
    streaming_query.awaitTermination()
