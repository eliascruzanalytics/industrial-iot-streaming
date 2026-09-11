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
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from streaming.schemas import IOT_EVENT_SCHEMA
from streaming.quality import apply_data_quality_spark


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RefinedPipeline")


def calculate_anomaly_python(temperature: float, pressure: float, vibration: float) -> str:
    """
    Pure Python function for evaluating anomaly level.
    Used by pytest unit tests and business logic evaluation.
    
    Priority: CRITICAL > WARNING > NORMAL
    """
    if temperature > 90.0 or vibration > 15.0 or pressure > 7.0:
        return "CRITICAL"
    elif temperature > 75.0 or vibration > 6.0 or pressure > 5.0:
        return "WARNING"
    else:
        return "NORMAL"


def enrich_refined_spark(df: DataFrame) -> DataFrame:
    """
    Enriches valid PySpark DataFrame with alert flags, anomaly level, and timestamps.
    """
    # Cast timestamp column if string
    df_ts = df.withColumn("event_timestamp_ts", F.to_timestamp(F.col("event_timestamp")))
    
    # Calculate alert boolean flags
    temp_alert = F.col("temperature") > 75.0
    press_alert = F.col("pressure") > 5.0
    vib_alert = F.col("vibration") > 6.0
    
    # Calculate anomaly level with hierarchy
    critical_cond = (F.col("temperature") > 90.0) | (F.col("vibration") > 15.0) | (F.col("pressure") > 7.0)
    warning_cond = (F.col("temperature") > 75.0) | (F.col("vibration") > 6.0) | (F.col("pressure") > 5.0)
    
    anomaly_expr = F.when(critical_cond, F.lit("CRITICAL")) \
                    .when(warning_cond, F.lit("WARNING")) \
                    .otherwise(F.lit("NORMAL"))

    return df_ts \
        .withColumn("temperature_alert", temp_alert) \
        .withColumn("pressure_alert", press_alert) \
        .withColumn("vibration_alert", vib_alert) \
        .withColumn("anomaly_level", anomaly_expr) \
        .withColumn("machine_status", F.col("status")) \
        .withColumn("event_timestamp", F.col("event_timestamp_ts")) \
        .withColumn("processing_timestamp", F.current_timestamp()) \
        .drop("event_timestamp_ts")


def process_end_to_end_batch(df: DataFrame, epoch_id: int):
    """
    Micro-batch execution logic:
    1. Apply Data Quality
    2. Route invalid events to Quarantine Parquet
    3. Route valid events to Deduplication + Refined Enrichment
    4. Persist Refined events to PostgreSQL
    """
    if df.isEmpty():
        logger.info(f"Batch {epoch_id} is empty. Skipping.")
        return

    logger.info(f"Processing micro-batch {epoch_id} with {df.count()} raw records.")

    # 1. Apply Data Quality
    dq_df = apply_data_quality_spark(df)
    
    # 2. Route invalid events to Quarantine
    invalid_df = dq_df.filter(~F.col("is_valid"))
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        logger.warning(f"Quarantining {invalid_count} invalid events in batch {epoch_id}.")
        quarantine_dir = os.path.join("data", "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        
        quarantine_payload = invalid_df.select(
            F.col("event_id"),
            F.col("device_id"),
            F.col("error_reason"),
            F.to_json(F.struct("*")).alias("original_payload"),
            F.current_timestamp().alias("quarantine_timestamp")
        )
        quarantine_payload.write.mode("append").parquet(quarantine_dir)

    # 3. Process valid events
    valid_df = dq_df.filter(F.col("is_valid"))
    valid_count = valid_df.count()
    if valid_count > 0:
        logger.info(f"Processing {valid_count} valid events in batch {epoch_id}.")
        
        # Deduplication on event_id
        dedup_df = valid_df.dropDuplicates(["event_id"])
        
        # Enrichment
        refined_df = enrich_refined_spark(dedup_df)
        
        # Select target schema columns
        target_columns = [
            "event_id", "device_id", "event_timestamp", "ingestion_timestamp",
            "temperature", "pressure", "vibration", "rpm",
            "machine_status", "anomaly_level",
            "temperature_alert", "pressure_alert", "vibration_alert",
            "processing_timestamp"
        ]
        final_refined_df = refined_df.select(target_columns)

        # Write to local Refined Parquet
        refined_path = os.path.join("data", "refined")
        os.makedirs(refined_path, exist_ok=True)
        final_refined_df.write.mode("append").parquet(refined_path)
        logger.info(f"Successfully wrote {valid_count} records to Refined Parquet.")

        # Persist to PostgreSQL if configured
        postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")
        postgres_db = os.getenv("POSTGRES_DB", "iot_db")
        postgres_user = os.getenv("POSTGRES_USER", "iot_user")
        postgres_pass = os.getenv("POSTGRES_PASSWORD", "iot_password")
        
        jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
        
        try:
            final_refined_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "refined_machine_measurements") \
                .option("user", postgres_user) \
                .option("password", postgres_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("Successfully wrote micro-batch to PostgreSQL table 'refined_machine_measurements'.")
        except Exception as pg_err:
            logger.error(f"Could not write batch to PostgreSQL JDBC: {pg_err}")


def start_refined_stream():
    """Initializes Spark Session, reads Kafka stream, and executes end-to-end refined pipeline."""
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "iot-machine-events")

    spark = SparkSession.builder \
        .appName("Industrial_IoT_Refined") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.postgresql:postgresql:42.6.0") \
        .master("local[*]") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"[SPARK] Starting Refined Streaming engine from Kafka topic '{kafka_topic}' at {kafka_host}")

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_host) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload", "timestamp as kafka_timestamp") \
        .select(
            F.from_json(F.col("json_payload"), IOT_EVENT_SCHEMA).alias("data"),
            F.col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp") \
        .withColumn("ingestion_timestamp", F.current_timestamp())

    checkpoint_dir = os.path.join("checkpoint", "refined")
    os.makedirs(checkpoint_dir, exist_ok=True)

    logger.info("[SPARK] Starting foreachBatch streaming query...")
    query = parsed_df.writeStream \
        .foreachBatch(process_end_to_end_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    logger.info("[SPARK] Streaming engine is active and waiting for events...")
    query.awaitTermination()


if __name__ == "__main__":
    start_refined_stream()

