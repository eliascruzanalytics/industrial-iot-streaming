import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Windows PySpark HADOOP_HOME & Java setup to prevent FileNotFoundException and Java 21+ getSubject Exception
if sys.platform.startswith("win"):
    # Fix PySpark Java 21+ incompatibility by prioritizing Java 8/11/17 if installed
    for java_candidate in [
        r"C:\Program Files\Java\jre1.8.0_491",
        r"C:\Program Files\Java\jdk1.8.0",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Java\jdk-11",
    ]:
        if os.path.exists(java_candidate):
            os.environ["JAVA_HOME"] = java_candidate
            os.environ["PATH"] = os.path.join(java_candidate, "bin") + os.pathsep + os.environ.get("PATH", "")
            break

    hadoop_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".hadoop_home"))
    bin_dir = os.path.join(hadoop_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    winutils_exe = os.path.join(bin_dir, "winutils.exe")
    dll_file = os.path.join(bin_dir, "hadoop.dll")
    if not os.path.exists(winutils_exe) or not os.path.exists(dll_file):
        try:
            import urllib.request
            winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"
            dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll"
            if not os.path.exists(winutils_exe):
                urllib.request.urlretrieve(winutils_url, winutils_exe)
            if not os.path.exists(dll_file):
                urllib.request.urlretrieve(dll_url, dll_file)
        except Exception:
            pass
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()


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
