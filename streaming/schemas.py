from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# Explicit PySpark Schema for raw IoT events from Kafka
IOT_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("location", StringType(), True),
])
