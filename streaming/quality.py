from typing import Dict, Any, Tuple, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_STATUSES = {"RUNNING", "STOPPED", "MAINTENANCE", "FAILURE"}


def validate_event_dict(event: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates a single event dictionary (useful for unit tests and Python validation).
    
    Returns:
        (is_valid: bool, errors: List[str])
    """
    errors = []
    
    if not event.get("event_id"):
        errors.append("event_id_null")
    if not event.get("device_id"):
        errors.append("device_id_null")
    if not event.get("event_timestamp"):
        errors.append("event_timestamp_null")
        
    temp = event.get("temperature")
    if temp is None:
        errors.append("temperature_null")
    elif temp < 0.0 or temp > 200.0:
        errors.append("temperature_out_of_range")
        
    press = event.get("pressure")
    if press is None:
        errors.append("pressure_null")
    elif press < 0:
        errors.append("pressure_negative")
        
    vib = event.get("vibration")
    if vib is None:
        errors.append("vibration_null")
    elif vib < 0:
        errors.append("vibration_negative")
        
    rpm = event.get("rpm")
    if rpm is None:
        errors.append("rpm_null")
    elif rpm < 0:
        errors.append("rpm_negative")
        
    status = event.get("status")
    if not status or status not in VALID_STATUSES:
        errors.append("invalid_status")
        
    return len(errors) == 0, errors


def apply_data_quality_spark(df: DataFrame) -> DataFrame:
    """
    Applies Data Quality validation rules to a PySpark DataFrame.
    Adds boolean column `is_valid` and string column `error_reason`.
    """
    quality_conditions = (
        F.col("event_id").isNotNull() & (F.col("event_id") != "") &
        F.col("device_id").isNotNull() & (F.col("device_id") != "") &
        F.col("event_timestamp").isNotNull() &
        F.col("temperature").isNotNull() & (F.col("temperature") >= 0.0) & (F.col("temperature") <= 200.0) &
        F.col("pressure").isNotNull() & (F.col("pressure") >= 0.0) &
        F.col("vibration").isNotNull() & (F.col("vibration") >= 0.0) &
        F.col("rpm").isNotNull() & (F.col("rpm") >= 0) &
        F.col("status").isIn("RUNNING", "STOPPED", "MAINTENANCE", "FAILURE")
    )
    
    error_reasons = F.concat_ws(", ",
        F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), F.lit("event_id_null")),
        F.when(F.col("device_id").isNull() | (F.col("device_id") == ""), F.lit("device_id_null")),
        F.when(F.col("event_timestamp").isNull(), F.lit("event_timestamp_null")),
        F.when(F.col("temperature").isNull() | (F.col("temperature") < 0.0) | (F.col("temperature") > 200.0), F.lit("temperature_out_of_range")),
        F.when(F.col("pressure").isNull() | (F.col("pressure") < 0.0), F.lit("pressure_negative")),
        F.when(F.col("vibration").isNull() | (F.col("vibration") < 0.0), F.lit("vibration_negative")),
        F.when(F.col("rpm").isNull() | (F.col("rpm") < 0), F.lit("rpm_negative")),
        F.when(~F.col("status").isIn("RUNNING", "STOPPED", "MAINTENANCE", "FAILURE"), F.lit("invalid_status"))
    )

    return df.withColumn("is_valid", quality_conditions) \
             .withColumn("error_reason", F.when(~quality_conditions, error_reasons).otherwise(F.lit(None)))
