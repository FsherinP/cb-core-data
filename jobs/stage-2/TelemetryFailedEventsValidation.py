import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .appName("TelemetryValidationComparator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


failed_df = spark.read.json("failed_events.json")

with open("mandatory_fields.json") as f:
    mandatory_config = json.load(f)

# Broadcast for performance
mandatory_broadcast = spark.sparkContext.broadcast(mandatory_config)


def check_missing_fields(eid, event_dict):
    mandatory_fields = mandatory_broadcast.value.get(eid, [])
    missing = []

    for field in mandatory_fields:
        keys = field.split(".")
        temp = event_dict

        for k in keys:
            if isinstance(temp, dict) and k in temp:
                temp = temp[k]
            else:
                missing.append(field)
                break

    if missing:
        return "Missing fields: " + ", ".join(missing)
    else:
        return "No missing mandatory fields"

check_udf = udf(lambda eid, event: check_missing_fields(eid, event), StringType())


analysis_df = failed_df.withColumn(
    "suggested_fix",
    check_udf(col("eid"), col("*"))
)

final_df = analysis_df.select(
    col("eid"),
    col("context.pdata.pid").alias("platform"),
    col("context.pdata.ver").alias("version"),
    col("metadata.validation_error").alias("error"),
    col("suggested_fix")
)

final_df.show(truncate=False)