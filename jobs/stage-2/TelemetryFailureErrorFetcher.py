import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, \
    to_date, hour, minute, month, weekofyear, year, dayofmonth, dayofweek
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col, when, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws,from_json,explode,trim,length,first,date_sub,current_date,
    count,desc,countDistinct,avg,size, expr, regexp_extract
)
from pyspark.sql.functions import collect_set
from pyspark.sql.types import ArrayType, StringType
import time
from datetime import datetime

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config

# Initialize Spark
spark = SparkSession.builder \
    .appName("TelemetryAnalytics") \
    .config("spark.executor.memory", "25g") \
    .config("spark.driver.memory", "15g") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def processTelemetryErrorFetcher(config):

    try:

        df = spark.read.json("path_to_your_json_file")

        # If reading from Kafka
        # kafka_df = spark.read.format("kafka") \
        #     .option("kafka.bootstrap.servers", "localhost:9092") \
        #     .option("subscribe", "your_topic") \
        #     .option("startingOffsets", "earliest") \
        #     .load()
        #
        # df = kafka_df.selectExpr("CAST(value AS STRING)") \
        #     .select(from_json(col("value"), schema).alias("data")) \
        #     .select("data.*")

        # -----------------------------------
        # 3. Extract Required Fields
        # -----------------------------------
        error_df = df.select(
            col("context.pdata.pid").alias("platform"),
            col("context.pdata.ver").alias("version"),
            col("metadata.validation_error").alias("error"),
            col("eid"),
            col("edata_pageid"),
            col("context.env"),
            col("ets").alias("event_time")
        ).filter(col("error").isNotNull())

        # -----------------------------------
        # 4. Convert ets (epoch millis) to readable time
        # -----------------------------------
        error_df = error_df.withColumn(
            "event_time_readable",
            from_unixtime(col("event_time") / 1000)
        )

        # -----------------------------------
        # 5. Group Similar Errors
        # -----------------------------------
        summary_df = error_df.groupBy(
            "platform",
            "version",
            "error"
        ).agg(
            count("*").alias("total_events_impacted"),
            countDistinct("eid").alias("distinct_events_failed"),
            countDistinct("edata_pageid").alias("distinct_pages_failed"),
            min("event_time_readable").alias("first_occurrence"),
            max("event_time_readable").alias("last_occurrence")
        ).orderBy(col("total_events_impacted").desc())

        # -----------------------------------
        # 6. Show Result
        # -----------------------------------
        summary_df.show(truncate=False)


    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] TelemetryErrorFetcher processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processTelemetryErrorFetcher(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] TelemetryErrorFetcher completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()