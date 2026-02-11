import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, \
    to_date, hour, minute, month, weekofyear, year, dayofmonth, dayofweek

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


def processTelemetryAnalytics(config):

    try:

        #SEARCH analytics
        yesterday = (
            spark.range(1)
            .select(date_format(date_sub(current_date(), 1), "yyyy-MM-dd").alias("yesterday"))
            .first()["yesterday"]
        )

        yesterday_telemetry_data = ( spark.read
                                     .option("mode", "PERMISSIVE")
                                     .option("columnNameOfCorruptRecord", "_corrupt_record")
                                     .json(f'gs://igotqadp/secor-dev/unique/raw/{yesterday}-*'.format(env = "qa", env1 = "dev", yesterday = yesterday))
                                     .filter(col("eid").isin("START", "END", "INTERACT", "IMPRESSION", "SEARCH")).filter(col("eid").isNotNull())
                                     )

        search_backend_events = yesterday_telemetry_data.filter(col("eid") == "SEARCH")

        data_with_time_columns = (search_backend_events
                                  .withColumn("event_time", from_unixtime(col("ets") / 1000).cast("timestamp"))
                                  .withColumn("event_date", to_date(col("event_time")))
                                  .withColumn("event_hour", hour(col("event_time")))
                                  .withColumn("event_minute", minute(col("event_time")))
                                  .withColumn("event_day_of_week", dayofweek(col("event_time")))
                                  .withColumn("event_month", month(col("event_time")))
                                  .withColumn("event_week", weekofyear(col("event_time")))
                                  .withColumn("event_year", year(col("event_time")))
                                  )

        most_common_query = (data_with_time_columns
                             .groupBy(col("edata_query"))
                             .agg(count("*").alias("search_count"))
                             .orderBy(desc("search_count"))
                             )

        peak_search_activity_hour = (data_with_time_columns
                                     .groupBy(col("event_hour"))
                                     .agg(count("*").alias("search_count"))
                                     .orderBy(desc("search_count"))
                                     )

        unique_queries_per_search = (data_with_time_columns
        .withColumn("edata_topn_identifier", from_json(col("edata_topn_identifier"), ArrayType(StringType)))
        .withColumn("content_id", explode(col("edata_topn_identifier")))
        .groupBy(col("edata_query"))
        .agg(
            countDistinct("edata_topn_identifier").alias("unique_results_count"),
            count("*").alias("total_results_count")
        )
        )

        query_specificity = (unique_queries_per_search
                             .withColumn("specificity", col("unique_results_count") / col("total_results_count"))
                             .orderBy(desc(col("specificity")))
                             )

        average_number_of_results_per_query = (data_with_time_columns
        .withColumn("num_results", size(expr("split(edata_topn_identifier, ',')")))
        .groupBy("edata_query")
        .agg(
            avg("num_results").alias("avg_num_results")
        )
        )

        primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
        contentOrgDF = (spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
                        .filter(col("category").isin(primary_categories))
                        .select(col("courseID").alias("content_id"), col("courseName"))
                        )
        content_frequency = (data_with_time_columns
                             .withColumn("edata_topn_identifier", from_json(col("edata_topn_identifier"), ArrayType(StringType)))
                             .withColumn("content_id", explode(col("edata_topn_identifier")))
                             .groupBy("content_id")
                             .agg(
            count("*").alias("frequency")
        )
                             .orderBy(desc("frequency"))
                             )

        # search activity trends
        hourly_activity_df = (data_with_time_columns
                              .groupBy("event_date", "event_hour")
                              .agg(count("*").alias("search_count"))
                              .orderBy("event_date", "event_hour")
                              )

        daily_activity_df = (data_with_time_columns
                             .groupBy("event_date")
                             .agg(count("*").alias("search_count"))
                             .orderBy("event_date")
                             )

        weekly_activity_df = (data_with_time_columns
                              .groupBy("event_week", "event_year")
                              .agg(count("*").alias("search_count"))
                              .orderBy("event_year", "event_week")
                              )

        monthly_activity_df = (data_with_time_columns
                               .groupBy("event_month", "event_year")
                               .agg(count("*").alias("search_count"))
                               .orderBy("event_year", "event_month")
                               )

        search_fe_data = (yesterday_telemetry_data
                          .filter((col("eid") == "IMPRESSION") & (col("edata_pageid") == "/app/globalsearch"))
                          .withColumn("query", regexp_extract(col("edata_uri"), "q=([^&]+)", 1))
                          .filter(col("query").isNotNull & col("query") != "")
                          .withColumn("event_time_bucket", (col("ets") / 1000).cast("long"))
                          )

        search_be_data = (yesterday_telemetry_data
                          .withColumn("event_time_bucket", (col("ets") / 1000).cast("long"))
                          )

        search_flow = (search_be_data.join(search_fe_data, on=["query","event_time_bucket"], how="inner")
                       .withColumn("Search Result Received",
                                   when(col("query_results").isNotNull & size(col("query_results")) > 0, lit("Yes"))
                                   .otherwise(lit("No"))
                                   )
                       .withColumn("Category", lit("Search Query"))
                       )


    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] TelemetryAnalytics processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processTelemetryAnalytics(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] TelemetryAnalytics completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()