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
                                     .filter(col("eid").isin("START", "END", "INTERACT", "IMPRESSION", "SEARCH"))
                                     .filter(col("eid").isNotNull())
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
        #--------------------------------------Enrolment from search results----------------------------------------
        search_fe_selected_columns = (search_fe_data
            .select(
                col("actor_id").alias("user_id"),
                col("query"),
                col("event_time").alias("search_time"),
                col("ets").alias("search_ets")
            )
        )

        content_select_from_search_results = (search_fe_data
            .filter((col("eid") == "INTERACT") & (col("edata_id") == "course-card"))
            .select(
                col("actor_id").alias("user_id"),
                col("object_id").alias("content_id"),
                col("object_type").alias("content_type"),
                col("event_time").alias("content_selection_time"),
                col("ets").alias("content_selection_ets")
            )
        )

        content_enrol_telemetry_data = (search_fe_data
            .filter((col("eid") == "INTERACT") & (col("edata_type") == "click") & (col("edata_subtype") == "enroll"))
            .select(
                col("actor_id").alias("user_id"),
                col("object_id").alias("content_id"),
                col("event_time").alias("enrol_time"),
                col("ets").alias("content_enrol_ets"),
                col("object_type").alias("content_type")
            )
        )

        time_window_in_millis = 1*60*1000

        search_to_content = (search_fe_selected_columns
            .join(content_select_from_search_results, on="user_id", how="full")
            .where((col("search_time") < col("content_selection_time")) &
                   (col("content_selection_ets") - col("search_ets") <= time_window_in_millis))
        )

        search_to_content_selection_to_enrolment = (search_to_content
            .join(content_enrol_telemetry_data, on=["user_id", "content_id"], how="full")
            .where((col("content_selection_time") < col("enrol_time")) &
                   (col("content_enrol_ets") - col("content_selection_ets") <= time_window_in_millis)
                   )
        )
        #---------------------------------------------------------------------------------------------------------------

        #--------------------------------------Enrolment from home page sections----------------------------------------
        home_page_clicks_on_sections = (yesterday_telemetry_data
            .filter((col("eid") == "INTERACT") & (col("edata_id") == "card-content") & (col("edata_pageid") == "/page/home"))
            .withColumn("home_page_click_time", col("ets")/ 1000).cast("long")
            .select(
                col("home_page_click_time"),
                col("actor_id").alias("user_id"),
                col("edata_subtype").alias("home_page_section"),
                col("ets").alias("home_page_click_ets"),
                col("object_id").alias("content_id")
            )
        )
        home_enrolments = (content_enrol_telemetry_data
            .select(
                col("enrol_time").alias("home_enrol_time"),
                col("content_enrol_ets").alias("home_enrol_ets"),
                col("user_id"),
                col("content_id"),
                col("content_type")
            )
        )
        enrolment_from_home_page_sections = (home_page_clicks_on_sections
            .join(home_enrolments, on=["user_id","content_id"], how="left")
            .filter((col("home_page_click_time") < col("home_enrol_time")) & (col("home_enrol_ets") - col("home_page_click_ets") <= time_window_in_millis))
            .select(col("user_id"), col("home_page_section"),col("content_id"),col("content_type"),col("home_page_click_time"),col("home_enrol_time"))
        )
        #---------------------------------------------------------------------------------------------------------------




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