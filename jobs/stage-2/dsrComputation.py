import findspark

findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, expr, countDistinct, size,current_timestamp, date_trunc, date_sub,to_timestamp
)
from pyspark.sql.functions import (countDistinct, col, split)
from pyspark.sql.functions import col, split, sum, round

from pyspark.sql.types import (StructType, StructField,StringType)
from datetime import datetime, timedelta, time, timezone
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from dfutil.utils.utils import druidDFOption
from dfutil.enrolment import enrolmentDFUtil
from dfutil.utils import utils
from dfutil.utils.redis import Redis
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config


class DSRComputationModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.DSRComputationModel"

    def name(self):
        return "DSRComputationModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def process_data(self, spark, config):
        try:
            output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
            userDF = spark.read.option("recursiveFileLookup", "true").parquet(ParquetFileConstants.USER_PARQUET_FILE) \
                .withColumnRenamed("id", "user_id") \
                .withColumnRenamed("rootorgid", "mdo_id") \
                .withColumn("userCreatedTimestamp", to_timestamp(col("createddate"), "yyyy-MM-dd HH:mm:ss:SSSZ").cast("long"))
            eventsEnrolmentDataDF = spark.read.parquet(f"{output_path}/eventEnrolmentDetails")
            contentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
            externalContentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE)
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            externalContentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_PARQUET_FILE)
            # --- Active users (status == 1) joined with org
            userWithOrgDF = userDF.filter(col("mdo_id").isNotNull())
            activeUsersDF = userDF.filter(col("status") == 1)

            # --- Content enrolments (active users only)
            enrichedContentEnrolmentsDF = contentEnrolmentDataDF.alias("e").join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"), col("e.courseID") == col("c.content_id"), "left") \
                .join(activeUsersDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner") \
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))
            total_enrolments = enrichedContentEnrolmentsDF.filter((col("content_type").isin("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program")) &
                                                                  (col("content_status").isin("Live", "Retired"))).count() + externalContentEnrolmentDataDF.count()
            Redis.update("dashboard_enrolment_count", str(total_enrolments), conf=config)

            # Unique users enrolled
            enrichedCourseEnrolmentsDF = contentEnrolmentDataDF.alias("e").join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"), col("e.courseID") == col("c.content_id"), "left") \
                .join(userWithOrgDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner") \
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))
            unique_users_enrolled = enrichedCourseEnrolmentsDF.filter((col("content_type").isin("Course")) & (col("content_status").isin("Live", "Retired"))) \
                .agg(countDistinct("e.userID").alias("c")).first()[0]
            Redis.update("dashboard_unique_users_enrolled_count", str(unique_users_enrolled), conf=config)

            # Content completions (distinct certificate_id)
            enrichedContentCompletedDF = contentEnrolmentDataDF.alias("e").join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"), col("e.courseID") == col("c.content_id"), "left") \
                .join(activeUsersDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner") \
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))
            total_content_completions = enrichedContentCompletedDF.filter((col("content_type").isin("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program")) &
                                                                          (col("content_status").isin("Live", "Retired"))).filter(col("dbCompletionStatus") == 2).count() + externalContentEnrolmentDataDF.filter(col("status") == 2).count()
            Redis.update("dashboard_completed_count", str(total_content_completions), conf=config)

            # --- Event metrics ---
            total_event_enrolments = eventsEnrolmentDataDF.select("event_id").count()
            Redis.update("dashboard_events_enrolment_count", str(total_event_enrolments), conf=config)

            enrichedEventCompletionsDF = eventsEnrolmentDataDF \
                .filter(col("certificate_id").isNotNull()) \
                .filter(col("status") == "completed")
            total_event_completions = enrichedEventCompletionsDF.select("certificate_id").distinct().count()
            Redis.update("dashboard_events_completed_count", str(total_event_completions), conf=config)

            # --- Certificates generated yesterday (content + events) ---
            ist_offset = timezone(timedelta(hours=5, minutes=30))

            current_date = datetime.now(ist_offset).date()

            previous_day_start = datetime.combine(current_date - timedelta(days=1), time.min, tzinfo=ist_offset)

            previous_day_end = datetime.combine(current_date, time.min, tzinfo=ist_offset) - timedelta(seconds=1)

            prev_start = previous_day_start.strftime("%Y-%m-%d %H:%M:%S")
            prev_end = previous_day_end.strftime("%Y-%m-%d %H:%M:%S")

            print("previous day start :", prev_start)
            print("previous day end   :", prev_end)

            # Parse strings -> timestamp Columns
            prev_end_ts = to_timestamp(lit(prev_end),   "yyyy-MM-dd HH:mm:ss")
            prev_start_ts = to_timestamp(lit(prev_start),   "yyyy-MM-dd HH:mm:ss")
            content_certs_yday = (enrichedContentCompletedDF.withColumn("firstCompletedOn_ts", to_timestamp(col("firstCompletedOn"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) \
                                  .filter((col("content_type").isin("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program")) & (col("content_status").isin("Live", "Retired")) & \
                                          (col("dbCompletionStatus") == 2) & \
                                          (col("firstCompletedOn_ts") >= prev_start_ts) & \
                                          (col("firstCompletedOn_ts") <= prev_end_ts)).count())

            event_certs_yday = eventsEnrolmentDataDF.filter(col("certificate_id").isNotNull()) \
                .filter(col("status") == "completed") \
                .filter((col("enrolled_on_datetime") >= lit(prev_start)) & (col("enrolled_on_datetime") <= lit(prev_end))) \
                .select("certificate_id").distinct().count()
            external_certs_yday = externalContentEnrolmentDataDF.withColumn("firstCompletedOn", to_timestamp \
                (when((col("issued_certificates").isNotNull()) & (size(col("issued_certificates")) > 0), \
                      col("issued_certificates")[0]["lastIssuedOn"]).otherwise(lit(None)))) \
                .filter((col("firstCompletedOn") >= prev_start_ts) & (col("firstCompletedOn") <= prev_end_ts)).count()
            print("content count : " + str(content_certs_yday))
            print("event count : " + str(event_certs_yday))
            print("external count : "  + str(external_certs_yday))
            total_certs_yday = content_certs_yday + event_certs_yday
            Redis.update("lp_completed_yesterday_count", str(total_certs_yday), conf=config)

            # --- Registered users (active) & registered yesterday ---
            total_registered_users = activeUsersDF.count()
            Redis.update("mdo_total_registered_officer_count", str(total_registered_users), conf=config)

            usersRegisteredYesterdayCount = activeUsersDF \
                .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long")) \
                .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long")) \
                .filter(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp")) \
                .count()
            Redis.update("dashboard_new_users_registered_yesterday", str(usersRegisteredYesterdayCount), conf=config)

            # --- Live courses count including external courses ---
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            liveCourseCount = contentDF.filter(col("content_status").isin("Live", "LIVE")).filter(col("content_sub_type").isin("Course", "Moderated Course", "External Content")).count()
            Redis.update("dashboard_courses_published_live_count", str(liveCourseCount), conf=config)
            parts = split(col("content_duration"), ":")
            result = contentDF.filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("Course", "Moderated Course", "External Content")) \
                .withColumn(
                "duration_seconds",
                parts[0].cast("int") * 3600 +
                parts[1].cast("int") * 60 +
                parts[2].cast("int")) \
                .agg(
                round(sum("duration_seconds") / 3600).cast("int").alias("total_hours"))
            total_hours = result.first()["total_hours"]
            Redis.update("dashboard_courses_published_live_duration", str(total_hours), conf=config)
            # --- MAU (last 30 days) via Druid ---
            loginSchema = StructType([StructField("user_id", StringType(), True)])
            mau_query = """SELECT COUNT(DISTINCT(actor_id)) AS activeCount FROM "telemetry-events-syncts" WHERE eid IN ('IMPRESSION', 'INTERACT', 'START', 'END')
                                                                                                            AND actor_type = 'User' AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '30' DAY, 'P1D')
                                                                                                            AND __time <  TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')"""
            #SELECT COUNT(DISTINCT(uid)) AS activeCount
            #FROM "summary-events"
            #WHERE dimensions_type = 'app'
            #AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP, 'P1D') - INTERVAL '30' DAY
            #AND __time < TIME_FLOOR(CURRENT_TIMESTAMP, 'P1D')"""

            mau_df = druidDFOption(mau_query, config.sparkDruidRouterHost, limit=10000000, spark=spark)
            if mau_df is None:
                mau_df = self._empty_df(spark, "activeCount")

            total_mau = mau_df.select("activeCount").first()[0]
            Redis.update("lp_monthly_active_users", str(total_mau), conf= config)

            print("[SUCCESS] DSRComputationModel unified metrics updated")

        except Exception as e:
            print(f"❌ Error occurred during DSRComputationModel processing: {str(e)}")
            raise e


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("DSR computation Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "15g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] DSR computation processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = DSRComputationModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] DSR computation processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


# Example usage:
if __name__ == "__main__":
    main()
