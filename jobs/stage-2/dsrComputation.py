import findspark

findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, expr, countDistinct, size, current_timestamp, date_trunc, date_sub,to_timestamp, split, sum, round, length
)
from datetime import datetime, timedelta
from pyspark.sql.types import (StructType, StructField, StringType)
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
            eventsEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE)
            contentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            externalContentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            externalContentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
            # ------------------------------------------------------------------ #
            # --- Active users (status == 1) joined with org
            userWithOrgDF = userDF.filter(col("mdo_id").isNotNull())
            activeUsersDF = userDF.filter(col("status") == 1)

            # ------------------------------------------------------------------ #
            # Content enrolments (active users only)
            # ------------------------------------------------------------------ #
            enrichedContentEnrolmentsDF = contentEnrolmentDataDF.alias("e") \
                .join(activeUsersDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner") \
                .select(col("e.*"))

            total_enrolments = enrichedContentEnrolmentsDF.count()
            Redis.update("dashboard_enrolment_count", str(total_enrolments), conf=config)
            #print(f"Total Enrolments : {total_enrolments}")

            # ------------------------------------------------------------------ #
            # Unique users enrolled in Course (Live or Retired)
            # ------------------------------------------------------------------ #
            enrichedCourseEnrolmentsDF = contentEnrolmentDataDF.alias("e") \
                .join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"),
                col("e.content_id") == col("c.content_id"), "left"           # FIX: e.content_id not e.courseID
            ) \
                .join(
                userWithOrgDF.select("user_id").alias("u"),
                col("e.userID") == col("u.user_id"), "inner"
            ) \
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))

            unique_users_enrolled = enrichedCourseEnrolmentsDF \
                .filter(
                (col("content_type").isin("Course")) &
                (col("content_status").isin("Live", "Retired"))
            ) \
                .agg(countDistinct("e.userID").alias("c")).first()[0]
            Redis.update("dashboard_unique_users_enrolled_count", str(unique_users_enrolled), conf=config)
            #print(f"Unique users enrolled in Course : {unique_users_enrolled}")

            # ------------------------------------------------------------------ #
            # Content completions — certificateID > 5 chars
            # FIX: column is certificateID not certificate_id in contentEnrolmentDataDF
            # ------------------------------------------------------------------ #
            total_content_completions = enrichedContentEnrolmentsDF \
                .filter(length(col("certificateID")) > 5) \
                .count()
            #print(f"Total Completions : {total_content_completions}")
            Redis.update("dashboard_completed_count", str(total_content_completions), conf=config)

            # ------------------------------------------------------------------ #
            # Event metrics
            # ------------------------------------------------------------------ #
            total_event_enrolments = eventsEnrolmentDataDF.count()
            Redis.update("dashboard_events_enrolment_count", str(total_event_enrolments), conf=config)
            #print(f"Total Event enrolments : {total_event_enrolments}")

            enrichedEventCompletionsDF = eventsEnrolmentDataDF \
                .filter(length(col("certificate_id")) > 5)   # certificate_id ✅ correct for events
            total_event_completions = enrichedEventCompletionsDF.count()
            Redis.update("dashboard_events_completed_count", str(total_event_completions), conf=config)
            #print(f"Total Event completions : {total_event_completions}")

            # ------------------------------------------------------------------ #
            # Certificates generated yesterday (content + events)
            # FIX: content uses first_completed_on not first_certificate_generated_on
            #      event uses completed_on_datetime ✅
            # ------------------------------------------------------------------ #
            prev_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            print(prev_day)

            content_certs_yday = enrichedContentEnrolmentsDF \
                .filter(length(col("certificateID")) > 5) \
                .filter(F.to_date(col("first_completed_on")) == prev_day) \
                .count()                                                          # FIX: added .count()

            event_certs_yday = eventsEnrolmentDataDF \
                .filter(length(col("certificate_id")) > 5) \
                .filter(F.to_date(col("completed_on_datetime")) == prev_day) \
                .count()

            print("content count : " + str(content_certs_yday))
            print("event count : " + str(event_certs_yday))
            total_certs_yday = content_certs_yday + event_certs_yday
            Redis.update("lp_completed_yesterday_count", str(total_certs_yday), conf=config)
            #print(f"Total certificates yesterday : {total_certs_yday}")

            # ------------------------------------------------------------------ #
            # Registered users (active) & registered yesterday
            # ------------------------------------------------------------------ #
            total_registered_users = activeUsersDF.count()
            Redis.update("mdo_total_registered_officer_count", str(total_registered_users), conf=config)

            usersRegisteredYesterdayCount = activeUsersDF \
                .withColumn("yesterdayStartTimestamp",
                            date_trunc("day", date_sub(current_timestamp(), 1)).cast("long")) \
                .withColumn("todayStartTimestamp",
                            date_trunc("day", current_timestamp()).cast("long")) \
                .filter(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp")) \
                .count()
            Redis.update("dashboard_new_users_registered_yesterday", str(usersRegisteredYesterdayCount), conf=config)

            # ------------------------------------------------------------------ #
            # Live courses count including external courses
            # FIX: content_sub_type is correct ✅ — was content_substatus in schema comment
            # ------------------------------------------------------------------ #
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            liveCourseCount = contentDF \
                .filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("Course", "Moderated Course", "External Content")) \
                .count()
            Redis.update("dashboard_courses_published_live_count", str(liveCourseCount), conf=config)

            # set new variable for external content live count
            liveCourseCount = contentDF \
                .filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("External Content")) \
                .count()
            Redis.update("dashboard_external_courses_published_live_count", str(liveCourseCount), conf=config)

            parts = split(col("content_duration"), ":")
            result = contentDF \
                .filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("Course", "Moderated Course", "External Content")) \
                .withColumn(
                "duration_seconds",
                parts[0].cast("int") * 3600 +
                parts[1].cast("int") * 60 +
                parts[2].cast("int")
            ) \
                .agg(round(sum("duration_seconds") / 3600).cast("int").alias("total_hours"))
            total_hours = result.first()["total_hours"]
            Redis.update("dashboard_courses_published_live_duration", str(total_hours), conf=config)

            # ------------------------------------------------------------------ #
            # MAU (last 30 days) via Druid
            # ------------------------------------------------------------------ #
            loginSchema = StructType([StructField("user_id", StringType(), True)])
            mau_query = """SELECT COUNT(DISTINCT(actor_id)) AS activeCount
                           FROM "telemetry-events-syncts"
                           WHERE eid IN ('IMPRESSION', 'INTERACT', 'START', 'END')
                             AND actor_type = 'User'
                             AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '30' DAY, 'P1D')
                             AND __time <  TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')"""

            mau_df = druidDFOption(mau_query, config.sparkDruidRouterHost, limit=10000000, spark=spark)
            if mau_df is None:
                mau_df = self._empty_df(spark, "activeCount")

            total_mau = mau_df.select("activeCount").first()[0]
            Redis.update("lp_monthly_active_users", str(total_mau), conf=config)

            print("[SUCCESS] DSRComputationModel unified metrics updated")

        except Exception as e:
            print(f"❌ Error occurred during DSRComputationModel processing: {str(e)}")
            raise e


def main():
    spark = SparkSession.builder \
        .appName("DSR computation Model") \
        .config("spark.executor.memory", "90g") \
        .config("spark.driver.memory", "20g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

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


if __name__ == "__main__":
    main()
