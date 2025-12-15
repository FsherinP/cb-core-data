import findspark

findspark.init()
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, expr, countDistinct, size,current_timestamp, date_trunc, date_sub,to_timestamp,broadcast
)
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


class DSRComputationUpdatedModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.DSRComputationUpdatedModel"

    def name(self):
        return "DSRComputationUpdatedModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def process_data(self, spark, config):
        try:
            # Active users from user parquet
            activeUsersDF = spark.read.option("recursiveFileLookup", "true").parquet(ParquetFileConstants.USER_PARQUET_FILE) \
                .withColumnRenamed("id", "user_id") \
                .withColumnRenamed("rootorgid", "mdo_id") \
                .withColumn("userCreatedTimestamp", to_timestamp(col("createddate"), "yyyy-MM-dd HH:mm:ss:SSSZ").cast("long")) \
                .filter(col("status") == 1)
            contentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)
            externalContentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE)
            contentDF = spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE) \
                .withColumnRenamed("identifier", "content_id") \
                .withColumnRenamed("primaryCategory", "content_type") \
                .withColumnRenamed("status", "content_status") \
                .withColumnRenamed("courseCategory", "content_sub_type")


            ist_offset = timezone(timedelta(hours=5, minutes=30))

            current_date = datetime.now(ist_offset).date()

            previous_day_start = datetime.combine(current_date - timedelta(days=1), time.min, tzinfo=ist_offset)

            previous_day_end = datetime.combine(current_date, time.min, tzinfo=ist_offset) - timedelta(milliseconds=1)

            prev_start = previous_day_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            prev_end   = previous_day_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            print("previous day start :", prev_start)
            print("previous day end   :", prev_end)
            prev_start_ts = lit(prev_start).cast("timestamp")
            prev_end_ts = lit(prev_end).cast("timestamp")

            #Count of Content Published
            overall_live_course_count = contentDF.filter((col("content_status") == 'Live') & (col("content_type") == 'Course') & (col("content_sub_type").isin('Course', 'Moderated Course'))).count()
            yest_total_live_course_count = contentDF.filter((col("content_status") == 'Live') &
                (col("content_type") == 'Course') &
                (col("content_sub_type").isin('Course', 'Moderated Course')) &
                (col("lastPublishedOn") >= prev_start_ts) &
                (col("lastPublishedOn") <= prev_end_ts)).count()

            Redis.update("live_course_published_overall", str(overall_live_course_count), conf=config)
            Redis.update("live_course_published_yday", str(yest_total_live_course_count), conf=config)

            contentFilter = (col("content_type").isin("Course", "Program", "Blended Program", "CuratedCollections", "Curated Program") &
                            col("content_status").isin("Live", "Retired"))
            enrichedContentEnrolmentsDF = contentEnrolmentDataDF.alias("e").join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"), col("e.courseID") == col("c.content_id"), "left")\
                .join(activeUsersDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner")\
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))
            
            # Overall Course enrolment count
            total_enrolments = enrichedContentEnrolmentsDF.filter(contentFilter).count() + externalContentEnrolmentDataDF.count()

            enrichedContentEnrolmentsYestDF=enrichedContentEnrolmentsDF.filter(contentFilter &
                (col("courseEnrolledTimestamp") >= prev_start_ts) &
                (col("courseEnrolledTimestamp") <= prev_end_ts))

            externalContentEnrolmentDataYestDF = externalContentEnrolmentDataDF.filter(
                (col("enrolled_date") >= prev_start_ts) &
                (col("enrolled_date") <= prev_end_ts))

            #Yesterday Course enrolment count
            yesterday_enrolments = enrichedContentEnrolmentsYestDF.count() + externalContentEnrolmentDataYestDF.count()


            Redis.update("content_enrolments_overall", str(total_enrolments), conf=config)
            Redis.update("content_enrolments_yday", str(yesterday_enrolments), conf=config)


            # Content completions
            completionFilter = (
            (col("dbCompletionStatus") == 2)
            )

            enrichedContentCompletedDF = contentEnrolmentDataDF.alias("e").join(
                 contentDF.select("content_id", "content_type", "content_status").alias("c"), col("e.courseID") == col("c.content_id"), "left")\
                .join(activeUsersDF.select("user_id").alias("u"), col("e.userID") == col("u.user_id"), "inner")\
                .select(col("e.*"), col("c.content_type"), col("c.content_status"))
            
            total_content_completions = enrichedContentCompletedDF.filter(contentFilter & completionFilter).count() + externalContentEnrolmentDataDF.filter(col("status") == 2).count()


            enrichedContentCompletedYestDF = enrichedContentCompletedDF.filter(contentFilter & 
                completionFilter &
                (col("courseCompletedTimestamp") >= prev_start_ts) &
                (col("courseCompletedTimestamp") <= prev_end_ts))

            externalContentCompletedDataYestDF = externalContentEnrolmentDataDF.filter(
                (col("status") == 2) &
                (col("completedon") >= prev_start_ts) &
                (col("completedon") <= prev_end_ts))


            # yesterday Content Completion Count
            yesterday_content_completions = enrichedContentCompletedYestDF.count() + externalContentCompletedDataYestDF.count()


            Redis.update("content_completion_overall", str(total_content_completions), conf=config)
            Redis.update("content_completion_yday", str(yesterday_content_completions), conf=config)

            # --- Registered users (active) & registered yesterday ---
            total_registered_users = activeUsersDF.count()

            Redis.update("users_registered_overall", str(total_registered_users), conf=config)

            usersRegisteredYesterdayCount = activeUsersDF \
            .withColumn("yesterdayStartTimestamp", date_trunc("day", date_sub(current_timestamp(), 1)).cast("long")) \
            .withColumn("todayStartTimestamp", date_trunc("day", current_timestamp()).cast("long")) \
            .filter(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp")) \
            .count()
            print("Total live content:", overall_live_course_count)
            print("Yesterday live content:", yest_total_live_course_count)
            print("Total enrolments:", total_enrolments)
            print("Yesterday enrolments:", yesterday_enrolments)
            print("Total content completions:", total_content_completions)
            print("Yesterday content completions:", yesterday_content_completions)
            print("Total registered users:", total_registered_users)
            print("Yesterday registrations:", usersRegisteredYesterdayCount)

            Redis.update("users_registered_yday", str(usersRegisteredYesterdayCount), conf=config)

            print("[SUCCESS] DSRComputationModel unified metrics updated")

        except Exception as e:
            print(f"❌ Error occurred during DSRComputationModel processing: {str(e)}")
            raise e


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("DSR computation updated Model") \
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
    print(f"[START] DSR computation updated processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = DSRComputationUpdatedModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] DSR computation updated processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


# Example usage:
if __name__ == "__main__":
    main()