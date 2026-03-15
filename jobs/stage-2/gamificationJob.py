import findspark
from duckdb.experimental.spark.sql.functions import current_date

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, coalesce, lit,current_date, expr, to_date,
    current_timestamp, date_format, from_unixtime, concat_ws, from_json, explode, trim, length, first, countDistinct,
    rank, to_timestamp, date_sub, dense_rank
)
import time
from datetime import datetime

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config
from dfutil.utils.redis import Redis


# Initialize Spark
spark = SparkSession.builder \
    .appName("GamificationJob") \
    .config("spark.executor.memory", "25g") \
    .config("spark.driver.memory", "15g") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def processGamificationJob(config):

    try:
        start_time = time.time()

        # Step 1: Load User Master Data
        print("📊 Step 1: Loading User Master Data...")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        print("✅ Step 1 Complete")

        # Step 2: Load Enrolment Data
        print("📚 Step 2: Loading Enrolment Data...")
        enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)

        user_enrolment_df = (enrolment_df
                             .withColumn("badge_details", explode("issued_badges"))
                             .select("userID","courseID", "dbCompletionStatus", "certificateID",
                                     col("badge_details")["badgeid"].alias("badge_id"),
                                     col("badge_details")["criteria"].alias("badge_criteria"),col("badge_details")["issuedOn"].alias("badge_issued_on"))
                             .withColumn("badge_issued_ts", to_timestamp(col("badge_issued_on"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
                             )
        print("✅ Step 2 Complete")

        # Step 3: Load Content Data from ES
        print("📖 Step 3: Loading Content Data...")
        es_content_data = spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)
        print("✅ Step 3 Complete")

        # Step 4: Load Badges data
        print("🏷️ Step 4: Loading Badges data...")
        primary_categories= ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
        badge_data = (es_content_data
                      .filter(col("category").isin(primary_categories))
                      .withColumn("badge_details", explode("badgeDetails_v1"))
                      .select(
            col("courseID").alias("content_id"),
            col("category"),
            col("badge_details.badgeEarningDateTime").alias("badge_earning_date_time"),
            col("badge_details.badgeId").alias("badge_id"),
            col("badge_details.criteria").alias("badge_criteria"),
            col("badge_details.badgeTitle").alias("badge_title"),
            col("badge_details.badgeSubTitle").alias("badge_sub_title")
        ).withColumn("badge_earning_date_time", from_unixtime(col("badge_earning_date_time")/1000))
                      .filter(col("badge_id").isNotNull())
                      )
        print("✅ Step 4 Complete")

        # Step 5: Join User Enrolment and Badge Data
        print("🔗 Step 5: Joining User Enrolment and Badge Data...")
        enrolment_content_with_badge_data = (user_enrolment_df.withColumnRenamed("courseID", "content_id").withColumnRenamed("badge_id", "enrolment_badge_id")
                                             .join(badge_data, on="content_id", how="left"))
        print("✅ Step 5 Complete")

        # Step 6: Add Total Badges Metric
        print("📊 Step 6: Adding Total Badges Metric...")
        total_badges = badge_data.select("badge_id").distinct().count()
        Redis.update("dashboard_all_course_badge_count", str(total_badges), conf = config)
        print("✅ Step 6 Complete")

        # Step 7: Add Total Live Badges Metric
        print("✨ Step 7: Adding Total Live Badges Metric...")
        total_live_badges = badge_data.filter(col("badge_earning_date_time") >= current_timestamp()).select("badge_id").distinct().count()
        Redis.update("dashboard_live_course_badge_count", str(total_live_badges), conf = config)
        print("✅ Step 7 Complete")

        # Step 8: Add Total Badges Awarded Metric
        print("🎯 Step 8: Adding Total Badges Awarded Metric...")
        total_badges_awarded = user_enrolment_df.select("badge_id").count()
        Redis.update("dashboard_total_badge_awarded_count", str(total_badges_awarded), conf = config)
        print("✅ Step 8 Complete")

        # Step 9: Add Active Learners Metric
        print("📁 Step 9: Adding Active Learners Metric...")
        active_learners = (enrolment_content_with_badge_data.filter(col("dbCompletionStatus") == 1)
                           .select("userID").distinct().count()
                           )
        Redis.update("dashboard_active_learners_for_badge_courses_count", str(active_learners), conf = config)
        print("✅ Step 9 Complete")


        # Step 10: Add Badge Award Rate Metric
        print("🔍 Step 10: Adding Badge Award Rate Metric...")
        badge_earned_learners = enrolment_content_with_badge_data.filter(col("dbCompletionStatus") == 2).select("userID").distinct().count()
        badge_earning_rate = (badge_earned_learners / active_learners * 100) if active_learners > 0 else 0
        Redis.update("dashboard_badge_award_rate", str(badge_earning_rate), conf = config)
        print("✅ Step 10 Complete")

        # Step 11: Add Badge Performance Rate Metric
        print("🔍 Step 11: Adding Badge Performance Rate Metric...")
        window_spec = Window.orderBy(col("user_count").desc())
        badge_performance = (enrolment_content_with_badge_data
                             .groupBy("badge_title")
                             .agg(countDistinct("userID").alias("user_count"))
                             .withColumn("rank", dense_rank().over(window_spec))
                             .select("badge_title","user_count", "rank"))
        Redis.dispatchDataFrame("dashboard_badge_performance_rate", badge_performance, "badge_title", "rank", conf = config)
        print("✅ Step 11 Complete")

        # Step 12: Add Content Completion Rate Metric
        print("🔍 Step 12: Adding Content Completion Rate Metric...")
        content_data = (enrolment_content_with_badge_data
                        .groupBy("content_id")
                        .agg(
            expr("count(distinct userID)").alias("total_enrolments"),
            expr("""countDistinct(when(count(distinct case (col("dbCompletionStatus")) == 2 & when dbCompletionStatus = 2 (col("certificateID").isNotNull()) & and certificateID is not null 
                (col("badge_id").isNotNull()), col("userID")).alias("total_completions_with_badge"))
                and badge_id is not null then userID end)""").alias("total_completions_with_badge")
        ).join(es_content_data.select(col("courseID").alias("content_id"), col("courseName").alias("content_name")), on="content_id", how="inner")
                        .select("content_name","total_enrolments","total_completions_with_badge")
                        )
        Redis.dispatchDataFrame("dashboard_content_completion_rate", content_data, "content_name", "total_enrolments",conf = config)
        print("✅ Step 12 Complete")

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] UserReport processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processGamificationJob(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserReport completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()