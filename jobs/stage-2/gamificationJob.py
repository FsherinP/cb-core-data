import os

import findspark
findspark.init()
from duckdb.experimental.spark.sql.functions import current_date, broadcast
from pyspark import StorageLevel
from pyspark.sql.types import *
import sys
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, current_date, expr, to_date, explode_outer, size,
    current_timestamp, from_unixtime, countDistinct,
    rank, to_timestamp, date_sub, dense_rank, trunc, add_months, date_format,lower
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
from dfutil.dfexport import dfexportutil


# Initialize Spark
spark = SparkSession.builder \
    .appName("GamificationJob") \
    .config("spark.executor.memory", "90g") \
    .config("spark.driver.memory", "120g") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def build_metric_df(spark, metric_name, total, prev, curr):
    if prev == 0:
        if curr > 0:
            trend = ["INCREASED"]
            count_rate = 100.0
        else:
            trend = ["NO_CHANGE"]
            count_rate = 0.0
    else:
        count_rate = ((curr - prev) / prev) * 100
        if curr > prev:
            trend = ["INCREASED"]
        elif curr < prev:
            trend = ["DECREASED"]
        else:
            trend = ["NO_CHANGE"]
    return spark.createDataFrame([Row(
        metric=metric_name,
        totalCount=total,
        countRate=count_rate,
        trend=trend
    )])

def processGamificationJob(config):

    try:
        start_time = time.time()
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

        # Step 1: Load Enrolment Data
        print("📚 Step 1: Loading Enrolment Data...")
        enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)

        user_enrolment_df = (enrolment_df
                             .withColumn("badge_details", explode_outer("issued_badges"))
                             .select("userID","courseID", "dbCompletionStatus", "certificateID",
                                     col("badge_details")["badgeId"].alias("badge_id"),
                                     col("badge_details")["criteria"].alias("badge_criteria_enrolment"),
                                     col("badge_details")["issuedOn"].alias("badge_issued_on"))
                             .withColumn("badge_issued_ts", to_date(to_timestamp(col("badge_issued_on"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")))
                             )
        external_enrolment_df = (spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
                                 .withColumn("badge_details", explode_outer("issued_badges"))
                                 .withColumn("certificateID",
                                             when(col("issued_certificates").isNull(), "")
                                             .otherwise(col("issued_certificates")[size(col("issued_certificates")) - 1]["identifier"]))
                                 .withColumnRenamed("content_id", "courseID")
                                 .withColumnRenamed("userid", "userID")
                                 .withColumnRenamed("status", "dbCompletionStatus")
                                 .select("userID","courseID", "dbCompletionStatus", "certificateID",
                                         col("badge_details.badgeId").alias("badge_id"),
                                         col("badge_details.criteria").alias("badge_criteria_enrolment"),
                                         col("badge_details.issuedOn").alias("badge_issued_on"))
                                 .withColumn("badge_issued_ts", to_date(to_timestamp(col("badge_issued_on"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))))
        enrolment_complete_data = user_enrolment_df.unionByName(external_enrolment_df)
        print("✅ Step 1 Complete")

        # Step 2: Load External Content Data
        print("📚 Step 2: Loading External Content Data...")
        external_content_data = (spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
                                 .filter(col("badge").isNotNull())
                                 #.withColumn("parsed", from_json(col("cios_data"), schema))
                                 .withColumn("badge", explode_outer(col("badge")))
                                 .withColumn("badge_earning_date_time", when(col("badge.badgeEarningDateEnabled") == True,
                                                                             from_unixtime(col("badge.badgeEarningDateTime")/1000)).otherwise(lit(None)))
                                 .withColumn("is_badge_active", when(col("badge.badgeEarningDateEnabled") == False, True)
                                             .when((col("badge.badgeEarningDateEnabled") == True) & (col("badge_earning_date_time").isNotNull()) &
                                                   (col("badge_earning_date_time") >= current_timestamp()), True).otherwise(False))
                                 .select("content_id","is_badge_active", col("badge.badgeId").alias("badge_id"),
                                         col("badge.criteria").alias("badge_criteria_content"),col("courseStatus").alias("courseReviewStatus"),
                                         col("badge.badgeTitle").alias("badge_title"),col("courseName"),col("category"),
                                         to_date(to_timestamp(col("badge.createdOn"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")).alias("badge_created_date_time"),
                                         col("badge.badgeSubTitle").alias("badge_sub_title"), col("badge.badgeEarningDateTime").alias("badge_earning_date"))
                                 )
        print("✅ Step 2 Complete")

        # Step 3: Load Content Badges data
        print("🏷️ Step 3: Loading Content Badges data...")
        es_content_data = spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)
        badge_data = (es_content_data
                      .filter(col("badgeDetails_v1").isNotNull())
                      .withColumn("badge_details", explode_outer("badgeDetails_v1"))
                      .withColumn("badge_earning_date_time", from_unixtime(col("badge_details.badgeEarningDateTime")/1000))
                      .withColumn("is_badge_active", when(col("badge_details.badgeEarningDateEnabled") == False, True)
                                  .when((col("badge_details.badgeEarningDateEnabled") == True) & (col("badge_earning_date_time").isNotNull()) &
                                        (col("badge_earning_date_time") >= current_timestamp()), True).otherwise(False))
                      .select(
                            col("courseID").alias("content_id"),col("courseReviewStatus"),
                            col("is_badge_active"),
                            col("badge_details.badgeId").alias("badge_id"),col("category"),
                            col("badge_details.criteria").alias("badge_criteria_content"),
                            col("badge_details.badgeTitle").alias("badge_title"),col("courseName"),
                            to_date(to_timestamp(col("badge_details.createdOn"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")).alias("badge_created_date_time"),
                            col("badge_details.badgeSubTitle").alias("badge_sub_title"), col("badge_details.badgeEarningDateTime").alias("badge_earning_date")
            )
        )
        content_badge_complete_data = badge_data.unionByName(external_content_data)
        print("✅ Step 3 Complete")

        # Step 4: Join User Enrolment and Badge Data
        print("🔗 Step 4: Joining User Enrolment and Badge Data...")
        enrolment_content_with_badge_data = (enrolment_complete_data.withColumnRenamed("courseID", "content_id").withColumnRenamed("badge_id", "enrolment_badge_id")
                                             .join(broadcast(content_badge_complete_data), on="content_id", how="left"))
        enrolment_content_with_badge_data.cache()
        enrolment_content_with_badge_data.count()
        enrolment_content_with_badge_data.write.mode("overwrite").option("compression", "snappy").parquet(ParquetFileConstants.GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE)
        print("✅ Step 4 Complete")

        current_month_start = trunc(current_date(), "month")
        previous_month_start = trunc(add_months(current_date(), -1), "month")

        # Step 5: Add Enrolment Related Metrics
        print("✨ Step 5: Adding Enrolment Related Metrics...")
        enrolment_related_metrics = enrolment_content_with_badge_data.select(
            # -------------------------------
            # Total badges
            # -------------------------------
            F.countDistinct("badge_id").alias("total_badges"),
            F.countDistinct(F.when(
                (col("badge_created_date_time") >= previous_month_start) &
                (col("badge_created_date_time") < current_month_start), col("badge_id")
            )).alias("total_badges_previous_month"),
            F.countDistinct(F.when(
                col("badge_created_date_time") >= current_month_start , col("badge_id")
            )).alias("total_badges_current_month"),

            # -------------------------------
            # Total live badges
            # -------------------------------
            F.countDistinct(F.when(col("is_badge_active") == True, col("badge_id"))).alias("total_live_badges"),
            F.countDistinct(F.when(
                (col("is_badge_active") == True) &
                (col("badge_created_date_time") >= previous_month_start) &
                (col("badge_created_date_time") < current_month_start), col("badge_id")
            )).alias("total_live_badges_previous_month"),
            F.countDistinct(F.when(
                (col("is_badge_active") == True) &
                (col("badge_created_date_time") >= current_month_start) , col("badge_id")
            )).alias("total_live_badges_current_month"),

            # -------------------------------
            # Total badges awarded
            # -------------------------------
            F.count(F.when(col("enrolment_badge_id").isNotNull(), col("enrolment_badge_id"))).alias("total_badges_awarded"),
            F.sum(F.when(
                (col("badge_issued_ts") >= previous_month_start) &
                (col("badge_issued_ts") < current_month_start), 1)
                  .otherwise(0)).alias("total_badges_awarded_previous_month"),
            F.sum(F.when(
                col("badge_issued_ts") >= current_month_start, 1
            ).otherwise(0)).alias("total_badges_awarded_current_month"),

            # -------------------------------
            # Active Learners
            # -------------------------------
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 1) & (col("badge_id").isNotNull()), col("userID")
            )).alias("active_learners"),
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 1) &
                (col("badge_id").isNotNull()) &
                (col("badge_issued_ts") >= previous_month_start) &
                (col("badge_issued_ts") < current_month_start), col("userID")
            )).alias("active_learners_previous_month"),
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 1) &
                (col("badge_id").isNotNull()) &
                (col("badge_issued_ts") >= current_month_start), col("userID")
            )).alias("active_learners_current_month"),

            # -------------------------------
            # Badge earned learners
            # -------------------------------
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 2) & (col("enrolment_badge_id").isNotNull()), col("userID")
            )).alias("badge_earned_learners"),
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 2) &
                (col("enrolment_badge_id").isNotNull()) &
                (col("badge_issued_ts") >= previous_month_start) &
                (col("badge_issued_ts") < current_month_start),  col("userID")
            )).alias("badge_earned_learners_previous_month"),
            F.countDistinct(F.when(
                (col("dbCompletionStatus") == 2) &
                (col("enrolment_badge_id").isNotNull()) &
                (col("badge_issued_ts") >= current_month_start),  col("userID")
            )).alias("badge_earned_learners_current_month")
        ).collect()[0]

        # -------------------------------
        # Total badges
        # -------------------------------
        total_badges = enrolment_related_metrics["total_badges"]
        total_badges_previous_month = enrolment_related_metrics["total_badges_previous_month"]
        total_badges_current_month = enrolment_related_metrics["total_badges_current_month"]
        total_badges_metric = build_metric_df(spark, "total_badges", total_badges, total_badges_previous_month, total_badges_current_month)
        print("total badges")
        Redis.dispatchDataFrameList("dashboard_all_course_badge_count_last_month_diff",total_badges_metric, "metric", ["totalCount", "countRate", "trend"],  conf = config)

        # -------------------------------
        # Total live badges
        # -------------------------------
        total_live_badges = enrolment_related_metrics["total_live_badges"]
        total_live_badges_previous_month = enrolment_related_metrics["total_live_badges_previous_month"]
        total_live_badges_current_month = enrolment_related_metrics["total_live_badges_current_month"]
        total_live_badges_metric = build_metric_df(spark, "total_live_badges", total_live_badges, total_live_badges_previous_month, total_live_badges_current_month)
        print("total live badges")
        Redis.dispatchDataFrameList("dashboard_live_course_badge_count_last_month_diff",total_live_badges_metric, "metric", ["totalCount", "countRate", "trend"], conf = config)

        # -------------------------------
        # Total badges awarded
        # -------------------------------
        total_badges_awarded = enrolment_related_metrics["total_badges_awarded"]
        total_badges_awarded_previous_month = enrolment_related_metrics["total_badges_awarded_previous_month"]
        total_badges_awarded_current_month = enrolment_related_metrics["total_badges_awarded_current_month"]
        total_badges_awarded_diff = build_metric_df(spark, "badges_awarded", total_badges_awarded, total_badges_awarded_previous_month, total_badges_awarded_current_month)
        print("total badges awarded")
        Redis.dispatchDataFrameList("dashboard_total_badge_awarded_count_last_month_diff",total_badges_awarded_diff,"metric",["totalCount", "countRate", "trend"], conf = config)

        # -------------------------------
        # Active Learners
        # -------------------------------
        active_learners = enrolment_related_metrics["active_learners"]
        active_learners_previous_month = enrolment_related_metrics["active_learners_previous_month"]
        active_learners_current_month = enrolment_related_metrics["active_learners_current_month"]
        active_learners_diff = build_metric_df(spark, "active_learners_diff", active_learners, active_learners_previous_month, active_learners_current_month)
        print("active learners")
        Redis.dispatchDataFrameList("dashboard_active_learners_for_badge_courses_count_last_month_diff",active_learners_diff, "metric", ["totalCount", "countRate", "trend"],conf = config)

        # -------------------------------
        # Badge earned learners
        # -------------------------------
        badge_earned_learners = enrolment_related_metrics["badge_earned_learners"]
        badge_earned_learners_previous_month = enrolment_related_metrics["badge_earned_learners_previous_month"]
        badge_earned_learners_current_month = enrolment_related_metrics["badge_earned_learners_current_month"]
        badge_earning_rate = (badge_earned_learners / active_learners * 100) if active_learners > 0 else 0
        badge_earning_rate_previous_month = (badge_earned_learners_previous_month / active_learners_previous_month * 100) if active_learners_previous_month > 0 else 0
        badge_earning_rate_current_month = (badge_earned_learners_current_month / active_learners_current_month * 100) if active_learners_current_month > 0 else 0
        badge_earning_rate_diff = build_metric_df(spark, "badge_earned_learners", badge_earning_rate, badge_earning_rate_previous_month, badge_earning_rate_current_month)
        Redis.dispatchDataFrameList("dashboard_badge_earning_rate_last_month_diff",badge_earning_rate_diff,"metric", ["totalCount", "countRate", "trend"], conf = config)
        print("badge earning rate")
        print("✅ Step 5 Complete")

        # Step 6: Add Badge Performance Rate Metric
        print("📁 Step 6: Adding Badge Performance Rate Metric...")
        badge_performance_df = enrolment_content_with_badge_data.select("enrolment_badge_id","badge_title","userID").filter(col("enrolment_badge_id").isNotNull()).groupBy("badge_title").agg(F.count("userID").alias("user_count"))
        window_spec = Window.orderBy(col("user_count").desc())
        badge_performance = badge_performance_df.withColumn("rank", dense_rank().over(window_spec))
        Redis.dispatchDataFrameList("dashboard_badge_performance_rate", badge_performance, "badge_title", ["rank","user_count"], conf = config)
        print("✅ Step 6 Complete")

        # Step 7: Add Content Completion Rate Metric
        print("🔍 Step 7: Adding Content Completion Rate Metric...")
        content_data = (enrolment_content_with_badge_data.select("content_id", "userID", "dbCompletionStatus", "certificateID", "enrolment_badge_id", "courseReviewStatus", "content_id", "courseName").filter(lower(col("courseReviewStatus")) == "live").groupBy("content_id", "courseName").agg(
            expr("count( userID)").alias("total_enrolments"),
            expr("""
                count( CASE 
                    WHEN dbCompletionStatus = 2 
                         AND certificateID IS NOT NULL 
                         AND enrolment_badge_id IS NOT NULL 
                    THEN userID 
                END)
            """).alias("total_completions_with_badge")
        ).select(
            col("courseName").alias("content_name"),"total_enrolments","total_completions_with_badge"
        ).withColumn(
            "sort_col",
            F.when(
                F.max("total_completions_with_badge").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)) == 0,
                F.col("total_enrolments")
            ).otherwise(F.col("total_completions_with_badge"))
        ).orderBy(F.col("sort_col").desc()).limit(10).drop("sort_col"))
        print("content_data")
        Redis.dispatchDataFrameList("dashboard_content_completion_rate",content_data,"content_name", ["total_enrolments", "total_completions_with_badge"],conf = config)
        print("✅ Step 7 Complete")

        # Step 8: Add Gamification MDO report
        print("🔍 Step 8: Adding Gamification MDO report...")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select(
            "userID",col("fullName").alias("Learner Name"), col("ministry_name").alias("Ministry"),
            col("dept_name").alias("Department"), col("userOrgID").alias("Organization ID"), col("employmentDetails.employeeCode").alias("Employee_Id"))
        reporting_data = (enrolment_content_with_badge_data.filter(col("badge_id").isNotNull()).select(
            col("userID"),
            col("enrolment_badge_id").alias("Badge ID"),
            col("badge_title").alias("Badge Title"),
            col("badge_sub_title").alias("Badge Subtitle"),
            col("badge_criteria_enrolment").alias("Rule/criteria ID"),
            col("content_id").alias("courseID"),
            col("courseName").alias("Content Name"),
            col("category").alias("Source"),
            col("badge_issued_ts").alias("Date and time of award"),
            col("badge_id"),
            when(col("dbCompletionStatus").isNull(), "not-enrolled")
            .when(col("dbCompletionStatus") == 0, "not-started")
            .when(col("dbCompletionStatus") == 1, "in-progress")
            .otherwise("completed")
            .alias("Content Completion Status")
        ).join(user_master_df, on="userID", how="inner")
                          )
        reporting_data = (reporting_data.filter(col("badge_id").isNotNull())
                          .select("Learner Name",col("Employee_Id").alias("Employee Id"),"Content Name", "Content Completion Status", "Badge ID","Badge Title","Badge Subtitle","Rule/criteria ID", "Source", "Date and time of award", "Ministry", "Department", "Organization ID")
                          .withColumn("Report_Last_Generated_On", currentDateTime).withColumn("mdoid", col("Organization ID"))
                          .repartition(col("Organization ID")).cache())
        today = datetime.now().strftime("%Y-%m-%d")

        dfexportutil.write_csv_per_mdo_id_duckdb(
            reporting_data,
            f"{config.localReportDir}/{config.gamificationReportPath}/{today}",
            'mdoid',
            f"{config.localReportDir}/temp/gamificationReport/{today}",
            csv_filename="GamificationReport.csv"
        )
        reporting_data.unpersist()
        enrolment_content_with_badge_data.unpersist(blocking=True)


    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] Gamification processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processGamificationJob(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Gamification completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()