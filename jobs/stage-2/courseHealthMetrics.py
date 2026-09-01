import findspark
findspark.init()

import os
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    col, when, count, lit, explode_outer, from_json, greatest, countDistinct, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, DoubleType, IntegerType
)
import redis

import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))

from dfutil.utils.redis import Redis
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config


# ── Constants ──────────────────────────────────────────────────────────────────
W_COMPLETION  = 5
W_DROPOFF     = 15
W_AVG_RATING  = 10
W_RESOURCE    = 10
W_SENTIMENT   = 10
W_TIME_SPENT  = 10
D_DYNAMIC_MAX = 50      # sum of all dynamic metric weights: 5+15+10+10+10 = 50
D_STATIC_MAX  = 10      # sum of all static metric weights:  10
WINDOW_DAYS   = 90      # completion rate: last 90 days enrolments
DROPOFF_DAYS  = 30      # drop-off rate: enrolments 30+ days old

# ── Metric config ──────────────────────────────────────────────────────────────
METRIC_CONFIG = {
    "completion_rate": {
        "name":      "Completion Rate",
        "overview":  "Shows how smoothly learners progress through and finish the course. If completion rates are low, it may suggest that learners are encountering barriers such as unclear instructions, content gaps, technical issues etc. or reduced learner's interest in continuing the course.",
        "maxWeight": W_COMPLETION,
        "type":      "dynamic"
    },
    "dropoff_rate": {
        "name":      "Drop-off Rate",
        "overview":  "Measures the percentage of learners who enroll in a course but do not progress beyond the first learning resource within 90 days. Low score indicates that the course introduction or initial content is not effectively encouraging learners to continue and high score indicates that learners are continuing beyond the course introduction.",
        "maxWeight": W_DROPOFF,
        "type":      "dynamic"
    },
    "avg_rating": {
        "name":      "Average Rating",
        "overview":  "Measures how a course's average rating compares with the average rating of all courses on the platform. A higher score indicates that learners rate the course more favorably than average, while a lower score suggests that learner ratings are below the platform average.",
        "maxWeight": W_AVG_RATING,
        "type":      "dynamic"
    },
    "resource_length": {
        "name":      "Resource Length Distribution",
        "overview":  "Measures the percentage of course resources that are within 10 minutes duration. Shorter learning modules are generally associated with better learner retention and easier content consumption. A higher score indicates a well-balanced course structure with fewer overly long resources, while a lower score may suggest the presence of lengthy modules that could impact learner progress.",
        "maxWeight": W_RESOURCE,
        "type":      "static"
    },
    "feedback_sentiment": {
        "name":      "Feedback Sentiment",
        "overview":  "Measures how the overall sentiment of learner feedback compares with the average sentiment across all courses on the platform. A higher score indicates more positive learner feedback, while a lower score suggests more negative feedback relative to the platform average.",
        "maxWeight": W_SENTIMENT,
        "type":      "dynamic"
    },
    "time_spent": {
        "name":      "Course Time Spent vs Expected",
        "overview":  "Measures whether learners spend an appropriate amount of time completing the course. Significantly less time may indicate skipped content, while significantly more time may suggest difficulties understanding the material or navigating the course.",
        "maxWeight": W_TIME_SPENT,
        "type":      "dynamic"
    }
}

METRIC_KEYS = ["completion_rate", "dropoff_rate", "avg_rating",
               "resource_length", "feedback_sentiment", "time_spent"]


class ContentHealthMetricsModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.health.ContentHealthMetricsModel"

    def name(self):
        return "ContentHealthMetricsModel"

    def read_postgres_history(self, spark, postgres_url, username, password):
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", "course_health_metrics_history") \
                .option("user", username) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            return df
        except Exception as e:
            print(f"⚠️  Could not read PostgreSQL history (table may not exist yet): {e}")
            return None

    def read_druid_time_spent(self, spark, druid_host, three_month_start, calculated_month):
        """
        Avg actual time spent per course (seconds), last 3 months.
        Runs month by month to avoid Druid gateway timeout on large scans.
        Results are unioned and re-aggregated.
        """
        from dfutil.utils import utils
        from datetime import datetime, timedelta

        # Build list of monthly windows within the 3-month range
        # e.g. for June run: [(Mar-01, Apr-01), (Apr-01, May-01), (May-01, Jun-01)]
        windows = []
        start_dt = datetime.strptime(three_month_start, "%Y-%m-%d")
        end_dt   = datetime.strptime(calculated_month,   "%Y-%m-%d")
        cursor   = start_dt
        while cursor < end_dt:
            if cursor.month == 12:
                next_cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                next_cursor = cursor.replace(month=cursor.month + 1)
            next_cursor = min(next_cursor, end_dt)
            windows.append((cursor.strftime("%Y-%m-%d"), next_cursor.strftime("%Y-%m-%d")))
            cursor = next_cursor

        print(f"📅 Druid: fetching {len(windows)} monthly windows: {windows}")

        monthly_dfs = []
        for w_start, w_end in windows:
            query = f"""
                SELECT
                    object_rollup_l1               AS content_id,
                    actor_id,
                    SUM(edata_duration)            AS user_total_time_spent
                FROM "telemetry-events-syncts"
                WHERE
                    eid = 'END'
                    AND edata_type = 'Player'
                    AND object_rollup_l1 IS NOT NULL
                    AND object_rollup_l1 <> ''
                    AND __time >= '{w_start}' AND __time < '{w_end}'
                GROUP BY object_rollup_l1, actor_id
            """
            print(f"  Fetching {w_start} → {w_end}...")
            try:
                df = utils.druidDFOption(query, druid_host, limit=5000000, spark=spark)
                if df is not None:
                    monthly_dfs.append(df)
                    print(f"  ✅ {w_start} → {w_end}: {df.count()} records")
                else:
                    print(f"  ⚠️  {w_start} → {w_end}: empty result")
            except Exception as e:
                print(f"  ⚠️  {w_start} → {w_end} failed: {e}")

        if not monthly_dfs:
            print("⚠️  No Druid data fetched for any window.")
            return None

        try:
            # Union all monthly results, then aggregate user totals across months,
            # then average across users per course
            from pyspark.sql.functions import col as _col
            from pyspark.sql import functions as _F

            combined = monthly_dfs[0]
            for mdf in monthly_dfs[1:]:
                combined = combined.unionByName(mdf)

            result = combined                 .groupBy("content_id", "actor_id")                 .agg(_F.sum("user_total_time_spent").alias("total_time"))                 .groupBy("content_id")                 .agg(_F.avg("total_time").alias("avg_actual_time_spent"))

            print(f"✅ Druid time spent records (3 months): {result.count()}")
            return result

        except Exception as e:
            print(f"⚠️  Could not aggregate Druid results: {e}")
            return None

    def _metric_month_array_element(self, metric_key: str) -> F.Column:
        cfg = METRIC_CONFIG[metric_key]
        score_path          = f"$.{metric_key}.score"
        weighted_score_path = f"$.{metric_key}.weighted_score"
        return F.concat(
            F.lit('{'),
            F.lit(f'"name":"{cfg["name"]}",'),
            F.lit(f'"overview":"{cfg["overview"]}",'),
            F.lit(f'"maxWeight":{cfg["maxWeight"]},'),
            F.lit(f'"type":"{cfg["type"]}",'),
            F.lit('"score":'),
            F.coalesce(F.get_json_object(col("metrics"), score_path),          F.lit("0")),
            F.lit(',"weighted_score":'),
            F.coalesce(F.get_json_object(col("metrics"), weighted_score_path), F.lit("0")),
            F.lit('}')
        )

    def build_trend_details_df(self, history_df_raw, calculated_month, calculated_at, recent_months):
        if history_df_raw is None or not recent_months:
            return None
        try:
            monthly_df = history_df_raw \
                .filter(col("calculated_month").isin(recent_months)) \
                .select(
                col("content_id"),
                F.date_format(
                    F.to_date(col("calculated_month"), "yyyy-MM-dd"),
                    "MMM-yyyy"
                ).alias("month_label"),
                col("metrics")
            )
            metric_elements = [self._metric_month_array_element(mk) for mk in METRIC_KEYS]
            array_content   = F.concat(*[
                elem if i == len(metric_elements) - 1
                else F.concat(elem, F.lit(","))
                for i, elem in enumerate(metric_elements)
            ])
            month_object = F.concat(
                F.lit('"'), col("month_label"), F.lit('":['),
                array_content, F.lit(']')
            )
            trend_df = monthly_df \
                .withColumn("month_obj_fragment", month_object) \
                .groupBy("content_id") \
                .agg(
                F.concat(
                    F.lit("{"),
                    F.concat_ws(",", F.collect_list(col("month_obj_fragment"))),
                    F.lit(f',"calculated_at":"{calculated_at}"}}')
                ).alias("trend_details_json")
            ).cache()
            print(f"✅ trend_details_df built: {trend_df.count()}")
            return trend_df
        except Exception as e:
            print(f"⚠️  Could not build trend_details: {e}")
            return None

    def process_data(self, spark, config):
        try:
            calculated_at    = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            calculated_month = datetime.utcnow().strftime("%Y-%m-01")

            now = datetime.utcnow()
            last_month_str = (
                f"{now.year - 1}-12-01" if now.month == 1
                else f"{now.year}-{now.month - 1:02d}-01"
            )

            # Last month start and end dates
            last_month_end_dt   = datetime(now.year, now.month, 1) - timedelta(days=1)
            last_month_start_dt = datetime(last_month_end_dt.year, last_month_end_dt.month, 1)
            last_month_start    = last_month_start_dt.strftime("%Y-%m-%d")
            last_month_end      = last_month_end_dt.strftime("%Y-%m-%d")

            # 3-month window: go back 2 more months from last_month_start
            three_month_start_dt = (last_month_start_dt - timedelta(days=1)).replace(day=1)
            three_month_start_dt = (three_month_start_dt - timedelta(days=1)).replace(day=1)
            three_month_start    = three_month_start_dt.strftime("%Y-%m-%d")

            print(f"📅 Last month window : {last_month_start} to {last_month_end}")
            print(f"📅 3-month window    : {three_month_start} to {last_month_end}")

            # ── Read source data ───────────────────────────────────────────────
            print("📥 Reading source data...")
            warehouse_path       = config.warehouseReportDir
            enrolment_df         = spark.read.parquet(f"{warehouse_path}/{config.dwEnrollmentsTable}").cache()
            consumption_df       = spark.read.parquet("/home/analytics/pyspark/data-res/pq_files/cache_pq/consumption_v2").cache() \
                .filter(col("status") == 2).cache()  # pre-filter completed only
            content_resource_df  = spark.read.parquet(f"{warehouse_path}/{config.dwContentResourceTable}")
            ratings_df           = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE).cache()
            content_hierarchy_df = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE).withColumnRenamed("identifier", "courseID").cache()
            content_df = spark.read.parquet("/home/analytics/pyspark/warehouse/content") \
                .filter(col("content_type").isin(["Course", "Moderated Course"])) \
                .cache()
            try:
                sentiment_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/analytics/pyspark/data_res/pq_files/cache_pq/feedback_sentiment").cache()
                sentiment_available = True
                print("✅ Sentiment CSV loaded.")
            except Exception as e:
                print(f"⚠️  Sentiment CSV not found, skipping feedback_sentiment metric: {e}")
                sentiment_df = None
                sentiment_available = False

            r = redis.Redis(
                host=config.redisHost,
                port=config.redisPort,
                db="12",
                decode_responses=True)

            # ── Read PostgreSQL history ────────────────────────────────────────
            print("📥 Reading PostgreSQL history...")
            postgres_url   = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"
            history_df_raw = self.read_postgres_history(
                spark, postgres_url, config.dwPostgresUsername, config.dwPostgresCredential
            )

            # ── Last month score → diff_percentage ────────────────────────────
            last_month_score_df = None
            if history_df_raw is not None:
                try:
                    last_month_score_df = history_df_raw \
                        .filter(col("calculated_month") == lit(last_month_str)) \
                        .select(
                        col("content_id"),
                        F.get_json_object(col("metrics"), "$.total_health_score")
                        .cast("double")
                        .alias("last_month_health_score")
                    ).cache()
                    print(f"✅ Last month records loaded: {last_month_score_df.count()}")
                except Exception as e:
                    print(f"⚠️  Could not parse last month scores: {e}")
                    last_month_score_df = None

            # ── Resolve recent_months ──────────────────────────────────────────
            recent_months = []
            if history_df_raw is not None:
                try:
                    recent_months = [
                        row["calculated_month"]
                        for row in history_df_raw
                        .filter(col("calculated_month") < lit(calculated_month))
                        .select("calculated_month")
                        .distinct()
                        .orderBy(col("calculated_month").desc())
                        .limit(6)
                        .collect()
                    ]
                    print(f"📅 Recent months found: {recent_months}")
                except Exception as e:
                    print(f"⚠️  Could not resolve recent months: {e}")

            # ── last_6_month_score ─────────────────────────────────────────────
            last_6_months_df = None
            if recent_months:
                try:
                    last_6_months_df = history_df_raw \
                        .filter(col("calculated_month").isin(recent_months)) \
                        .select(
                        col("content_id"),
                        F.date_format(
                            F.to_date(col("calculated_month"), "yyyy-MM-dd"),
                            "MMM-yyyy"
                        ).alias("month_label"),
                        F.get_json_object(col("metrics"), "$.total_health_score")
                        .cast("double")
                        .alias("monthly_health_score")
                    ) \
                        .withColumn(
                        "kv_fragment",
                        F.concat(
                            F.lit('"'), col("month_label"), F.lit('":'),
                            F.coalesce(col("monthly_health_score").cast("string"), F.lit("null"))
                        )
                    ) \
                        .groupBy("content_id") \
                        .agg(
                        F.concat(
                            F.lit("{"),
                            F.concat_ws(",", F.collect_list(col("kv_fragment"))),
                            F.lit("}")
                        ).alias("monthly_scores_json")
                    ).cache()
                    print(f"✅ last_6_months_df built: {last_6_months_df.count()}")
                except Exception as e:
                    print(f"⚠️  Could not build last_6_month_score: {e}")
                    last_6_months_df = None

            # ── trend_details ──────────────────────────────────────────────────
            trend_details_df = self.build_trend_details_df(
                history_df_raw, calculated_month, calculated_at, recent_months
            )

            # ── SCORM detection ────────────────────────────────────────────────
            print("🔍 Detecting SCORM content...")

            child_schema = StructType([
                StructField("mimeType", StringType(), True),
                StructField("children", ArrayType(StructType([
                    StructField("mimeType", StringType(), True)
                ])), True)
            ])
            hierarchy_schema = StructType([
                StructField("children", ArrayType(child_schema), True)
            ])

            exploded_df = content_hierarchy_df.withColumn(
                "hierarchy", from_json(col("hierarchy"), hierarchy_schema)
            )

            level1_df = exploded_df \
                .withColumn("level1_child", explode_outer(col("hierarchy.children"))) \
                .withColumn("lvl1_mime_match",
                            when(col("level1_child.mimeType").endswith("html-archive"), 1).otherwise(0)
                            ).groupBy("courseID") \
                .agg(F.max("lvl1_mime_match").alias("lvl1_flag"))

            level2_df = exploded_df \
                .withColumn("level1_child", explode_outer(col("hierarchy.children"))) \
                .withColumn("level2_child", explode_outer(col("level1_child.children"))) \
                .withColumn("lvl2_mime_match",
                            when(col("level2_child.mimeType").endswith("html-archive"), 1).otherwise(0)
                            ).groupBy("courseID") \
                .agg(F.max("lvl2_mime_match").alias("lvl2_flag"))

            scorm_ids = level1_df \
                .join(level2_df, on="courseID", how="outer") \
                .na.fill(0, subset=["lvl1_flag", "lvl2_flag"]) \
                .withColumn("scorm_flag", greatest(col("lvl1_flag"), col("lvl2_flag"))) \
                .filter(col("scorm_flag") == 1) \
                .select(col("courseID").alias("content_id"))

            print(f"SCORM courses found: {scorm_ids.count()}")

            # ── Live non-SCORM courses published >= 3 months ago ──────────────
            live_courses = content_df.filter(
                (col("content_status").isin("Live", "LIVE")) &
                (col("content_type").isin(["Course", "Moderated Course"])) &
                (F.to_date(col("last_published_on"), "yyyy-MM-dd") <= F.date_sub(F.current_date(), 90))
                #(F.to_date(col("last_published_on"), "yyyy-MM-dd") <= F.date_sub(F.current_date(), 90))
            ).select("content_id", "content_duration") \
                .join(scorm_ids, "content_id", "left_anti") \
                .distinct() \
                .cache()

            live_courses_ids = live_courses.select("content_id")
            print(f"✅ Live non-SCORM courses: {live_courses.count()}")

            # ── Metric 1: Completion Rate (last 90 days enrolments) ───────────
            print("📊 Metric 1: Completion Rate...")

            completion_rate_df = enrolment_df \
                .filter(
                (col("enrolled_on") >= lit(three_month_start)) &
                (col("enrolled_on") <= lit(last_month_end))
            ) \
                .join(live_courses_ids, "content_id", "inner") \
                .groupBy("content_id").agg(
                count("*").alias("total_enrolments"),
                count(when(col("first_completed_on").isNotNull(), 1)).alias("total_completions")
            ).withColumn(
                "completion_rate_value",
                F.round((col("total_completions") / col("total_enrolments")) * 100, 2)
            ).withColumn(
                "completion_rate_score",
                when(col("completion_rate_value") > 70,  5)
                .when(col("completion_rate_value") >= 50, 4)
                .when(col("completion_rate_value") >= 30, 3)
                .when(col("completion_rate_value") >= 15, 2)
                .otherwise(1)
            ).withColumn(
                "completion_rate_weighted_score",
                F.round((col("completion_rate_score") / 5.0) * W_COMPLETION, 2)
            ).select(
                "content_id", "completion_rate_value",
                "completion_rate_score", "completion_rate_weighted_score"
            )

            # ── Metric 2: Drop-off Rate (enrolments 30+ days old) ─────────────
            print("📊 Metric 2: Drop-off Rate...")

            resources_per_course = content_resource_df \
                .join(live_courses_ids, "content_id", "inner") \
                .select("content_id", "resource_id")

            total_resources_per_course = resources_per_course \
                .groupBy("content_id") \
                .agg(count("resource_id").alias("total_resources"))

            aged_enrolments = enrolment_df \
                .filter(
                (col("enrolled_on") >= lit(three_month_start)) &
                (col("enrolled_on") <= lit(last_month_end))
            ) \
                .join(live_courses_ids, "content_id", "inner") \
                .select("user_id", "content_id")

            completed_resources = consumption_df \
                .join(
                broadcast(resources_per_course),
                (consumption_df["contentid"] == resources_per_course["resource_id"]) &
                (consumption_df["courseid"]  == resources_per_course["content_id"]),
                "inner"
            ).select(
                col("userid").alias("user_id"),
                col("courseid").alias("content_id"),
                col("contentid").alias("resource_id")
            )

            completed_count_per_user = completed_resources \
                .groupBy("user_id", "content_id") \
                .agg(countDistinct("resource_id").alias("completed_resource_count"))

            dropoff_rate_df = aged_enrolments \
                .join(completed_count_per_user, ["user_id", "content_id"], "left") \
                .fillna(0, subset=["completed_resource_count"]) \
                .join(broadcast(total_resources_per_course), "content_id", "inner") \
                .withColumn(
                "is_dropoff",
                when(
                    (col("completed_resource_count") == 1) & (col("total_resources") > 1), 1
                ).otherwise(0)
            ).groupBy("content_id").agg(
                count("*").alias("total_starters"),
                F.sum("is_dropoff").alias("dropoff_count")
            ).withColumn(
                "dropoff_rate_value",
                F.round((col("dropoff_count") / col("total_starters")) * 100, 2)
            ).withColumn(
                "dropoff_rate_score",
                when(col("dropoff_rate_value") < 2,  5)
                .when(col("dropoff_rate_value") < 5,  4)
                .when(col("dropoff_rate_value") < 10, 3)
                .when(col("dropoff_rate_value") < 20, 2)
                .otherwise(1)
            ).withColumn(
                "dropoff_rate_weighted_score",
                F.round((col("dropoff_rate_score") / 5.0) * W_DROPOFF, 2)
            ).select(
                "content_id", "dropoff_rate_value",
                "dropoff_rate_score", "dropoff_rate_weighted_score"
            )

            # ── Metric 3: Average Rating (current month, PROD red flag) ───────
            print("📊 Metric 3: Average Rating...")

            current_month_ratings = ratings_df \
                .filter(
                (col("updatedon") >= lit(last_month_start)) &
                (col("updatedon") <= lit(last_month_end))
            ).join(
                live_courses_ids.withColumnRenamed("content_id", "activityid"),
                "activityid", "inner"
            )


            # Fallback to last 90 days if no ratings this month (avoids mean/SD=0)
            if current_month_ratings.limit(1).count() == 0:
                print("⚠️  No ratings this month — falling back to last 90 days")
                current_month_ratings = ratings_df \
                    .filter(col("updatedon") >= F.date_sub(lit(last_month_start), 60)) \
                    .join(
                    live_courses_ids.withColumnRenamed("content_id", "activityid"),
                    "activityid", "inner"
                )

            course_rating_stats = current_month_ratings.groupBy("activityid").agg(
                F.avg("rating").alias("avg_rating_value"),
                count("*").alias("total_ratings"),
                count(when(col("rating") <= 2, 1)).alias("low_rating_count")
            ).withColumn(
                "low_rating_pct",
                F.round((col("low_rating_count") / col("total_ratings")) * 100, 2)
            ).withColumn(
                # PROD red flag: (5% of total > 20) AND low_rating_count >= 20
                # i.e. course has >400 ratings AND at least 20 are 1-2 star
                "red_flag",
                ((col("total_ratings") * 0.05) > 20) &
                (col("low_rating_count") >= 20)
            )

            platform_stats = course_rating_stats.agg(
                F.mean("avg_rating_value").alias("platform_mean"),
                F.stddev("avg_rating_value").alias("platform_sd")
            ).collect()[0]

            platform_mean = platform_stats["platform_mean"] or 0.0
            platform_sd   = platform_stats["platform_sd"]   or 0.0
            print(f"Rating Platform Mean: {platform_mean:.4f}, SD: {platform_sd:.4f}")

            avg_rating_df = course_rating_stats \
                .withColumn(
                "avg_rating_score",
                when(col("avg_rating_value") >= platform_mean + platform_sd,        5)
                .when(col("avg_rating_value") >= platform_mean + 0.3 * platform_sd, 4)
                .when(col("avg_rating_value") >= platform_mean - 0.3 * platform_sd, 3)
                .when(col("avg_rating_value") >= platform_mean - platform_sd,       2)
                .otherwise(1)
            ).withColumn(
                "avg_rating_weighted_score",
                F.round((col("avg_rating_score") / 5.0) * W_AVG_RATING, 2)
            ).select(
                col("activityid").alias("content_id"),
                F.round(col("avg_rating_value"), 2).alias("avg_rating_value"),
                col("avg_rating_score"),
                col("avg_rating_weighted_score"),
                col("red_flag")
            )

            # ── Metric 4: Resource Length Distribution ─────────────────────────
            print("📊 Metric 4: Resource Length Distribution...")

            resource_stats = content_resource_df \
                .join(live_courses_ids, "content_id", "inner") \
                .withColumn("duration_parts", F.split(col("resource_duration"), ":")) \
                .withColumn("duration_sec",
                            when(
                                col("resource_duration").isNotNull() & (col("resource_duration") != ""),
                                col("duration_parts").getItem(0).cast("double") * 3600 +
                                col("duration_parts").getItem(1).cast("double") * 60  +
                                col("duration_parts").getItem(2).cast("double")
                            ).otherwise(lit(None).cast("double"))
                            ) \
                .groupBy("content_id").agg(
                count("resource_id").alias("total_resources"),
                count(when(col("duration_sec") < 600,  1)).alias("optimal_count"),
                count(when(col("duration_sec") > 1800, 1)).alias("outlier_count")
            ).withColumn(
                "pct_optimal",
                F.round((col("optimal_count") / col("total_resources")) * 100, 2)
            ).withColumn(
                "resource_length_score",
                when((col("pct_optimal") >= 70) & (col("outlier_count") == 0), 5)
                .when((col("pct_optimal") >= 60) & (col("outlier_count") <= 1), 4)
                .when((col("pct_optimal") >= 40) | (col("outlier_count") <= 2), 3)
                .when((col("pct_optimal") >= 20) | (col("outlier_count") >= 3), 2)
                .otherwise(1)
            ).withColumn(
                "resource_length_weighted_score",
                F.round((col("resource_length_score") / 5.0) * W_RESOURCE, 2)
            ).select(
                "content_id", "pct_optimal", "outlier_count",
                "resource_length_score", "resource_length_weighted_score"
            )

            # ── Metric 5: Feedback Sentiment ───────────────────────────────────
            print("📊 Metric 5: Feedback Sentiment...")

            if sentiment_available:
                # Parquet has content_id + score (1-5 already computed)
                sentiment_stats_df = sentiment_df \
                    .join(live_courses_ids, "content_id", "inner") \
                    .select(
                    "content_id",
                    col("score").cast("int").alias("feedback_sentiment_score")
                ).withColumn(
                    "feedback_sentiment_weighted_score",
                    F.round((col("feedback_sentiment_score") / 5.0) * W_SENTIMENT, 2)
                )
            else:
                sentiment_stats_df = spark.createDataFrame([], StructType([
                    StructField("content_id",                        StringType(), True),
                    StructField("feedback_sentiment_score",          IntegerType(), True),
                    StructField("feedback_sentiment_weighted_score", DoubleType(),  True),
                ]))
                print("⚠️  feedback_sentiment metric will be zeroed for all courses.")

            # ── Metric 6: Course Time Spent vs Expected ────────────────────────
            print("📊 Metric 6: Course Time Spent vs Expected...")

            druid_time_df = self.read_druid_time_spent(spark, config.sparkDruidRouterHost, three_month_start, calculated_month)

            content_duration_sec_df = live_courses.select(
                "content_id", col("content_duration")
            ).withColumn(
                "duration_parts", F.split(col("content_duration"), ":")
            ).withColumn(
                "expected_duration_sec",
                when(
                    col("content_duration").isNotNull() & (col("content_duration") != ""),
                    (
                            col("duration_parts").getItem(0).cast("double") * 3600 +
                            col("duration_parts").getItem(1).cast("double") * 60  +
                            col("duration_parts").getItem(2).cast("double")
                    )
                ).otherwise(lit(None).cast("double"))
            ).select("content_id", "expected_duration_sec")

            if druid_time_df is not None:
                time_spent_df = druid_time_df \
                    .join(content_duration_sec_df, "content_id", "inner") \
                    .withColumn(
                    "time_spent_value",
                    when(
                        col("expected_duration_sec").isNotNull() & (col("expected_duration_sec") > 0),
                        F.round((col("avg_actual_time_spent") / col("expected_duration_sec")) * 100, 2)
                    ).otherwise(lit(None).cast("double"))
                ).withColumn(
                    "time_spent_score",
                    when(col("time_spent_value").between(80, 120), 5)
                    .when(col("time_spent_value").between(60, 79)  | col("time_spent_value").between(121, 140), 4)
                    .when(col("time_spent_value").between(40, 59)  | col("time_spent_value").between(141, 160), 3)
                    .when(col("time_spent_value").between(20, 39)  | col("time_spent_value").between(161, 200), 2)
                    .otherwise(1)
                ).withColumn(
                    "time_spent_weighted_score",
                    F.round((col("time_spent_score") / 5.0) * W_TIME_SPENT, 2)
                ).select(
                    "content_id",
                    col("avg_actual_time_spent").alias("time_spent_actual_sec"),
                    col("expected_duration_sec").alias("time_spent_expected_sec"),
                    "time_spent_value", "time_spent_score", "time_spent_weighted_score"
                )
            else:
                time_spent_df = spark.createDataFrame([], StructType([
                    StructField("content_id",                StringType(), True),
                    StructField("time_spent_actual_sec",     DoubleType(),  True),
                    StructField("time_spent_expected_sec",   DoubleType(),  True),
                    StructField("time_spent_value",          DoubleType(),  True),
                    StructField("time_spent_score",          IntegerType(), True),
                    StructField("time_spent_weighted_score", DoubleType(),  True),
                ]))

            # ── Set checkpoint dir ────────────────────────────────────────────
            spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")

            # ── Join all metrics + health score ────────────────────────────────
            print("🔗 Joining all metrics...")

            all_metrics_df = live_courses_ids \
                .join(completion_rate_df,  "content_id", "left") \
                .join(dropoff_rate_df,     "content_id", "left") \
                .join(avg_rating_df,       "content_id", "left") \
                .join(resource_stats,      "content_id", "left") \
                .join(sentiment_stats_df,  "content_id", "left") \
                .join(time_spent_df,       "content_id", "left") \
                .fillna(0, subset=[
                "completion_rate_weighted_score",    "completion_rate_score",    "completion_rate_value",
                "dropoff_rate_weighted_score",       "dropoff_rate_score",       "dropoff_rate_value",
                "avg_rating_weighted_score",         "avg_rating_score",         "avg_rating_value",
                "resource_length_weighted_score",    "resource_length_score",    "pct_optimal",    "outlier_count",
                "feedback_sentiment_weighted_score", "feedback_sentiment_score",
                "time_spent_weighted_score",         "time_spent_score",         "time_spent_value",
                "time_spent_actual_sec",             "time_spent_expected_sec"
            ]) \
                .withColumn(
                "dynamic_raw",
                col("completion_rate_weighted_score")    +
                col("dropoff_rate_weighted_score")       +
                col("avg_rating_weighted_score")         +
                col("feedback_sentiment_weighted_score") +
                col("time_spent_weighted_score")
            ).withColumn(
                "static_raw",
                col("resource_length_weighted_score")
            ).withColumn(
                "total_health_score",
                F.round(
                    ((col("dynamic_raw") / D_DYNAMIC_MAX) * 50) +
                    ((col("static_raw")  / D_STATIC_MAX)  * 50),
                    2
                )
            ).cache()

            # ── diff_percentage ────────────────────────────────────────────────
            if last_month_score_df is not None:
                all_metrics_df = all_metrics_df.join(last_month_score_df, "content_id", "left")
            else:
                all_metrics_df = all_metrics_df.withColumn("last_month_health_score", lit(None).cast("double"))

            all_metrics_df = all_metrics_df.withColumn(
                "diff_percentage",
                when(
                    col("last_month_health_score").isNotNull() & (col("last_month_health_score") != 0),
                    F.round(
                        ((col("total_health_score") - col("last_month_health_score"))
                         / col("last_month_health_score")) * 100,
                        1
                    )
                ).otherwise(lit(None).cast("double"))
            ).cache()

            all_metrics_df.count()
            print(f"✅ Total courses computed: {all_metrics_df.count()}")

            # ── Join last_6_months + trend_details ────────────────────────────
            if last_6_months_df is not None:
                all_metrics_df = all_metrics_df.join(last_6_months_df, "content_id", "left")
            else:
                all_metrics_df = all_metrics_df.withColumn("monthly_scores_json", lit(None).cast("string"))

            if trend_details_df is not None:
                all_metrics_df = all_metrics_df.join(trend_details_df, "content_id", "left")
            else:
                all_metrics_df = all_metrics_df.withColumn("trend_details_json", lit(None).cast("string"))

            # ── Prepare Redis DF ───────────────────────────────────────────────
            print("📝 Preparing Redis dataframe...")

            metrics_redis_df = all_metrics_df.select(
                "content_id",

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["completion_rate"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["completion_rate"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["completion_rate"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["completion_rate"]["type"]).alias("type"),
                    col("completion_rate_score").cast("int").alias("score"),
                    col("completion_rate_value").cast("double").alias("value"),
                    col("completion_rate_weighted_score").cast("double").alias("weighted_score")
                )).alias("completion_rate_json"),

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["dropoff_rate"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["dropoff_rate"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["dropoff_rate"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["dropoff_rate"]["type"]).alias("type"),
                    col("dropoff_rate_score").cast("int").alias("score"),
                    col("dropoff_rate_value").cast("double").alias("value"),
                    col("dropoff_rate_weighted_score").cast("double").alias("weighted_score")
                )).alias("dropoff_rate_json"),

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["avg_rating"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["avg_rating"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["avg_rating"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["avg_rating"]["type"]).alias("type"),
                    col("avg_rating_score").cast("int").alias("score"),
                    col("avg_rating_value").cast("double").alias("value"),
                    col("avg_rating_weighted_score").cast("double").alias("weighted_score"),
                    F.coalesce(col("red_flag"), F.lit(False)).cast("boolean").alias("red_flag")
                )).alias("avg_rating_json"),

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["resource_length"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["resource_length"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["resource_length"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["resource_length"]["type"]).alias("type"),
                    col("resource_length_score").cast("int").alias("score"),
                    col("pct_optimal").cast("double").alias("value"),
                    col("resource_length_weighted_score").cast("double").alias("weighted_score"),
                    col("pct_optimal").cast("double").alias("pct_optimal"),
                    col("outlier_count").cast("int").alias("outliers")
                )).alias("resource_length_json"),

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["feedback_sentiment"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["feedback_sentiment"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["feedback_sentiment"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["feedback_sentiment"]["type"]).alias("type"),
                    col("feedback_sentiment_score").cast("int").alias("score"),
                    F.lit(0.0).cast("double").alias("value"),
                    col("feedback_sentiment_weighted_score").cast("double").alias("weighted_score")
                )).alias("feedback_sentiment_json"),

                F.to_json(F.struct(
                    F.lit(METRIC_CONFIG["time_spent"]["name"]).alias("name"),
                    F.lit(METRIC_CONFIG["time_spent"]["overview"]).alias("overview"),
                    F.lit(METRIC_CONFIG["time_spent"]["maxWeight"]).alias("maxWeight"),
                    F.lit(METRIC_CONFIG["time_spent"]["type"]).alias("type"),
                    col("time_spent_score").cast("int").alias("score"),
                    col("time_spent_value").cast("double").alias("value"),
                    col("time_spent_weighted_score").cast("double").alias("weighted_score"),
                    col("time_spent_actual_sec").cast("double").alias("actual_time_sec"),
                    col("time_spent_expected_sec").cast("double").alias("expected_time_sec")
                )).alias("time_spent_json"),

                F.to_json(F.struct(
                    col("total_health_score").cast("double").alias("total_health_score"),
                    col("diff_percentage").cast("double").alias("diff_percentage"),
                    F.coalesce(col("red_flag"), F.lit(False)).cast("boolean").alias("red_flag"),
                    F.lit(calculated_at).alias("calculated_at")
                ), {"ignoreNullFields": "false"}).alias("health_score_json"),

                when(
                    col("monthly_scores_json").isNotNull(),
                    F.concat(
                        F.regexp_replace(col("monthly_scores_json"), r"\}$", ""),
                        F.lit(f',"calculated_at":"{calculated_at}"}}')
                    )
                ).otherwise(
                    F.lit(f'{{"calculated_at":"{calculated_at}"}}')
                ).alias("last_6_month_score_json"),

                F.coalesce(
                    col("trend_details_json"),
                    F.lit(f'{{"calculated_at":"{calculated_at}"}}')
                ).alias("trend_details_json")
            )

            # ── Wide → long ────────────────────────────────────────────────────
            def to_subkey_df(base_df, sub_key, json_col):
                return base_df.select(
                    "content_id",
                    F.lit(sub_key).alias("sub_key"),
                    col(json_col).alias("metrics_json")
                )

            metrics_redis_final_df = (
                to_subkey_df(metrics_redis_df, "completion_rate",    "completion_rate_json")
                .unionByName(to_subkey_df(metrics_redis_df, "dropoff_rate",       "dropoff_rate_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "avg_rating",         "avg_rating_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "resource_length",    "resource_length_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "feedback_sentiment", "feedback_sentiment_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "time_spent",         "time_spent_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "health_score",       "health_score_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "last_6_month_score", "last_6_month_score_json"))
                .unionByName(to_subkey_df(metrics_redis_df, "trend_details",      "trend_details_json"))
            )

            print(f"✅ Redis records prepared: {metrics_redis_final_df.count()}")

            # ── Optimized Redis Write ──────────────────────────────────────────
            print("📝 Writing to Redis...")

            redis_ready_df = metrics_redis_final_df \
                .groupBy("content_id") \
                .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(col("sub_key"), col("metrics_json")))
                ).alias("metrics_map")
            )

            rows = redis_ready_df.collect()
            pipe = r.pipeline(transaction=False)
            BATCH_SIZE = 500
            counter = 0

            for row in rows:
                pipe.hset(
                    f"course_metrics:{row['content_id']}",
                    mapping=row["metrics_map"]
                )
                counter += 1
                if counter % BATCH_SIZE == 0:
                    pipe.execute()
                    pipe = r.pipeline(transaction=False)
                    print(f"✅ Written {counter} courses")

            pipe.execute()
            print(f"✅ Redis write complete. Total courses: {counter}")

            # ── Write to Parquet ──────────────────────────────────────────────
            print("📝 Writing to parquet...")

            parquet_df = all_metrics_df.select(
                col("content_id"),
                col("completion_rate_value").cast("double"),
                col("completion_rate_weighted_score").cast("double"),
                col("dropoff_rate_value").cast("double"),
                col("dropoff_rate_weighted_score").cast("double"),
                col("avg_rating_value").cast("double"),
                col("avg_rating_weighted_score").cast("double"),
                col("pct_optimal").cast("double").alias("resource_length_value"),
                col("resource_length_weighted_score").cast("double"),
                col("feedback_sentiment_weighted_score").cast("double").alias("sentiment_weighted_score"),
                col("time_spent_value").cast("double"),
                col("time_spent_weighted_score").cast("double"),
                col("total_health_score").cast("double"),
                F.coalesce(col("red_flag"), F.lit(False)).cast("boolean").alias("health_score_red_flag"),
                F.lit(calculated_at).alias("calculated_at")
            )

            parquet_df.write.mode("overwrite") \
                .parquet("/home/analytics/pyspark/warehouse/content_health_score")
            print(f"✅ Parquet written: {parquet_df.count()} courses")


            # ── Prepare PostgreSQL DF ──────────────────────────────────────────
            print("📝 Preparing PostgreSQL dataframe...")

            postgres_df = all_metrics_df.select(
                col("content_id"),
                F.lit(calculated_month).alias("calculated_month"),
                F.to_json(
                    F.struct(
                        F.struct(
                            col("completion_rate_score").alias("score"),
                            col("completion_rate_value").alias("value"),
                            col("completion_rate_weighted_score").alias("weighted_score")
                        ).alias("completion_rate"),
                        F.struct(
                            col("dropoff_rate_score").alias("score"),
                            col("dropoff_rate_value").alias("value"),
                            col("dropoff_rate_weighted_score").alias("weighted_score")
                        ).alias("dropoff_rate"),
                        F.struct(
                            col("avg_rating_score").alias("score"),
                            col("avg_rating_value").alias("value"),
                            col("avg_rating_weighted_score").alias("weighted_score")
                        ).alias("avg_rating"),
                        F.struct(
                            col("resource_length_score").alias("score"),
                            col("pct_optimal").alias("pct_optimal"),
                            col("outlier_count").alias("outliers"),
                            col("resource_length_weighted_score").alias("weighted_score")
                        ).alias("resource_length"),
                        F.struct(
                            col("feedback_sentiment_score").alias("score"),
                            F.lit(0.0).alias("value"),
                            col("feedback_sentiment_weighted_score").alias("weighted_score")
                        ).alias("feedback_sentiment"),
                        F.struct(
                            col("time_spent_score").alias("score"),
                            col("time_spent_value").alias("value"),
                            col("time_spent_actual_sec").alias("actual_time_sec"),
                            col("time_spent_expected_sec").alias("expected_time_sec"),
                            col("time_spent_weighted_score").alias("weighted_score")
                        ).alias("time_spent"),
                        col("total_health_score").alias("total_health_score"),
                        F.coalesce(col("red_flag"), F.lit(False)).alias("red_flag")
                    )
                ).alias("metrics")
            )

            # ── Write to PostgreSQL ────────────────────────────────────────────
            print("📝 Writing history to PostgreSQL...")
            self.write_postgres_table(
                postgres_df, postgres_url,
                "course_health_metrics_history",
                config.dwPostgresUsername, config.dwPostgresCredential
            )
            print("✅ PostgreSQL write complete.")

            # ── Unpersist ──────────────────────────────────────────────────────
            enrolment_df.unpersist()
            consumption_df.unpersist()
            content_df.unpersist()
            ratings_df.unpersist()
            if sentiment_df is not None:
                sentiment_df.unpersist()
            content_hierarchy_df.unpersist()
            live_courses.unpersist()
            all_metrics_df.unpersist()
            if last_month_score_df is not None:
                last_month_score_df.unpersist()
            if last_6_months_df is not None:
                last_6_months_df.unpersist()
            if trend_details_df is not None:
                trend_details_df.unpersist()

        except Exception as e:
            print(f"❌ Error in ContentHealthMetricsModel: {str(e)}")
            raise

    def write_postgres_table(self, df, url: str, table: str, username: str, password: str, mode: str = "overwrite"):
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()


def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("Content Health Metrics Model") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.executor.memory", "180g") \
        .config("spark.driver.memory", "60g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "10g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def main():
    start_time = datetime.now()
    print(f"[START] ContentHealthMetricsModel started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    config_dict = get_environment_config()
    config      = create_config(config_dict)
    spark       = create_spark_session_with_packages(config)

    model = ContentHealthMetricsModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    print(f"[END] ContentHealthMetricsModel completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    spark.stop()


if __name__ == "__main__":
    main()
