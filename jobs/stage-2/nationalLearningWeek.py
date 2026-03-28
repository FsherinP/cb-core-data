import findspark

findspark.init()

import time
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dfutil.utils.redis import Redis
from dfutil.utils import utils
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config


class NationalLearningWeekLeaderboardModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.NationalLearningWeekLeaderboardModel"

    def name(self):
        return "NationalLearningWeekLeaderboardModel"

    # ------------------------------------------------------------------
    # HELPER: HH:MM:SS -> decimal hours
    # ------------------------------------------------------------------
    def duration_to_hours(self, colname: str):
        parts = F.split(F.col(colname), ":")
        return F.when(
            F.col(colname).rlike(r"^\d{1,2}:\d{2}:\d{2}$"),
            parts.getItem(0).cast("double")
            + parts.getItem(1).cast("double") / 60.0
            + parts.getItem(2).cast("double") / 3600.0
        ).otherwise(F.lit(0.0))

    # ------------------------------------------------------------------
    # HELPER: parse bucket string -> list of dicts
    #   format: "min-max-SIZE,...,above N-SIZE"
    # ------------------------------------------------------------------
    def parse_bucket_string(self, bucket_str):
        buckets = []
        for segment in bucket_str.split(","):
            segment = segment.strip()
            if segment.lower().startswith("above"):
                rest = segment[len("above"):].strip()
                min_val, size = rest.rsplit("-", 1)
                buckets.append({
                    "size": size.strip().upper(),
                    "min": int(min_val.strip()),
                    "max": None
                })
            else:
                parts = segment.rsplit("-", 1)
                size = parts[1].strip().upper()
                range_part = parts[0].strip().split("-")
                buckets.append({
                    "size": size,
                    "min": int(range_part[0].strip()),
                    "max": int(range_part[1].strip())
                })
        return buckets

    # ------------------------------------------------------------------
    # HELPER: build bucket size column expression from config list
    #   operates on column named `user_count`
    # ------------------------------------------------------------------
    def build_bucket_expr(self, bucket_config):
        sorted_buckets = sorted(bucket_config, key=lambda b: b["min"], reverse=True)
        expr = None
        for bucket in sorted_buckets:
            size = bucket["size"]
            min_val = bucket["min"]
            max_val = bucket.get("max")

            if max_val is None:
                condition = F.col("user_count") >= F.lit(min_val)
            else:
                condition = (
                        (F.col("user_count") >= F.lit(min_val)) &
                        (F.col("user_count") <= F.lit(max_val))
                )

            if expr is None:
                expr = F.when(condition, F.lit(size))
            else:
                expr = expr.when(condition, F.lit(size))

        # Uppercase "XS" — consistent with all parsed bucket labels (all uppercased above).
        # The original code had F.lit("xs") (lowercase) which created a phantom partition
        # separate from the real "XS" bucket, causing every fallthrough org to get rank=1.
        return expr.otherwise(F.lit("XS"))

    # ------------------------------------------------------------------
    # MAIN
    # ------------------------------------------------------------------
    def process_data(self, spark, config):
        try:
            start_time = time.time()

            # -----------------------------------------------------------
            # CONFIG
            # -----------------------------------------------------------
            nlw_start = config.nationalLearningWeekStart  # "YYYY-MM-DD HH:MM:SS"
            nlw_end = config.nationalLearningWeekEnd  # "YYYY-MM-DD HH:MM:SS"
            print("nlw_start", nlw_start)
            print("nlw_end", nlw_end)

            bucket_config = self.parse_bucket_string(config.sizeBucketString)
            state_bucket_config = self.parse_bucket_string(config.stateSizeBucketString)
            state_names_list = [s.strip() for s in config.nlwStatesList.split(",")]
            state_universe_map = config.stateUniverseMap  # dict

            app_postgres_url = (
                f"jdbc:postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"
            )

            EVENT_ENROLMENT_PARQUET = f"{config.baseCachePath}/eventEnrolmentDetails"

            # -----------------------------------------------------------
            # 1. READ SOURCE DATA
            # -----------------------------------------------------------

            orgHierDF = (
                spark.read.parquet(f"{config.warehouseReportDir}/{config.dwOrgTable}")
                .select("mdo_id", "mdo_name", "ministry_id", "department_id")
                .dropDuplicates(["mdo_id"])
            )

            userDF = (
                spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
                .withColumnRenamed("userID", "user_id")
                .withColumnRenamed("userOrgID", "mdo_id")
                .select("user_id", "mdo_id", "fullName", "designation", "userProfileImgUrl")
                .dropDuplicates(["user_id"])
            )

            karmaPerUserDF = (
                spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE)
                .filter(
                    (F.col("credit_date") >= F.lit(nlw_start)) &
                    (F.col("credit_date") <= F.lit(nlw_end))
                )
                .groupBy(F.col("userid").alias("user_id"))
                .agg(
                    F.sum("points").alias("user_total_points"),
                    F.max("credit_date").alias("last_credit_date")
                )
            )
            karmaPerUserDF.filter(F.col("user_id") == 'a1a6e5ce-9ca9-4b96-9799-69cac0d1e38b').show()

            contentEnrolDF = (
                spark.read.parquet(
                    ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE
                )
                .withColumnRenamed("userID", "user_id")
                .withColumnRenamed("certificateID", "certificate_id")
            )

            contentMasterDF = (
                spark.read.parquet(
                    ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE
                )
                .filter(F.col("content_sub_type").isin("Course", "Moderated Course", "External Content"))
                .select("content_id", "content_duration")
            )

            eventEnrolDF = spark.read.parquet(EVENT_ENROLMENT_PARQUET)

            eventMasterDF = (
                spark.read.parquet(ParquetFileConstants.EVENT_PARQUET_FILE)
                .withColumnRenamed("duration", "event_complete_duration")
                .select("event_id", "event_complete_duration")
            )

            contentCertDF = (
                contentEnrolDF
                .filter(
                    (F.col("first_completed_on") >= F.lit(nlw_start)) &
                    (F.col("first_completed_on") <= F.lit(nlw_end)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(contentMasterDF, "content_id", "inner")
                .select("user_id", "certificate_id")
            )

            eventCertDF = (
                eventEnrolDF
                .filter(
                    (F.col("completed_on_datetime") >= F.lit(nlw_start)) &
                    (F.col("completed_on_datetime") <= F.lit(nlw_end)) &
                    F.col("certificate_id").isNotNull()
                )
                .select("user_id", "certificate_id")
            )

            totalCertificatesDF = (
                contentCertDF
                .unionByName(eventCertDF)
                .groupBy("user_id")
                .agg(F.countDistinct("certificate_id").alias("total_certificates"))
            )

            badgesDF = (
                spark.read.parquet(ParquetFileConstants.GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE)
                .withColumnRenamed("userID", "user_id")
                .filter(
                    (F.col("badge_issued_on") >= F.lit(nlw_start)) &
                    (F.col("badge_issued_on") <= F.lit(nlw_end)) &
                    F.col("enrolment_badge_id").isNotNull()
                )
                .groupBy("user_id")
                .agg(F.count("enrolment_badge_id").alias("total_badges"))
            )

            # -----------------------------------------------------------
            # 2. PER-USER LEARNING HOURS IN THE NLW WINDOW
            # -----------------------------------------------------------

            contentHoursDF = (
                contentEnrolDF
                .filter(
                    (F.col("first_completed_on") >= F.lit(nlw_start)) &
                    (F.col("first_completed_on") <= F.lit(nlw_end)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(contentMasterDF, "content_id", "inner")
                .withColumn("content_duration_hours", self.duration_to_hours("content_duration"))
                .groupBy("user_id")
                .agg(
                    F.sum(
                        F.coalesce(F.col("content_duration_hours"), F.lit(0.0))
                    ).alias("content_learning_hours")
                )
            )

            eventHoursDF = (
                eventEnrolDF
                .filter(
                    (F.col("completed_on_datetime") >= F.lit(nlw_start)) &
                    (F.col("completed_on_datetime") <= F.lit(nlw_end)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(eventMasterDF, "event_id", "left")
                .withColumn("event_duration_hours", self.duration_to_hours("event_complete_duration"))
                .groupBy("user_id")
                .agg(
                    F.sum(
                        F.coalesce(F.col("event_duration_hours"), F.lit(0.0))
                    ).alias("event_learning_hours")
                )
            )

            userLearningHoursDF = (
                contentHoursDF
                .join(eventHoursDF, "user_id", "full_outer")
                .select(
                    "user_id",
                    F.round(
                        F.coalesce(F.col("content_learning_hours"), F.lit(0.0)) +
                        F.coalesce(F.col("event_learning_hours"), F.lit(0.0)),
                        2
                    ).alias("total_learning_hours")
                )
            )

            # -----------------------------------------------------------
            # 3. STATE LEADERBOARD
            # -----------------------------------------------------------

            stateOrgMapDF = (
                orgHierDF
                .filter(F.col("mdo_name").isin(state_names_list))
                .select(
                    F.col("mdo_id").alias("state_id"),
                    F.col("mdo_name").alias("state_name")
                )
            )

            print(f"[INFO] States matched from org hierarchy: {stateOrgMapDF.count()}")
            stateOrgMapDF.show(40, truncate=False)

            childrenDF = (
                orgHierDF
                .join(stateOrgMapDF, orgHierDF["ministry_id"] == stateOrgMapDF["state_id"], "inner")
                .select("mdo_id", "state_id", "state_name")
            )

            stateItselfDF = (
                stateOrgMapDF
                .select(
                    F.col("state_id").alias("mdo_id"),
                    "state_id",
                    "state_name"
                )
            )

            stateMdoDF = (
                childrenDF
                .unionByName(stateItselfDF)
                .dropDuplicates(["mdo_id", "state_id"])
            )

            stateUsersDF = (
                userDF.select("user_id", "mdo_id")
                .join(stateMdoDF, "mdo_id", "inner")
                .select("user_id", "state_id", "state_name")
            )

            stateStatsDF = (
                stateUsersDF
                .join(karmaPerUserDF, "user_id", "left")
                .join(userLearningHoursDF, "user_id", "left")
                .groupBy("state_id", "state_name")
                .agg(
                    F.countDistinct("user_id").alias("actual_user_count"),
                    F.coalesce(
                        F.sum("user_total_points"), F.lit(0).cast("long")
                    ).alias("total_points"),
                    F.coalesce(
                        F.round(F.sum("total_learning_hours"), 2), F.lit(0.0)
                    ).alias("total_learning_hours")
                )
            )

            if state_universe_map:
                universeDF = spark.createDataFrame(
                    [(k, v) for k, v in state_universe_map.items()],
                    ["state_name", "universe_count"]
                )
                stateStatsDF = (
                    stateStatsDF
                    .join(universeDF, "state_name", "left")
                    .withColumn(
                        "user_count",
                        F.when(
                            F.col("universe_count").isNotNull(),
                            F.col("universe_count")
                        ).otherwise(F.col("actual_user_count"))
                    )
                    .drop("universe_count")
                )
            else:
                stateStatsDF = stateStatsDF.withColumn("user_count", F.col("actual_user_count"))

            stateStatsDF = stateStatsDF.withColumn(
                "per_capita_kp",
                F.when(
                    F.col("user_count") > 0,
                    F.round(
                        F.col("total_points").cast("double") /
                        F.col("user_count").cast("double"),
                        4
                    )
                ).otherwise(F.lit(0.0))
            )

            stateStatsDF.select(
                "state_name", "actual_user_count", "user_count", "total_points", "per_capita_kp"
            ).show(truncate=False)

            # FIX: materialize size + is_state into a concrete intermediate DataFrame
            # BEFORE defining the window and calling dense_rank().
            # When size/is_state are added in the SAME withColumn chain as row_num,
            # Spark's lazy resolution may evaluate partition keys as null for all rows,
            # collapsing every org into one null-null partition -> all get row_num=1.
            # Breaking into two steps forces the columns to be fully resolved first.
            stateSizedDF = (
                stateStatsDF
                .withColumn("size", self.build_bucket_expr(state_bucket_config))
                .withColumn("is_state", F.lit(True))
            )

            wStateRank = (
                Window
                .partitionBy("is_state", "size")
                .orderBy(F.desc("per_capita_kp"))
            )

            stateLeaderboardFinalDF = (
                stateSizedDF
                .withColumn("row_num", F.dense_rank().over(wStateRank))
                .select(
                    F.col("state_id").alias("org_id"),
                    F.col("state_name").alias("org_name"),
                    F.col("user_count").alias("total_users"),
                    "size",
                    "total_points",
                    "per_capita_kp",
                    "is_state",
                    "row_num",
                    F.lit(None).cast("string").alias("last_credit_date")
                )
            )

            # -----------------------------------------------------------
            # 4. CENTRE LEADERBOARD
            # -----------------------------------------------------------

            state_ids_list = [r["state_id"] for r in stateOrgMapDF.collect()]

            centreUsersDF = (
                userDF.select("user_id", "mdo_id")
                .filter(~F.col("mdo_id").isin(state_ids_list))
            )

            centreStatsDF = (
                centreUsersDF
                .join(karmaPerUserDF, "user_id", "left")
                .join(userLearningHoursDF, "user_id", "left")
                .groupBy("mdo_id")
                .agg(
                    F.countDistinct("user_id").alias("user_count"),
                    F.coalesce(
                        F.sum("user_total_points"), F.lit(0).cast("long")
                    ).alias("total_points"),
                    F.coalesce(
                        F.round(F.sum("total_learning_hours"), 2), F.lit(0.0)
                    ).alias("total_learning_hours")
                )
                .withColumn(
                    "per_capita_kp",
                    F.when(
                        F.col("user_count") > 0,
                        F.round(
                            F.col("total_points").cast("double") /
                            F.col("user_count").cast("double"),
                            4
                        )
                    ).otherwise(F.lit(0.0))
                )
            )

            orgNameDF = (
                orgHierDF
                .select(
                    F.col("mdo_id"),
                    F.col("mdo_name").alias("org_name")
                )
            )

            # FIX: same two-step pattern — materialize size + is_state first,
            # then define the window and apply dense_rank in a separate chain.
            centreSizedDF = (
                centreStatsDF
                .join(orgNameDF, "mdo_id", "left")
                .withColumn(
                    "org_name",
                    F.coalesce(F.col("org_name"), F.col("mdo_id"))
                )
                .withColumn("size", self.build_bucket_expr(bucket_config))
                .withColumn("is_state", F.lit(False))
            )

            wCentreRank = (
                Window
                .partitionBy("is_state", "size")
                .orderBy(F.desc("per_capita_kp"))
            )

            centreLeaderboardFinalDF = (
                centreSizedDF
                .withColumn("row_num", F.dense_rank().over(wCentreRank))
                .select(
                    F.col("mdo_id").alias("org_id"),
                    "org_name",
                    F.col("user_count").alias("total_users"),
                    "size",
                    "total_points",
                    "per_capita_kp",
                    "is_state",
                    "row_num",
                    F.lit(None).cast("string").alias("last_credit_date")
                )
            )

            # -----------------------------------------------------------
            # 5. UNION STATES + CENTRES INTO SINGLE LEADERBOARD
            # -----------------------------------------------------------
            finalLeaderboardDF = stateLeaderboardFinalDF.unionByName(centreLeaderboardFinalDF)

            # -----------------------------------------------------------
            # 6. USER-LEVEL STATS
            # -----------------------------------------------------------
            wUserRank = (
                Window
                .partitionBy("org_id")
                .orderBy(F.desc("total_points"), F.asc("last_credit_date"))
            )

            userStatsFinalDF = (
                userDF
                .select("user_id", "mdo_id", "fullName", "designation", "userProfileImgUrl")
                .join(karmaPerUserDF, "user_id", "left")
                .join(userLearningHoursDF, "user_id", "left")
                .join(totalCertificatesDF, "user_id", "left")
                .join(badgesDF, "user_id", "left")
                .select(
                    F.col("user_id").alias("userid"),
                    F.col("mdo_id").alias("org_id"),
                    F.col("fullName").alias("fullname"),
                    F.col("designation"),
                    F.col("userProfileImgUrl").alias("profile_image"),
                    F.coalesce(F.col("user_total_points"), F.lit(0)).alias("total_points"),
                    F.coalesce(F.col("total_learning_hours"), F.lit(0.0)).alias("total_learning_hours"),
                    F.coalesce(F.col("total_certificates"), F.lit(0)).alias("count"),
                    F.coalesce(F.col("total_badges"), F.lit(0)).alias("total_badges"),
                    F.col("last_credit_date"),
                    F.lit(None).cast("integer").alias("row_num")
                )
                .withColumn("rank", F.dense_rank().over(wUserRank))
            )

            # -----------------------------------------------------------
            # 7. WRITE BOTH TABLES
            # -----------------------------------------------------------
            finalLeaderboardDF.show(15, truncate=False)
            userStatsFinalDF.show(20, truncate=False)

            # utils.writeToCassandra(finalLeaderboardDF, config.cassandraUserKeyspace, "nlw_mdo_leaderboard")
            '''self.write_postgres_table(
                userStatsFinalDF,
                app_postgres_url,
                "nlw_user_leaderboard",
                config.appPostgresUsername,
                config.appPostgresCredential
            )'''

            # -----------------------------------------------------------
            # 8. LAST-24-HOUR STATS
            # -----------------------------------------------------------
            '''cert_count_str, learning_hours_str, org_learning_hoursDF = (
                self.get_last_24h_stats(
                    spark,
                    contentEnrolDF,
                    contentMasterDF,
                    eventEnrolDF,
                    eventMasterDF,
                    userDF,
                )
            )

            certificates_yesterday_df = spark.createDataFrame([("across:yesterday", cert_count_str)], ["across:yesterday", "cert_count_str"])
            Redis.dispatchDataFrame("lhp_certifications", certificates_yesterday_df, "across:yesterday", "cert_count_str", conf=config)

            # Learning hours yesterday
            learning_hours_yesterday_df = spark.createDataFrame([("across:yesterday", learning_hours_str)], ["across:yesterday", "learning_hours_str"])
            Redis.dispatchDataFrame("lhp_learningHours", learning_hours_yesterday_df, "across:yesterday", "learning_hours_str", conf=config)

            # Org-wise learning hours yesterday
            # org_learning_hoursDF already has org_id and learning_hours columns
            org_learning_hoursDF = org_learning_hoursDF.withColumn("redis_key", F.concat(F.col("org_id"), F.lit(":yesterday")))
            Redis.dispatchDataFrame("lhp_learningHours", org_learning_hoursDF, "redis_key", "learning_hours", conf=config)
            print("\n📊 24-Hour Stats:")
            print(f"  {cert_count_str}")
            print(f"  {learning_hours_str}")
            print("\n  Org-wise learning hours (last 24h):")
            org_learning_hoursDF.show(truncate=False)'''
            yesterday_cert_str, yesterday_hours_str, today_cert_str, today_hours_str = (
                self.get_last_24h_stats(
                    spark=spark,
                    contentEnrolDF=contentEnrolDF,
                    contentMasterDF=contentMasterDF,
                    eventEnrolDF=eventEnrolDF,
                    eventMasterDF=eventMasterDF,
                    userDF=userDF,
                    config=config,
                    redis_client=Redis
                )
            )

            print("\n📊 24-Hour Stats:")
            print(f"  across:yesterday certs = {yesterday_cert_str}")
            print(f"  across:yesterday hours = {yesterday_hours_str}")
            print(f"  across:today certs     = {today_cert_str}")
            print(f"  across:today hours     = {today_hours_str}")

            total_time = time.time() - start_time
            print(
                f"\n✅ NLW Leaderboard completed in "
                f"{total_time:.2f}s ({total_time / 60:.1f} min)"
            )

        except Exception as e:
            print(f"❌ Error in NLW Leaderboard processing: {str(e)}")
            raise

    # ------------------------------------------------------------------
    # HELPER: last-24-hour stats
    #   Returns:
    #     cert_count_str       : str  e.g. "Total certificates in last 24h: 142"
    #     learning_hours_str   : str  e.g. "Total learning hours in last 24h: 318.75"
    #     org_learning_hoursDF : DataFrame[org_id, learning_hours]
    # ------------------------------------------------------------------
    def get_last_24h_stats(self,
                           spark,
                           contentEnrolDF,
                           contentMasterDF,
                           eventEnrolDF,
                           eventMasterDF,
                           userDF,
                           config,
                           redis_client  # pass your redis connection
                           ):
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)

        # ----------------------------------------------------------
        # Define time windows
        # today window:     yesterday 00:00 UTC → today 00:00 UTC
        # yesterday window: day-before 00:00 UTC → yesterday 00:00 UTC
        # ----------------------------------------------------------
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        day_before_start = today_start - timedelta(days=2)

        today_start_str = today_start.strftime("%Y-%m-%d %H:%M:%S")
        yesterday_start_str = yesterday_start.strftime("%Y-%m-%d %H:%M:%S")
        day_before_start_str = day_before_start.strftime("%Y-%m-%d %H:%M:%S")

        print(f"[Stats] yesterday window : {day_before_start_str} -> {yesterday_start_str}")
        print(f"[Stats] today window     : {yesterday_start_str} -> {today_start_str}")

        # ----------------------------------------------------------
        # Check if across:today already exists in Redis
        # If yes  → promote it to yesterday, calculate fresh today
        # If no   → first run, calculate both fresh
        # ----------------------------------------------------------
        existing_today_certs = redis_client.getMapField("lhp_certifications", "across:today", conf=config)
        existing_today_hours = redis_client.getMapField("lhp_learningHours", "across:today", conf=config)

        def compute_stats(start_str, end_str):
            """Returns (cert_count_str, learning_hours_str, org_learning_hoursDF)"""
            content_cert = (
                contentEnrolDF
                .filter(
                    (F.col("first_completed_on") >= F.lit(start_str)) &
                    (F.col("first_completed_on") < F.lit(end_str)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(contentMasterDF, "content_id", "inner")
                .select("certificate_id")
            )
            event_cert = (
                eventEnrolDF
                .filter(
                    (F.col("completed_on_datetime") >= F.lit(start_str)) &
                    (F.col("completed_on_datetime") < F.lit(end_str)) &
                    F.col("certificate_id").isNotNull()
                )
                .select("certificate_id")
            )
            cert_count = (
                content_cert.unionByName(event_cert)
                .select(F.countDistinct("certificate_id").alias("cnt"))
                .collect()[0]["cnt"]
            )

            content_hours = (
                contentEnrolDF
                .filter(
                    (F.col("first_completed_on") >= F.lit(start_str)) &
                    (F.col("first_completed_on") < F.lit(end_str)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(contentMasterDF, "content_id", "inner")
                .withColumn("duration_hours", self.duration_to_hours("content_duration"))
                .select("user_id", F.coalesce(F.col("duration_hours"), F.lit(0.0)).alias("hours"))
            )
            event_hours = (
                eventEnrolDF
                .filter(
                    (F.col("completed_on_datetime") >= F.lit(start_str)) &
                    (F.col("completed_on_datetime") < F.lit(end_str)) &
                    F.col("certificate_id").isNotNull()
                )
                .join(eventMasterDF, "event_id", "left")
                .withColumn("duration_hours", self.duration_to_hours("event_complete_duration"))
                .select("user_id", F.coalesce(F.col("duration_hours"), F.lit(0.0)).alias("hours"))
            )
            all_hours = content_hours.unionByName(event_hours)

            total_hours_row = (
                all_hours
                .select(F.round(F.sum("hours"), 2).alias("total"))
                .collect()[0]["total"]
            )
            total_hours = float(total_hours_row) if total_hours_row is not None else 0.0

            org_hours_df = (
                all_hours
                .join(userDF.select("user_id", F.col("mdo_id").alias("org_id")), "user_id", "left")
                .groupBy("org_id")
                .agg(F.round(F.sum("hours"), 2).alias("learning_hours"))
                .filter(F.col("org_id").isNotNull())
                .orderBy(F.desc("learning_hours"))
            )

            return str(cert_count), f"{total_hours:.2f}", org_hours_df

        # ----------------------------------------------------------
        # Compute today stats always (fresh)
        # ----------------------------------------------------------
        today_cert_str, today_hours_str, today_org_df = compute_stats(yesterday_start_str, today_start_str)

        # ----------------------------------------------------------
        # Yesterday: promote from Redis if exists, else compute fresh
        # ----------------------------------------------------------
        if existing_today_certs and existing_today_hours:
            print("[Stats] Promoting across:today → across:yesterday from Redis")
            yesterday_cert_str = existing_today_certs.decode("utf-8")
            yesterday_hours_str = existing_today_hours.decode("utf-8")

            # For org-wise yesterday, read from Redis hgetall and rebuild df
            org_yesterday_raw = Redis.getMap("lhp_learningHours",
                                             conf=config)  # returns Dict[str, str], already decoded
            org_yesterday_rows = [
                (k.replace(":today", ""), v)
                for k, v in org_yesterday_raw.items()
                if k.endswith(":today")
            ]
            if org_yesterday_rows:
                yesterday_org_df = spark.createDataFrame(org_yesterday_rows, ["org_id", "learning_hours"])
            else:
                # fallback: compute fresh for yesterday
                _, _, yesterday_org_df = compute_stats(day_before_start_str, yesterday_start_str)
        else:
            print("[Stats] First run — computing yesterday stats fresh")
            yesterday_cert_str, yesterday_hours_str, yesterday_org_df = compute_stats(
                day_before_start_str, yesterday_start_str
            )

        # ----------------------------------------------------------
        # Dispatch yesterday
        # ----------------------------------------------------------
        cert_yesterday_df = spark.createDataFrame(
            [("across:yesterday", yesterday_cert_str)],
            ["across:yesterday", "cert_count_str"]
        )
        Redis.dispatchDataFrame("lhp_certifications", cert_yesterday_df, "across:yesterday", "cert_count_str",
                                replace=False, conf=config)

        hours_yesterday_df = spark.createDataFrame(
            [("across:yesterday", yesterday_hours_str)],
            ["across:yesterday", "learning_hours_str"]
        )
        Redis.dispatchDataFrame("lhp_learningHours", hours_yesterday_df, "across:yesterday", "learning_hours_str",
                                replace=False, conf=config)

        yesterday_org_df = yesterday_org_df.withColumn("redis_key", F.concat(F.col("org_id"), F.lit(":yesterday")))
        Redis.dispatchDataFrame("lhp_learningHours", yesterday_org_df, "redis_key", "learning_hours", replace=False,
                                conf=config)

        # ----------------------------------------------------------
        # Dispatch today (always fresh)
        # ----------------------------------------------------------
        cert_today_df = spark.createDataFrame(
            [("across:today", today_cert_str)],
            ["across:today", "cert_count_str"]
        )
        Redis.dispatchDataFrame("lhp_certifications", cert_today_df, "across:today", "cert_count_str", replace=False,
                                conf=config)

        hours_today_df = spark.createDataFrame(
            [("across:today", today_hours_str)],
            ["across:today", "learning_hours_str"]
        )
        Redis.dispatchDataFrame("lhp_learningHours", hours_today_df, "across:today", "learning_hours_str",
                                replace=False, conf=config)

        today_org_df = today_org_df.withColumn("redis_key", F.concat(F.col("org_id"), F.lit(":today")))
        Redis.dispatchDataFrame("lhp_learningHours", today_org_df, "redis_key", "learning_hours", replace=False,
                                conf=config)

        print(f"\n📊 Stats dispatched to Redis:")
        print(f"  across:yesterday certs  = {yesterday_cert_str}")
        print(f"  across:yesterday hours  = {yesterday_hours_str}")
        print(f"  across:today certs      = {today_cert_str}")
        print(f"  across:today hours      = {today_hours_str}")

        return yesterday_cert_str, yesterday_hours_str, today_cert_str, today_hours_str

    # ------------------------------------------------------------------
    # HELPER: write dataframe to postgres (truncate + overwrite)
    # ------------------------------------------------------------------
    def write_postgres_table(self, df, url, table, username, password):
        (
            df.write
            .format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", username)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )


# -----------------------------------------------------------------------
# SPARK SESSION
# -----------------------------------------------------------------------
def create_spark_session(config):
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages "
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
        "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
        "org.postgresql:postgresql:42.6.0 "
        "pyspark-shell"
    )

    return (
        SparkSession.builder
        .appName("NationalLearningWeekLeaderboardModel")
        .master("local[*]")
        .config("spark.executor.memory", "20g")
        .config("spark.driver.memory", "18g")
        .config("spark.executor.memoryFraction", "0.7")
        .config("spark.storage.memoryFraction", "0.2")
        .config("spark.storage.unrollFraction", "0.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost)
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.cassandra.output.batch.size.rows", "10000")
        .config("spark.cassandra.connection.keepAliveMS", "60000")
        .config("spark.cassandra.connection.timeoutMS", "30000")
        .config("spark.cassandra.read.timeoutMS", "30000")
        .config("es.nodes", config.sparkElasticsearchConnectionHost)
        .config("es.port", config.sparkElasticsearchConnectionPort)
        .config("es.index.auto.create", "false")
        .config("es.nodes.wan.only", "true")
        .config("es.nodes.discovery", "false")
        .getOrCreate()
    )


# -----------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------
def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    spark = create_spark_session(config)

    start_time = datetime.now()
    print(
        f"[START] NLW Leaderboard processing started at: "
        f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    model = NationalLearningWeekLeaderboardModel()
    model.process_data(spark, config)

    end_time = datetime.now()
    print(f"[END]  NLW Leaderboard completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {end_time - start_time}")
    spark.stop()


if __name__ == "__main__":
    main()
