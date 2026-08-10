import findspark

findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, expr, countDistinct, size, current_timestamp, date_trunc, date_sub,
    to_timestamp, split, sum, round, length, upper, coalesce, from_json, explode_outer,
    exists, max as spark_max, trim
)
from datetime import datetime, timedelta
from pyspark.sql.types import (StructType, StructField, StringType, DateType, IntegerType, LongType)
from datetime import datetime, timedelta, time, timezone
import sys
import os
import requests
sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from dfutil.utils.utils import druidDFOption
from dfutil.enrolment import enrolmentDFUtil
from dfutil.utils import utils
from dfutil.utils.redis import Redis
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil
from util import schemas

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config


# ---------------------------------------------------------------------------
# Constants — previously hardcoded in Ansible, now owned by this script
# ---------------------------------------------------------------------------
CENTRAL_MINISTRIES_COUNT = 94
STATE_UT_COUNT = 36


def format_count(n):
    n = int(n)
    s = str(n)
    if len(s) <= 3:
        return s
    result = s[-3:]
    s = s[:-3]
    while s:
        result = s[-2:] + ',' + result
        s = s[:-2]
    return result


def format_count(n):
    n = int(n)
    s = str(n)
    if len(s) <= 3:
        return s
    result = s[-3:]
    s = s[:-3]
    while s:
        result = s[-2:] + ',' + result
        s = s[:-2]
    return result


def safe_int(v):
    return int(v) if v is not None else 0

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

            # ------------------------------------------------------------------ #
            # Exclude VOLUNTEER (Non-Govt) users from every metric in this report.
            # A user is VOLUNTEER/Non-Govt if EITHER:
            #   - any designation inside their profiledetails JSON is EXACTLY "VOLUNTEER"
            #     (case-insensitive, trimmed) — NOT a substring match, so designations
            #     like "Civil Defence Volunteer" are correctly left as Govt.
            #   - their roles array contains an entry that EXACTLY equals "VOLUNTEER"
            #     (case-insensitive, trimmed)
            #
            # designation lives inside profiledetails (a JSON string) under
            # professionalDetails, which is itself an array, so it has to be parsed
            # and exploded to get at it. Exploding directly on userDF would multiply
            # rows per user (bad for every downstream count), so instead we parse it
            # in a small side dataframe, collapse back to a single per-user flag via
            # groupBy, and join that flag onto userDF — which stays one row per user.
            # ------------------------------------------------------------------ #
            profileDetailsSchema = schemas.makeProfileDetailsSchema(False, True, True, True)

            designationVolunteerFlagDF = (
                userDF.select(col("user_id"), col("profiledetails"))
                .na.fill("{}", subset=["profiledetails"])
                .withColumn("profileDetailsParsed", from_json(col("profiledetails"), profileDetailsSchema))
                .withColumn("professionalDetail", explode_outer(col("profileDetailsParsed.professionalDetails")))
                .withColumn("designation", coalesce(col("professionalDetail.designation"), lit("")))
                .groupBy("user_id")
                .agg(
                    spark_max(
                        when(trim(upper(col("designation"))) == "VOLUNTEER", lit(1)).otherwise(lit(0))
                    ).alias("is_volunteer_by_designation")
                )
            )


            userDF = userDF.join(designationVolunteerFlagDF, ["user_id"], "left") \
                .withColumn("is_volunteer_by_designation",
                            coalesce(col("is_volunteer_by_designation"), lit(0)))

            is_volunteer_expr = (
                    (col("is_volunteer_by_designation") == 1) |
                    coalesce(exists(col("roles"), lambda r: trim(upper(r)) == "VOLUNTEER"), lit(False))
            )

            volunteerUserIdsDF = userDF.filter(is_volunteer_expr) \
                .select(col("user_id").alias("actor_id")) \
                .distinct() \
                .cache()

            userDF = userDF.filter(~is_volunteer_expr).drop("is_volunteer_by_designation")

            eventsEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE)
            contentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            externalContentEnrolmentDataDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            externalContentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

            # Event enrolments aren't joined against userDF anywhere below (unlike content
            # enrolments), so VOLUNTEER exclusion has to be applied explicitly here via an
            # inner join against the (already-filtered, govt-only) user set.
            eventsEnrolmentDataDF = eventsEnrolmentDataDF.join(
                userDF.select("user_id"),
                ["user_id"],
                "inner"
            )


            # ------------------------------------------------------------------ #
            # Active users (status == 1) joined with org
            # ------------------------------------------------------------------ #
            userWithOrgDF = userDF.filter(col("mdo_id").isNotNull())
            activeUsersDF = userDF.filter(col("status") == 1)

            # ------------------------------------------------------------------ #
            # Content enrolments (active users only)
            # ------------------------------------------------------------------ #
            enrichedContentEnrolmentsDF = contentEnrolmentDataDF.alias("e") \
                .join(activeUsersDF.select("user_id").alias("u"),
                      col("e.userID") == col("u.user_id"), "inner") \
                .select(col("e.*"))

            total_enrolments = enrichedContentEnrolmentsDF.count()
            #Redis.update("dashboard_enrolment_count", str(total_enrolments), conf=config)
            Redis.update("dashboard_enrolment_count_updated_format", format_count(total_enrolments), conf=config)

            # ------------------------------------------------------------------ #
            # Unique users enrolled in Course (Live or Retired)
            # ------------------------------------------------------------------ #
            enrichedCourseEnrolmentsDF = contentEnrolmentDataDF.alias("e") \
                .join(
                contentDF.select("content_id", "content_type", "content_status").alias("c"),
                col("e.content_id") == col("c.content_id"), "left"
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
            #Redis.update("dashboard_unique_users_enrolled_count", str(unique_users_enrolled), conf=config)
            Redis.update("dashboard_unique_users_enrolled_count_updated_format", format_count(unique_users_enrolled), conf=config)

            # ------------------------------------------------------------------ #
            # Content completions — certificateID > 5 chars
            # ------------------------------------------------------------------ #
            total_content_completions = enrichedContentEnrolmentsDF \
                .filter(length(col("certificateID")) > 5) \
                .count()
            #Redis.update("dashboard_completed_count", str(total_content_completions), conf=config)
            Redis.update("dashboard_completed_count_updated_format", format_count(total_content_completions), conf=config)

            # ------------------------------------------------------------------ #
            # Event metrics
            # ------------------------------------------------------------------ #
            total_event_enrolments = eventsEnrolmentDataDF.count()
            #Redis.update("dashboard_events_enrolment_count", str(total_event_enrolments), conf=config)
            Redis.update("dashboard_events_enrolment_count_updated_format", format_count(total_event_enrolments), conf=config)

            enrichedEventCompletionsDF = eventsEnrolmentDataDF \
                .filter(length(col("certificate_id")) > 5)
            total_event_completions = enrichedEventCompletionsDF.count()
            #Redis.update("dashboard_events_completed_count", str(total_event_completions), conf=config)
            Redis.update("dashboard_events_completed_count_updated_format", format_count(total_event_completions), conf=config)

            # ------------------------------------------------------------------ #
            # Certificates generated yesterday (content + events)
            # ------------------------------------------------------------------ #
            prev_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            print(prev_day)

            content_certs_yday = enrichedContentEnrolmentsDF \
                .filter(length(col("certificateID")) > 5) \
                .filter(F.to_date(col("first_completed_on")) == prev_day) \
                .count()

            event_certs_yday = eventsEnrolmentDataDF \
                .filter(length(col("certificate_id")) > 5) \
                .filter(F.to_date(col("completed_on_datetime")) == prev_day) \
                .count()

            print("content count : " + str(content_certs_yday))
            print("event count : " + str(event_certs_yday))
            total_certs_yday = content_certs_yday + event_certs_yday
            Redis.update("lp_completed_yesterday_count", str(total_certs_yday), conf=config)
            Redis.update("lp_completed_yesterday_count_updated_format", format_count(total_certs_yday), conf=config)

            # ------------------------------------------------------------------ #
            # Registered users (active) & registered yesterday
            # ------------------------------------------------------------------ #
            total_registered_users = activeUsersDF.count()
            #Redis.update("mdo_total_registered_officer_count", str(total_registered_users), conf=config)
            Redis.update("mdo_total_registered_officer_count_updated_format", format_count(total_registered_users), conf=config)

            usersRegisteredYesterdayCount = activeUsersDF \
                .withColumn("yesterdayStartTimestamp",
                            date_trunc("day", date_sub(current_timestamp(), 1)).cast("long")) \
                .withColumn("todayStartTimestamp",
                            date_trunc("day", current_timestamp()).cast("long")) \
                .filter(expr("userCreatedTimestamp >= yesterdayStartTimestamp AND userCreatedTimestamp < todayStartTimestamp")) \
                .count()
            #Redis.update("dashboard_new_users_registered_yesterday", str(usersRegisteredYesterdayCount), conf=config)
            Redis.update("dashboard_new_users_registered_yesterday_updated_format", format_count(usersRegisteredYesterdayCount), conf=config)

            # ------------------------------------------------------------------ #
            # Live courses — reuse single filtered DF for count, publishers, duration
            # ------------------------------------------------------------------ #
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)

            liveCoursesDF = contentDF \
                .filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("Course", "Moderated Course", "External Content")) \
                .cache()

            # Count
            liveCourseCount = liveCoursesDF.count()
            #Redis.update("dashboard_courses_published_live_count", str(liveCourseCount), conf=config)
            Redis.update("dashboard_courses_published_live_count_updated_format", format_count(liveCourseCount), conf=config)

            # Distinct publishers
            course_publisher_count = liveCoursesDF \
                .filter(col("content_provider_id").isNotNull()) \
                .select(countDistinct("content_provider_id").alias("publisher_count")) \
                .first()["publisher_count"]
            #Redis.update("dashboard_course_publisher_count", str(course_publisher_count), conf=config)
            Redis.update("dashboard_course_publisher_count_updated_format", format_count(course_publisher_count), conf=config)

            liveCoursesFilteredDF = contentDF \
                .filter(col("content_status").isin("Live", "LIVE")) \
                .filter(col("content_sub_type").isin("Course", "Moderated Course")) \
                .cache()

            # Duration in hours
            parts = split(col("content_duration"), ":")
            result = liveCoursesFilteredDF \
                .withColumn(
                "duration_seconds",
                parts[0].cast("int") * 3600 +
                parts[1].cast("int") * 60 +
                parts[2].cast("int")
            ) \
                .agg(round(sum("duration_seconds") / 3600).cast("int").alias("total_hours"))
            total_hours = result.first()["total_hours"]
            #Redis.update("dashboard_courses_published_live_duration", str(total_hours), conf=config)
            Redis.update("dashboard_courses_published_live_duration_updated_format", format_count(total_hours), conf=config)

            liveCoursesDF.unpersist()

            # ------------------------------------------------------------------ #
            # Hardcoded constants — written to Redis so Ansible just fetches them
            # ------------------------------------------------------------------ #
            Redis.update("dashboard_central_ministries_count", str(CENTRAL_MINISTRIES_COUNT), conf=config)
            Redis.update("dashboard_state_ut_count", str(STATE_UT_COUNT), conf=config)
            print("Done till here")
            # ------------------------------------------------------------------ #
            # Department organisations onboarded — ES org_v4 index
            # Total active orgs (status=1) minus central ministries minus state/UTs
            # ------------------------------------------------------------------ #
            es_url = f"http://{config.sparkElasticsearchConnectionHost}:{config.sparkElasticsearchConnectionPort}/org_v4/_count"
            count_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"status": 1}}
                        ]
                    }
                }
            }
            response = requests.post(es_url, json=count_query, headers={"Content-Type": "application/json"})
            total_orgs = response.json()["count"]

            #total_orgs = org_df.filter(col("status") == 1).count()
            dept_org_onboarded = total_orgs - CENTRAL_MINISTRIES_COUNT - STATE_UT_COUNT
            #Redis.update("dashboard_dept_org_onboarded_count", str(dept_org_onboarded), conf=config)
            Redis.update("dashboard_dept_org_onboarded_count_updated_format", format_count(dept_org_onboarded), conf=config)

            # ------------------------------------------------------------------ #
            # MAU (last 30 days) via Druid
            # Druid can't join against our user table directly, so instead of asking
            # Druid for a pre-aggregated COUNT(DISTINCT actor_id), we pull back the
            # distinct actor_ids themselves and exclude VOLUNTEER users here in Spark
            # by anti-joining against volunteerUserIdsDF (actor_id == user_id).
            # ------------------------------------------------------------------ #
            mau_query = """SELECT DISTINCT actor_id
                           FROM "telemetry-events-syncts"
                           WHERE eid IN ('IMPRESSION', 'INTERACT', 'START', 'END')
                             AND actor_type = 'User'
                             AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '30' DAY, 'P1D')
                             AND __time <  TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')"""

            mau_df = druidDFOption(mau_query, config.sparkDruidRouterHost, limit=10000000, spark=spark)
            if mau_df is None:
                mau_df = self._empty_df(spark, "actor_id")

            mau_govt_only_df = mau_df.join(volunteerUserIdsDF, ["actor_id"], "left_anti")
            total_mau = mau_govt_only_df.select(countDistinct("actor_id").alias("activeCount")).first()["activeCount"]
            Redis.update("lp_monthly_active_users", str(total_mau), conf=config)
            Redis.update("lp_monthly_active_users_updated_format", format_count(total_mau), conf=config)

            # ------------------------------------------------------------------ #
            # Users logged in yesterday via Druid — same approach as MAU above:
            # pull distinct actor_ids, then anti-join against volunteerUserIdsDF.
            # ------------------------------------------------------------------ #
            user_loggedin_yesterday_query = """SELECT DISTINCT actor_id
                                               FROM "telemetry-events-syncts"
                                               WHERE eid IN ('IMPRESSION', 'INTERACT', 'START', 'END')
                                                 AND actor_type = 'User'
                                                 AND __time >= TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE - INTERVAL '24' HOUR, 'P1D')
                                                 AND __time <  TIME_FLOOR(CURRENT_TIMESTAMP + INTERVAL '5:30' HOUR TO MINUTE, 'P1D')"""

            user_loggedin_yesterday_df = druidDFOption(
                user_loggedin_yesterday_query,
                config.sparkDruidRouterHost,
                limit=10000000,
                spark=spark
            )
            if user_loggedin_yesterday_df is None:
                user_loggedin_yesterday_df = spark.createDataFrame(
                    [], StructType([StructField("actor_id", StringType(), True)])
                )

            user_loggedin_yesterday_govt_only_df = user_loggedin_yesterday_df.join(
                volunteerUserIdsDF, ["actor_id"], "left_anti"
            )
            user_loggedin_yesterday_count = user_loggedin_yesterday_govt_only_df \
                .select(countDistinct("actor_id").alias("userLoggedInYesterday")) \
                .first()["userLoggedInYesterday"]
            #Redis.update("dashboard_user_loggedin_yesterday_count", str(user_loggedin_yesterday_count), conf=config)
            Redis.update("dashboard_user_loggedin_yesterday_count_updated_format", format_count(user_loggedin_yesterday_count), conf=config)

            volunteerUserIdsDF.unpersist()
            run_date = datetime.strptime(self.get_date(), "%Y-%m-%d").date()

            dsr_metrics_schema = StructType([
                StructField("date", DateType(), True),
                StructField("central_depts", IntegerType(), True),
                StructField("dept_orgs", IntegerType(), True),
                StructField("states_uts", IntegerType(), True),
                StructField("course_publishers", IntegerType(), True),
                StructField("courses_published", IntegerType(), True),
                StructField("course_duration_hours", IntegerType(), True),
                StructField("course_enrolments", LongType(), True),
                StructField("course_completions", LongType(), True),
                StructField("event_enrolments", LongType(), True),
                StructField("event_completions", LongType(), True),
                StructField("users_registered_total", LongType(), True),
                StructField("new_user_registrations", IntegerType(), True),
                StructField("users_enrolled_one_course", LongType(), True),
                StructField("certificates_issued", IntegerType(), True),
                StructField("users_logged_in", IntegerType(), True),
                StructField("mau", IntegerType(), True),
            ])

            metrics_row = (
                run_date,
                safe_int(CENTRAL_MINISTRIES_COUNT),
                safe_int(dept_org_onboarded),
                safe_int(STATE_UT_COUNT),
                safe_int(course_publisher_count),
                safe_int(liveCourseCount),
                safe_int(total_hours),
                safe_int(total_enrolments),
                safe_int(total_content_completions),
                safe_int(total_event_enrolments),
                safe_int(total_event_completions),
                safe_int(total_registered_users),
                safe_int(usersRegisteredYesterdayCount),
                safe_int(unique_users_enrolled),
                safe_int(total_certs_yday),
                safe_int(user_loggedin_yesterday_count),
                safe_int(total_mau),
            )

            dsrMetricsDF = spark.createDataFrame([metrics_row], schema=dsr_metrics_schema)

            dw_postgres_url = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"

            # -------------------------------------------------------------- #
            # Read existing history table. If this read fails for any
            # reason, ABORT — do not fall back to writing just this one
            # row, since that would wipe out all prior history.
            # -------------------------------------------------------------- #
            try:
                existingMetricsDF = self.read_postgres_table(
                    spark,
                    dw_postgres_url,
                    "dsr_metrics_history",
                    config.dwPostgresUsername,
                    config.dwPostgresCredential
                )
            except Exception as read_err:
                raise RuntimeError(
                    f"Failed to read existing dsr_metrics_history table — aborting to avoid "
                    f"overwriting history with a single row: {read_err}"
                ) from read_err

            # Drop any existing row for today's date (rerun-safe)
            existingMetricsDF = existingMetricsDF.filter(col("date") != lit(run_date))

            # CRITICAL: force this DataFrame to materialize NOW, before the
            # write below truncates the table. Spark reads are lazy — if we
            # don't force evaluation here, the write's "truncate=true" will
            # empty the table BEFORE this read actually executes, since both
            # are part of the same lazy plan reading/writing the same table.
            # That silently turns "existing + new row" into just "new row".
            existingMetricsDF = existingMetricsDF.cache()
            existing_row_count = existingMetricsDF.count()
            print(f"[INFO] Existing dsr_metrics_history rows before write: {existing_row_count}")

            # Postgres columns are all NUMERIC (unconstrained), which Spark's JDBC
            # reader maps to DecimalType — not the Integer/LongType used above.
            # Cast the new row's columns to match existingMetricsDF's actual schema
            # so unionByName doesn't fail on a type mismatch.
            for field in existingMetricsDF.schema.fields:
                if field.name != "date":
                    dsrMetricsDF = dsrMetricsDF.withColumn(field.name, col(field.name).cast(field.dataType))

            combinedMetricsDF = existingMetricsDF.unionByName(dsrMetricsDF)

            # Postgres's numeric column is unconstrained, so it stores whatever
            # scale the incoming value has. Casting to LongType here (rather than
            # leaving the DecimalType(38,18) picked up during the union) keeps
            # values stored as clean whole numbers instead of "94.000000000000000000".
            for field in combinedMetricsDF.schema.fields:
                if field.name != "date":
                    combinedMetricsDF = combinedMetricsDF.withColumn(field.name, col(field.name).cast("long"))

            self.write_postgres_table(combinedMetricsDF, dw_postgres_url,
                                      "dsr_metrics_history",
                                      config.dwPostgresUsername,
                                      config.dwPostgresCredential)

            existingMetricsDF.unpersist()

            print("[SUCCESS] DSRComputationModel unified metrics updated")

        except Exception as e:
            print(f"❌ Error occurred during DSRComputationModel processing: {str(e)}")
            raise e

    def write_postgres_table(self, df, url: str, table: str, username: str, password: str):
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

    def read_postgres_table(self, spark, url: str, table: str, username: str, password: str):
        """Read data from PostgreSQL table"""
        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .load()


def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
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
