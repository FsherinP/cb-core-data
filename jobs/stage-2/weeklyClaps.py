import findspark
findspark.init()
import os

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, MapType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, struct, lit, to_json, countDistinct, current_timestamp, current_date, date_format, broadcast, desc, unix_timestamp, when, lit, concat_ws, from_unixtime, format_string, expr)
from datetime import datetime
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, MapType

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.assessment import assessmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config

IST = ZoneInfo("Asia/Kolkata")


class WeeklyClapsModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.weeklyclaps"

    def name(self):
        return "WeeklyClapsModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def get_users_platform_engagement_schema(self) -> StructType:
        return StructType([
            StructField("userid", StringType(), nullable=True),
            StructField("platformEngagementTime", FloatType(), nullable=True),
            StructField("sessionCount", IntegerType(), nullable=True)
        ])
    def users_platform_engagement_dataframe(self, week_start: str, week_end: str, spark: SparkSession, config):
        query = f"""SELECT uid AS userid, SUM(total_time_spent) / 60.0 AS platformEngagementTime, 
                   COUNT(*) AS sessionCount FROM "summary-events" WHERE dimensions_type = 'app' 
                   AND __time >= TIMESTAMP '{week_start}' AND __time <= TIMESTAMP '{week_end}' 
                   AND uid IS NOT NULL GROUP BY 1"""
        print(query)
        df = utils.druidDFOption(query, config.sparkDruidRouterHost, limit=1000000, spark=spark)
        #df.filter(col("userid") == '765232d9-bccf-41c6-b8e9-8ac837e35af8').show()
        if df is None:
            print("Druid returned empty data, returning empty DataFrame with expected schema.")
            return spark.createDataFrame([], self.get_users_platform_engagement_schema())

        return df
    def get_this_week_dates(self, date_format: str = "%Y-%m-%d", datetime_format: str = "%Y-%m-%d %H:%M:%S"):
        now = datetime.now(IST)
        data_till_date = now - timedelta(days=1)

        start_of_week = (data_till_date - timedelta(days=data_till_date.weekday()))
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)

        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999_000)

        return (start_of_week.strftime(datetime_format), end_of_week.strftime(date_format), end_of_week.strftime(datetime_format), data_till_date.strftime(date_format),)

    def safe_to_json(self, df, col_name: str):
        field = df.schema[col_name].dataType
        if isinstance(field, (StructType, MapType)):
            return F.to_json(F.col(col_name))
        else:
            return F.col(col_name).cast("string")

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            today = datetime.now().strftime("%Y-%m-%d")
            weekStart, weekEnd, weekEndTime, dataTillDate = self.get_this_week_dates()
            app_postgres_url = f"jdbc:postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"

            existing_weekly_claps_df = spark.read.parquet(ParquetFileConstants.CLAPS_PARQUET_FILE)
            platform_engagement_df = self.users_platform_engagement_dataframe(weekStart, weekEndTime, spark, config)

            joined_df = existing_weekly_claps_df.join(platform_engagement_df, ["userid"], "full_outer")

            # === NEW: null-safety defaults for brand-new users ===
            # A full_outer join means users present ONLY in
            # platform_engagement_df (first-time users with no prior claps
            # row) come through with w1/w2/w3/total_claps/
            # claps_updated_this_week/last_claps_updated_on all NULL. Every
            # branch below assumes these are non-null, and Postgres has a
            # NOT NULL constraint on last_claps_updated_on — so we default
            # them here, immediately after the join.
            #
            # NOTE: this is a strict no-op for existing users. The
            # when(col(X).isNull(), ...) check is False for anyone who
            # already had a row in existing_weekly_claps_df, so their
            # original values pass through .otherwise(col(X)) completely
            # unchanged. Nothing about existing logic changes.
            #
            # IMPORTANT: w1/w2/w3 are stored as JSON STRINGS in the parquet
            # (safe_to_json converts them before every write), not structs.
            # The default must match that same STRING type, or Spark's
            # CASE WHEN throws a DATATYPE_MISMATCH error since both branches
            # of a when/otherwise must resolve to the same type.
            default_week_json = lit('{"timespent":0.0,"numberOfSessions":0}')

            joined_df = joined_df \
                .withColumn("w1", when(col("w1").isNull(), default_week_json).otherwise(col("w1"))) \
                .withColumn("w2", when(col("w2").isNull(), default_week_json).otherwise(col("w2"))) \
                .withColumn("w3", when(col("w3").isNull(), default_week_json).otherwise(col("w3"))) \
                .withColumn("total_claps", when(col("total_claps").isNull(), lit(0).cast("long")).otherwise(col("total_claps"))) \
                .withColumn("claps_updated_this_week",
                            when(col("claps_updated_this_week").isNull(), lit(False)).otherwise(col("claps_updated_this_week"))) \
                .withColumn("last_claps_updated_on",
                            when(col("last_claps_updated_on").isNull(), current_timestamp()).otherwise(col("last_claps_updated_on"))) \
                .withColumn("last_updated_on",
                            when(col("last_updated_on").isNull(), lit(dataTillDate)).otherwise(col("last_updated_on")))

            joined_df = joined_df.withColumn("w4", struct(
                when(col("platformEngagementTime").isNull(), lit(0.0)).otherwise(col("platformEngagementTime")).alias(
                    "timespent"),
                when(col("sessionCount").isNull(), lit(0)).otherwise(col("sessionCount")).alias("numberOfSessions")
            ))

            df = joined_df
            condition = (col("w4")["timespent"] >= lit(config.cutoffTime)) & (~col("claps_updated_this_week"))

            # === Weekend rollover check ===
            if (dataTillDate == weekEnd) and (dataTillDate != df.selectExpr("max(last_updated_on)").collect()[0][0]):
                print("Started weekend updates...")
                df = df.select(
                    col("w2").alias("w1"),
                    col("w3").alias("w2"),
                    col("w4").alias("w3"),
                    col("w4"),
                    col("total_claps"),
                    col("userid"),
                    col("platformEngagementTime"),
                    col("sessionCount"),
                    col("claps_updated_this_week"),
                    col("last_claps_updated_on")
                ).withColumn("total_claps",
                             when(col("w4")["timespent"] < lit(config.cutoffTime), 0).otherwise(col("total_claps"))) \
                    .withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps"))) \
                    .withColumn("last_updated_on", lit(dataTillDate)) \
                    .withColumn("claps_updated_this_week", lit(False)) \
                    .withColumn("w4", struct(
                    lit(0.0).alias("timespent"),
                    lit(0).alias("numberOfSessions")
                ))
                print("Completed weekend updates.")
            else:
                df = df.withColumn("total_claps", when(condition, col("total_claps") + 1).otherwise(col("total_claps"))) \
                    .withColumn("claps_updated_this_week",
                                when(condition, lit(True)).otherwise(col("claps_updated_this_week")))

            # === Cleanup / defaults (unchanged — now effectively a no-op
            # safety net since nulls are handled above, but left in place
            # so existing behavior/structure is not touched) ===
            df = df.withColumn("total_claps", when(col("total_claps").isNull(), 0).otherwise(col("total_claps"))) \
                .withColumn("claps_updated_this_week",
                            when(col("claps_updated_this_week").isNull(), False).otherwise(
                                col("claps_updated_this_week"))) \
                .withColumn("last_claps_updated_on",
                            when(condition, current_timestamp()).otherwise(col("last_claps_updated_on")))

            df = df.drop("platformEngagementTime", "sessionCount")

            # === Safe JSON conversion ===
            final_df = df \
                .withColumn("w1", self.safe_to_json(df, "w1")) \
                .withColumn("w2", self.safe_to_json(df, "w2")) \
                .withColumn("w3", self.safe_to_json(df, "w3")) \
                .withColumn("w4", self.safe_to_json(df, "w4"))

            final_df.show(5, truncate=False)
            # === Write outputs ===
            final_df.coalesce(1).write.mode("overwrite").csv(f"/tmp/weeklyClaps{today}", header=True)

            # Uncomment for Postgres writes
            self.write_postgres_table(final_df, app_postgres_url,
                                      config.dwLearnerStatsTable,
                                      config.appPostgresUsername,
                                      config.appPostgresCredential)

            total_time = time.time() - start_time
            print(f"\n✅ Weekly Claps Job completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

    def write_postgres_table(self, df, url: str, table: str, username: str, password: str, mode: str = "overwrite"):
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
def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Weekly Claps Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Weekly claps job processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = WeeklyClapsModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Weekly claps job completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()
if __name__ == "__main__":
    main()
