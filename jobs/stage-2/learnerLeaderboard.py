import findspark

findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, lit, count,current_date, date_format, desc, sum, max, lit, dense_rank,add_months, last_day, date_trunc, last_day, date_add, date_format, lit, concat)
from datetime import datetime
import sys
import os
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config
from pyspark.sql import functions as F


class LearnerLeaderBoardModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.leaderboard.LearnerLeaderBoardModel"

    def name(self):
        return "LearnerLeaderBoardModel"

    def process_data(self, spark, config):
        try:
            # Previous month start and end
            month_start = date_format(date_trunc("MONTH", add_months(current_date(), -1)), "yyyy-MM-dd HH:mm:ss")
            month_end = concat(date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd"), lit(" 23:59:59"))

            month = date_format(add_months(current_date(), -1), "M")
            year  = date_format(add_months(current_date(), -1), "yyyy")
            # Karma points
            karma_points_df = spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE) \
                .filter((col("credit_date") >= month_start) & (col("credit_date") <= month_end)) \
                .groupBy("userid") \
                .agg(sum("points").alias("total_points"), max("credit_date").alias("last_credit_date")) \
                .cache()

            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)

            # Orgs with more than N users
            orgWithNUsers = userOrgDF.groupBy("userOrgID") \
                .agg(count("userID").alias("count")) \
                .filter(col("count") > 10).select("userOrgID")

            filteredUserOrgDF = userOrgDF.join(orgWithNUsers, on="userOrgID", how="inner")

            userOrgDataDF = filteredUserOrgDF.select(
                col("userID").alias("userid"),
                col("userOrgID").alias("org_id"),
                col("fullName").alias("fullname"),
                col("userProfileImgUrl").alias("profile_image")
            ).cache()

            userLeaderboardDF = userOrgDataDF.join(karma_points_df, ["userid"], "left") \
                .filter(col("org_id") != "") \
                .select(
                "userid", "org_id", "fullname", "profile_image",
                "total_points", "last_credit_date"
            ) \
                .withColumn("month", month.cast("int")) \
                .withColumn("year", lit(year))

            #rank the users based on the points within each org
            window_spec_rank = Window.partitionBy("org_id").orderBy(desc("total_points"))
            userLeaderboardDF = userLeaderboardDF.withColumn("rank", dense_rank().over(window_spec_rank))

            #sort them based on their fullNames for each rank group within each org
            window_spec_row = Window.partitionBy("org_id").orderBy(col("rank"), col("last_credit_date").desc())
            userLeaderboardDF = userLeaderboardDF.withColumn("row_num", row_number().over(window_spec_row))

            # Read previous leaderboard data
            learnerLeaderboardDF = (spark.read.parquet(ParquetFileConstants.LEARNER_LEADERBOARD_PARQUET_FILE)
             .select("userid", "rank").alias("l"))

            u = userLeaderboardDF.alias("u")
            finalDF = (u.join(learnerLeaderboardDF, on="userid", how="left")
             .select(F.col("u.org_id"), F.col("u.userid"),
                F.col("u.total_points"),
                F.col("u.rank"),
                F.col("u.row_num"),
                F.col("u.fullname"),
                F.col("u.profile_image"),
                F.col("u.month"),
                F.col("u.year"),
                F.coalesce(F.col("l.rank"), F.lit(0)).alias("previous_rank"))
             .withColumn("job_execution_datetime", F.current_timestamp()))
            # Write to Cassandra
            utils.writeToCassandra(finalDF, config.cassandraUserKeyspace, config.cassandraLearnerLeaderBoardTable)
            utils.writeToCassandra(finalDF.select("userid", "row_num"), config.cassandraUserKeyspace, config.cassandraLearnerLeaderBoardLookupTable)
        except Exception as e:
            print(f"Error occurred during LearnerLeaderBoardModel processing: {str(e)}")
            raise

def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("Learner leaderboard Model - Cached") \
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
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .getOrCreate()
    return spark


def main():
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Learner leaderboard processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    spark = create_spark_session_with_packages(config)
    model = LearnerLeaderBoardModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Learner leaderboard completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()