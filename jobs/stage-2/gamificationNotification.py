import findspark
findspark.init()

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import  current_date, date_add, from_unixtime, col, md5, current_timestamp, to_date, struct, lit, array, concat, to_json
from datetime import datetime, time
import sys
import os
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import KAFKA_CONFIG, get_environment_config
from dfutil.utils.utils import dispatch_df_to_kafka

class GamificationNotificationSender:
    def __init__(self,spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.GamificationNotificationSender"

    def name(self):
        return "GamificationNotificationSender"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
    def read_postgres_table(self, table: str) -> "DataFrame":
        """Read data from PostgreSQL table"""
        postgres_url = f"jdbc:postgresql://{self.config.dwPostgresHost}/{self.config.dwPostgresSchema}"

        return self.spark.read \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", table) \
            .option("user", self.config.dwPostgresUsername,) \
            .option("password", self.config.dwPostgresCredential) \
            .option("driver", "org.postgresql.Driver") \
            .load()
    
    def write_postgres_table(self, df, table: str, mode: str = "overwrite"):
            postgres_url = f"jdbc:postgresql://{self.config.dwPostgresHost}/{self.config.dwPostgresSchema}"
            df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", table) \
                .option("user", self.config.dwPostgresUsername) \
                .option("password", self.config.dwPostgresCredential) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
    
    def send_notification(self):
        try:
            print("Step 1: Loading Gamification Master Data...")
            gamificationUsersDF = self.spark.read.parquet(ParquetFileConstants.GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE).filter((col("dbCompletionStatus") == 1) & (col("badge_earning_date").isNotNull()))
            print("✅ Step 1 Complete")
            
            print("Step 2: Calculating Notification Eligible Dates...")
            gamificationUsersDF = gamificationUsersDF.withColumn("badge_date",to_date(from_unixtime(col("badge_earning_date") / 1000)))
            print("✅ Step 2 Complete")

            print("Step 3: Filtering Eligible Users...")
            target_date = date_add(current_date(), self.config.gamificationNotificationEligibilityDays)
               
            eligibleUsersDF = gamificationUsersDF.filter(col("badge_date") == target_date)
            print("✅ Step 3 Complete")

            print("Step 4: Reading Course Reminder States...")
            stateDF = self.read_postgres_table(self.config.dwcourseReminderStateTable)
            print("✅ Step 4 Complete")

            print("Step 5: Joining with State Data...")
            newUsersDF = eligibleUsersDF.join(
                stateDF,
                [eligibleUsersDF["userID"] == stateDF["user_id"], 
                 eligibleUsersDF["content_id"] == stateDF["course_id"]],
                "left_anti"
            )
            print("✅ Step 5 Complete")

            print("Step 6: Constructing Notifications to save in DB...")

            notificationDF = newUsersDF.select(
                col("userID").alias("user_id"),
                lit("Gamification").alias("event_type"),
                col("content_id").alias("reference_id"),
                # The Payload
                to_json(struct(array(struct(
                    col("userID").alias("user_id"),
                    lit("IN_APP").alias("type"),
                    lit("LEARN").alias("category"),
                    lit("ALERT").alias("sub_type"),
                    lit("SYSTEM_CREATED").alias("source"),
                    lit("AWARD_BADGES_REMINDER").alias("sub_category"),
                    struct(
                        array(
                            struct(
                                col("content_id").alias("courseId"),
                                col("courseName"),
                                col("badge_earning_date").alias("badgeEarningDateTime"),
                                col("badge_title").alias("badgeTitle"),
                            )
                        ).alias("data"),
                        concat(
                            lit("Hurry! You’re close to earning "),
                            col("badge_title"),
                            lit(" . Complete the required criteria before the "),
                            col("badge_date").cast("string"),
                            lit(".")
                        ).alias("body")
                    ).alias("message")
                    )
                    ).alias("request")
                )).alias("payload"),
                md5(concat(col("userID"), lit("_"), col("content_id"))).alias("idempotency_key"),
                current_timestamp().alias("created_at"))
            
            self.write_postgres_table(notificationDF, self.config.dwnotificationQueue, mode="append")
            print("✅ Step 6 Complete")

            print("Step 7: Updating State Data...")
        
            stateUpdatesDF = newUsersDF.select(
                col("userID").alias("user_id"),
                col("content_id").alias("course_id"),
                lit("7_DAYS_BEFORE").alias("reminder_type") 
            )
            self.write_postgres_table(stateUpdatesDF, self.config.dwcourseReminderStateTable, mode="append")
            print("✅ Step 7 Complete")
            
            print("Processing Complete. Notifications have been saved to the database and state has been updated.")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Gamification Notification Sender Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.driver.maxResultSize", "3g") \
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
    print(f"[START] Gamification Notification Sender processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = GamificationNotificationSender(spark, config)
    model.send_notification()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Gamification Notification Sender completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()