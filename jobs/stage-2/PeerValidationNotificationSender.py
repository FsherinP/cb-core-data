import findspark
findspark.init()

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, struct, lit, col, collect_list, row_number, floor, array, concat, date_format
from datetime import datetime, time
import sys
import os
import requests
from pyspark.sql.window import Window
import json
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import KAFKA_CONFIG, get_environment_config
from dfutil.utils.utils import dispatch_df_to_kafka

class PeerValidationNotificationSender:
    def __init__(self,spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.PeerValidationNotificationSender"

    def name(self):
        return "PeerValidationNotificationSender"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

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
            today = self.get_date()
            eligibleUsersDF = self.spark.read.parquet(ParquetFileConstants.PEER_VALIDATION_ELIGIBLE_USERS_PARQUET_FILE)
            usersWindow = Window.orderBy("user_id")

            eligibleUsersDF = eligibleUsersDF.withColumn("row_num", row_number().over(usersWindow)) \
                .withColumn("batch_id", floor((col("row_num") - 1) / self.config.notificationBatchSize))

            notificationDF = eligibleUsersDF.withColumn(
                "notification",
                struct(
                    col("user_id"),
                    lit("IN_APP").alias("type"),
                    lit("PEER_VALIDATION").alias("category"),
                    lit("PEER_VALIDATION").alias("sub_type"),
                    lit("SYSTEM_CREATED").alias("source"),
                    lit("PEER_EVALUATION_ASSIGNED").alias("sub_category"),
                    struct(
                        array(
                            struct(
                                col("formId"),
                                col("courseName"),
                                lit(False).alias("isSurveySubmitted"),
                                date_format(col("first_completed_on"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("completionDate"),
                                col("createdBy").alias("surveyCreatedById"),
                                col("title").alias("surveyName"),
                                date_format(col("surveyEndDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("surveyEndDate"),
                                col("full_name").alias("learnerName")
                            )
                        ).alias("data"),
                        concat(
                            lit("Peer validation survey is now available for"),
                            col("courseName"),
                            lit(".")
                        ).alias("body")
                    ).alias("message")
                )
            )
            if self.config.apiBasedNotificationEnabled:
                notification_api_url = self.config.notificationAPIURL
                batchDF = notificationDF.groupBy("batch_id").agg(
                    collect_list("notification").alias("request")
                )

                results = []
                for batch in batchDF.toLocalIterator():
                    # Convert Row objects to proper dicts
                    clean_request = []
                    for notif in batch["request"]:
                        notif_dict = notif.asDict(recursive=True)
                        clean_request.append({
                            "user_id": notif_dict["user_id"],
                            "type": notif_dict["type"],
                            "category": notif_dict["category"],
                            "sub_type": notif_dict["sub_type"],
                            "source": notif_dict["source"],
                            "sub_category": notif_dict["sub_category"],
                            "message": {
                                "data": notif_dict["message"]["data"],
                                "body": notif_dict["message"]["body"]
                            }
                        })
                    payload = {"request": clean_request}

                    try:
                        
                        response = requests.post(notification_api_url, json=payload, timeout=300)
                        status_code = response.status_code
                        resp_text = response.text
                    except Exception as e:
                        status_code = 500
                        resp_text = str(e)

                    # Yield each user individually
                    for notif in clean_request:
                        data_item = notif["message"]["data"][0]
                        results.append((
                            notif["user_id"],
                            data_item["formId"],
                            data_item["courseName"],
                            data_item["completionDate"],
                            status_code,
                            resp_text
                        ))
                
                apiResponseDF = self.spark.createDataFrame(
                    results,
                    ["user_id", "form_id", "course_id", "first_completed_on", "http_status", "response_body"]
                )
                successDF = apiResponseDF.filter(col("http_status") == 200) \
                .withColumn("notification_sent_on", current_timestamp()) \
                .withColumn("data_generated_on", current_timestamp()) \
                .withColumnRenamed("http_status", "notification_status")

                failedDF = apiResponseDF.filter(col("http_status") != 200) \
                .withColumn("data_generated_on", current_timestamp()) \
                .withColumnRenamed("http_status", "notification_status")

            else:
                notificationDF.write.mode("overwrite").json(f"{self.config.localReportDir}/{self.config.peerValidationAPIPath}/{today}")
                successDF = eligibleUsersDF.withColumn("form_id", col("formId")) \
                            .withColumn("notification_status", lit(200)) \
                            .withColumn("notification_sent_on", current_timestamp()) \
                            .withColumn("data_generated_on", current_timestamp())

            self.write_postgres_table(successDF,self.config.dwnotifiedUsersTable,mode="append")

            if self.config.apiBasedNotificationEnabled:
                self.write_postgres_table(failedDF, self.config.dwfailednotifiedUsersTable,  mode="append")

            formNotificationCountDF = successDF.groupBy("form_id").count() \
                .withColumnRenamed("count", "incrementBy") \
                .withColumn("eventType", lit("PEER_SURVEY_NOTIFICATION_SENT")) \
                .withColumn("timestamp", lit(int(time.time() * 1000)))

            kafkaDF = formNotificationCountDF.select(
                col("eventType"),
                col("form_id").alias("formId"),
                col("incrementBy"),
                col("timestamp")
            )
            dispatch_df_to_kafka(kafkaDF, self.config.peerValidationKafkaTopic, broker_list=self.config.brokerList)

            print(f"[INFO] Notifications sent successfully")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise


def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Peer Validation Notification Sender Model") \
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
    print(f"[START] Peer Validation Notification Sender processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = PeerValidationNotificationSender(spark, config)
    model.send_notification()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Peer Validation Notification Sender completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()