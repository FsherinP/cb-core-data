import findspark
findspark.init()

from pathlib import Path
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import current_timestamp,  lit, col, max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import time
import sys
import os
import requests
import json
import psycopg2
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from jobs.default_config import create_config
from jobs.config import KAFKA_CONFIG, get_environment_config

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

    def update_notification_status(self, resultsDF):
        """Update status and error_message columns for existing notifications"""
        try:

            status_updates = resultsDF.select("notification_id", "status", "error_message").collect()
            
            if not status_updates:
                return
            host, port = self.config.dwPostgresHost.split(":")
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=self.config.dwPostgresSchema,
                user=self.config.dwPostgresUsername,
                password=self.config.dwPostgresCredential
            )
            cursor = conn.cursor()
            
            try:
                for row in status_updates:
                    error_msg = row.error_message.replace("'", "''") if row.error_message else ""
                    update_sql = f"""
                        UPDATE {self.config.dwpeerValidationNotificationQueue}
                        SET status = '{row.status}', 
                            error_message = '{error_msg}',
                            updated_at = NOW()
                        WHERE notification_id = '{row.notification_id}'
                    """
                    cursor.execute(update_sql)
                
                conn.commit()
                print(f"[INFO] Updated {len(status_updates)} notification statuses")
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            print(f"[ERROR] Could not update notification status: {str(e)}")
            raise


    def send_notification(self):
        try:
            pendingNotificationDF = self.read_postgres_table(self.config.dwpeerValidationNotificationQueue) \
            .filter(col("status") == "PENDING")

            if pendingNotificationDF.count() == 0:
                print("[INFO] No pending notifications.")
                return
            
            api_enabled = self.config.apiBasedNotificationEnabled
            api_url = self.config.notificationAPIURL
            batch_size = self.config.notificationBatchSize
            
            # Process locally with toLocalIterator to stream data
            results = []
            session = requests.Session()
            batch = []
            
            def flush_batch(batch_rows):
                """Send a batch of notifications and collect results"""
                if not batch_rows:
                    return
                
                payload_list = []
                for row in batch_rows:
                    try:
                        payload_dict = json.loads(row.payload)
                    except Exception as e:
                        payload_dict = {}
                    payload_list.append(payload_dict)
                
                # Send batch API call
                try:
                    if api_enabled:
                        response = session.post(
                            api_url,
                            json={"request": payload_list},
                            timeout=60
                        )
                        status_code = response.status_code
                        resp_text = response.text
                    else:
                        status_code = 200
                        resp_text = "Saved locally, API disabled."
                except Exception as e:
                    status_code = 500
                    resp_text = str(e)
                
                # Create results for each row in batch
                status = "SENT" if status_code == 200 else "FAILED"
                for row in batch_rows:
                    results.append(Row(
                        notification_id=row.notification_id,
                        user_id=row.user_id,
                        event_type=row.event_type,
                        form_id=row.form_id,
                        course_id=row.course_id,
                        course_name=row.course_name,
                        first_completed_on=row.first_completed_on,
                        status=status,
                        error_message=resp_text,
                        payload=row.payload,
                        first_trigger_end=row.first_trigger_end,
                        created_at=row.created_at,
                        updated_at=datetime.now()
                    ))
            
            try:
                # toLocalIterator — streams one partition at a time, not all at once
                for row in pendingNotificationDF.toLocalIterator():
                    batch.append(row)
                    if len(batch) >= batch_size:
                        flush_batch(batch)
                        batch = []
                if batch:
                    flush_batch(batch)  # remaining rows
            finally:
                session.close()
            
            if results:
                schema = StructType([
                    StructField("notification_id", StringType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("event_type", StringType(), True),
                    StructField("form_id", StringType(), True),
                    StructField("course_id", StringType(), True),
                    StructField("course_name", StringType(), True),
                    StructField("first_completed_on", TimestampType(), True),
                    StructField("status", StringType(), True),
                    StructField("error_message", StringType(), True),
                    StructField("payload", StringType(), True),
                    StructField("first_trigger_end", TimestampType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("updated_at", TimestampType(), True),
                ])
                resultsDF = self.spark.createDataFrame(results, schema=schema)
                
                # Update only status and error_message columns for existing rows
                self.update_notification_status(resultsDF)
                
                successDF = resultsDF.filter(col("status") == "SENT")
                sent_count = successDF.count()
                if sent_count > 0:
                    latestProcessedDF = successDF.groupBy("form_id") \
                    .agg(max("first_trigger_end").alias("last_processed_date"))

                    existingFormStateDF = self.read_postgres_table(self.config.dwpeerValidationFormStateTable)
                    formsToUpdateDF = existingFormStateDF.join(
                        latestProcessedDF,
                        existingFormStateDF["form_id"] == latestProcessedDF["form_id"],
                        "left_anti"
                    )
                    finalFormStateDF = formsToUpdateDF.union(
                    latestProcessedDF.withColumn("data_generated_at", current_timestamp())
                    )
                    self.write_postgres_table(finalFormStateDF, self.config.dwpeerValidationFormStateTable, mode="overwrite")

                    kafkaDF = successDF.groupBy("form_id").count() \
                        .withColumnRenamed("count", "incrementBy") \
                        .withColumn("eventType", lit("PEER_SURVEY_NOTIFICATION_SENT")) \
                        .withColumn("timestamp", lit(int(time.time() * 1000))) \
                        .select(
                        col("eventType"),
                        col("form_id").alias("formId"),
                        col("incrementBy"),
                        col("timestamp")
                    )
                    from dfutil.utils.utils import dispatch_df_to_kafka
                    dispatch_df_to_kafka(kafkaDF, self.config.peerValidationKafkaTopic, broker_list=self.config.kpBrokerList)

                    print(f"[INFO] {sent_count} notifications sent successfully")

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