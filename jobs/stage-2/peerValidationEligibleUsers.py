import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (array, col, concat, concat_ws, current_timestamp, date_format, date_add, explode_outer, md5, struct, to_date,lit, date_sub, from_unixtime,when, to_json)
from datetime import datetime
import sys
import os
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config

class PeerValidationEligibleUsers:
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.PeerValidationEligibleUsers"

    def name(self):
        return "PeerValidationEligibleUsers"
    
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
            postgres_url = f"jdbc:postgresql://{self.config.dwPostgresHost}/{self.config.dwPostgresSchema}?stringtype=unspecified"
            df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", table) \
                .option("user", self.config.dwPostgresUsername) \
                .option("password", self.config.dwPostgresCredential) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
    
    
    def load_parquet_data(self):
        enrolmentDF = self.spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).select(
            col("userID").alias("user_id"),
            col("courseID").alias("course_id"),
            col("firstCompletedOn"),
            col("certificateID"),
            col("dbCompletionStatus")
        ).filter(
            (col("certificateID").isNotNull()) &
            (col("certificateID") != "") &
            (col("dbCompletionStatus") == "2")
        ).withColumn(
            "firstCompletedOn_date",
            to_date(date_format(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT))
        )
        userOrgDF = self.spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select(
            col("userID").alias("user_id"),
            col("userOrgID"),
            col("fullName")
        )
        courseDetailsDF = self.spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).select(
            col("courseID").alias("course_id"),
            col("courseName").alias("course_name")
        )
        return enrolmentDF, userOrgDF, courseDetailsDF
    
    def filter_forms_by_state(self, forms: DataFrame, formHistoryDF: DataFrame) -> DataFrame:
        return forms.join(formHistoryDF, on="form_id", how="left") \
            .withColumn("published_date", col("createdDate").cast("date")) \
            .filter(
                (col("last_processed_date").isNull()) |
                (date_sub(col("endDate"), col("min_trigger_days")) > col("last_processed_date"))
            )
    
    def expand_forms(self, formsDF: DataFrame) -> DataFrame:
        return formsDF \
        .withColumn("org", explode_outer(col("createdFor"))) \
        .withColumn("form_org_id", col("org.orgId")) \
        .drop("org")
    
    def add_trigger_windows(self, df: DataFrame) -> DataFrame:
        return df.withColumn('first_trigger_start', when(col('last_processed_date').isNull(), date_sub(col("published_date"), col("max_trigger_days"))) \
                                    .otherwise(date_add(col("last_processed_date"), 1))) \
                .withColumn('first_trigger_end', when(col('last_processed_date').isNull(), date_sub(col("published_date"), col("min_trigger_days"))) \
                                    .otherwise(date_add(col("last_processed_date"), 1)))
    
    def compute_eligible_users(self, formsDF: DataFrame, enrolmentDF: DataFrame, userOrgDF: DataFrame, courseDetailsDF: DataFrame) -> DataFrame:
        return  enrolmentDF.join(userOrgDF.alias("org"), on="user_id", how="left") \
                .join(formsDF.alias("forms"), on="course_id", how="inner") \
            .join(courseDetailsDF.alias("course"), on="course_id", how="left") \
            .filter(
                (
                    (col("forms.is_spv_created") == True) |
                    (
                        (col("forms.is_spv_created") == False) &
                        (col("org.userOrgID") == col("forms.form_org_id"))
                    )
                ) &
                (col("firstCompletedOn_date").between(col("forms.first_trigger_start"), col("forms.first_trigger_end")))
            ) \
            .withColumn("notification_id", md5(concat_ws("_", col("user_id"), col("form_id"))))
            
    
    def process_data(self,output_path):
        try:
            print("Step 1: Loading Forms State Data...")
            notification_queue = self.read_postgres_table(self.config.dwpeerValidationNotificationQueue).select(col("notification_id"))
            formHistoryDF = self.read_postgres_table(self.config.dwpeerValidationFormStateTable)
            print("✅ Step 1 Complete")
            print("Step 2: Loading Forms Data from Elasticsearch...")
            context_type = ["peerValidationSurvey"]
            fields = ["formId","contextType","title","version", "status", "createdBy", "additionalProperties","createdFor","endDate","createdDate"]
            query = {"bool": {"must": [{"match": {"contextType": pc}} for pc in context_type]}}

            formsDF = utils.read_elasticsearch_data_scroll(
                self.spark, 
                self.config.sparkIGotElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "fs-forms-alias-v2",
                fields = fields,
                query = query
            )
            formsDF = formsDF.withColumn("endDate",from_unixtime(col("endDate")/1000).cast("timestamp")) \
                .withColumn("createdDate",from_unixtime(col("createdDate")/1000).cast("timestamp")) \
                    .filter(col("status") == "Active") \
            .select(
                col("formId").alias("form_id"),
                "title",
                "createdBy",
                "createdFor",
                "endDate",
                "createdDate",
                col("additionalProperties.identifier").alias("course_id"),
                col("additionalProperties.triggerAfter").cast("int").alias("min_trigger_days"),
                col("additionalProperties.completionLookBack").cast("int").alias("max_trigger_days"),
                col("additionalProperties.thumbnail").alias("thumbnail"),
                col("additionalProperties.isSpvCreated").alias("is_spv_created")
            )
            enrolmentDF, userOrgDF, courseDetailsDF = self.load_parquet_data()
            print("✅ Step 2 Complete")
            print("Step 3: Joining Forms with History and Filtering Eligible Forms...")
            formsDF = self.filter_forms_by_state(formsDF, formHistoryDF)
            print("✅ Step 3 Complete")
            print("Step 4: Splitting Forms into SPV and MDO and Unifying...")
            peervalidationForms = self.expand_forms(formsDF)
            print("✅ Step 4 Complete")
            print("Step 5: Calculating Trigger Windows for Eligible Forms...")
            peervalidationForms = self.add_trigger_windows(peervalidationForms)
            print("✅ Step 5 Complete")
            print("Step 6: Filtering Eligible Users...")
            eligibleUsersDF = self.compute_eligible_users(peervalidationForms,enrolmentDF,userOrgDF,courseDetailsDF)
            print("✅ Step 6 Complete")
            print("Step 7: Removing Already Notified Users from Eligible Users...")
            eligibleUsersDF = eligibleUsersDF.join(notification_queue, on="notification_id", how="left_anti")
            print("✅ Step 7 Complete")
            print("Step 8: Building Notification Payload and Saving to DB...")
            eligibleUsersDF = eligibleUsersDF.withColumn(
                "payload",
                to_json(
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
                                    col("form_id").alias("formId"),
                                    col("course_id").alias("contextId"),
                                    col("course_name").alias("courseName"),
                                    lit(False).alias("isSurveySubmitted"),
                                    date_format(col("firstCompletedOn_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("completionDate"),
                                    col("createdBy").alias("surveyCreatedById"),
                                    col("title").alias("surveyName"),
                                    date_format(col("endDate"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("surveyEndDate"),
                                    col("fullName").alias("learnerName"),
                                    col("form_org_id").alias("contextOrgId"),
                                    col("thumbnail")
                                )
                            ).alias("data"),
                            concat(lit("Peer validation survey is now available for '"), col("course_name"), lit("'.")).alias("body")
                        ).alias("message")
                    )
                )
            ) \
            .withColumn("status", lit("PENDING")) \
            .withColumn("error_message", lit(None)) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp()) \
            .withColumn("first_trigger_end", col("first_trigger_end")) \
            .select(
                col("notification_id").cast("string"),
                col("user_id").cast("string"),
                lit("PEER_VALIDATION").alias("event_type").cast("string"),
                col("form_id").cast("string"),
                col("course_id").cast("string"),
                col("course_name").cast("string"),
                col("firstCompletedOn_date").alias("first_completed_on").cast("timestamp"),
                col("status").cast("string"),
                col("error_message").cast("string"),
                col("payload").cast("string"),
                col("first_trigger_end").cast("timestamp"), 
                col("created_at").cast("timestamp"),
                col("updated_at").cast("timestamp")
            )
            count = eligibleUsersDF.count()
            self.write_postgres_table(eligibleUsersDF, self.config.dwpeerValidationNotificationQueue, mode="append")
            print(f"Step 8 Complete - {count} notifications inserted into queue.")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise
    
def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Peer Validation Eligible Users Model") \
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
    print(f"[START] Peer Validation Eligible Users processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
    model = PeerValidationEligibleUsers(spark,config)
    model.process_data(output_path)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Peer Validation Eligible Users completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()