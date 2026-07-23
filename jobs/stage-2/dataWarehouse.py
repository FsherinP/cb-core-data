import findspark

findspark.init()

import time
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import (col, lower, when, lit, expr, concat_ws, explode_outer, from_json, to_date, regexp_replace,
                                   current_timestamp, date_format, round, coalesce, broadcast, size, map_keys,
                                   map_values)
from zipfile import ZipFile, ZIP_DEFLATED
import pyspark.sql.functions as F
import shutil
import subprocess
import sys
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from jobs.config import get_environment_config
from jobs.default_config import create_config


class DataWarehouseModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report..DataWarehouse"

    def name(self):
        return "DataWarehouse"

    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            today = self.get_date()
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            warehouse_path = config.warehouseReportDir
            output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
            postgres_url = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"
            today_date = datetime.today().strftime('%Y-%m-%d')

            print("📚 Step 1: Loading User Details Data...")
            userDetailsDF = spark.read.parquet(f"{warehouse_path}/{config.dwUserTable}") \
                .withColumn("status", col("status").cast("int")) \
                .withColumn("no_of_karma_points", col("no_of_karma_points").cast("int")) \
                .withColumn("marked_as_not_my_user", col("marked_as_not_my_user").cast("boolean")) \
                .withColumn("total_event_learning_hours", col("total_event_learning_hours").cast("double")) \
                .withColumn("total_content_learning_hours", col("total_content_learning_hours").cast("double")) \
                .withColumn("total_learning_hours", col("total_learning_hours").cast("double"))
            self.write_postgres_table(userDetailsDF, postgres_url, config.dwUserTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("User details table updated")


            print("📚 Step 2: Loading Content Data...")
            contentDF = spark.read.parquet(f"{warehouse_path}/{config.dwCourseTable}") \
                .withColumn("resource_count", col("resource_count").cast("int")) \
                .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int")) \
                .withColumn("content_rating", col("content_rating").cast("float")) \
                .dropDuplicates(["content_id"])
            self.write_postgres_table(contentDF, postgres_url,config.dwCourseTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("Content table updated")

            print("📚 Step 3: Loading Assessment Data...")
            assessment = spark.read.parquet(f"{warehouse_path}/{config.dwAssessmentTable}") \
                .withColumn("score_achieved", col("score_achieved").cast("float")) \
                .withColumn("overall_score", col("overall_score").cast("float")) \
                .withColumn("cut_off_percentage", col("cut_off_percentage").cast("float")) \
                .withColumn("total_question", col("total_question").cast("int")) \
                .withColumn("number_of_incorrect_responses", col("number_of_incorrect_responses").cast("int")) \
                .withColumn("number_of_retakes", col("number_of_retakes").cast("int")) \
                .filter(col("content_id").isNotNull())
            self.write_postgres_table(assessment, postgres_url, config.dwAssessmentTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("Assessment table updated")

            print("📚 Step 4: Loading BP Enrolments Data...")
            bp_enrolments = spark.read.parquet(f"{warehouse_path}/{config.dwBPEnrollmentsTable}") \
                .withColumn("component_progress_percentage", col("component_progress_percentage").cast("float")) \
                .withColumn("offline_session_date", to_date(col("offline_session_date"))) \
                .withColumn("component_completed_on", to_date(col("component_completed_on"))) \
                .withColumn("last_accessed_on", to_date(col("last_accessed_on"))) \
                .withColumnRenamed("instructor(s)_name", "instructors_name") \
                .filter(col("content_id").isNotNull()) \
                .filter(col("user_id").isNotNull()) \
                .filter(col("batch_id").isNotNull())

            self.write_postgres_table(bp_enrolments, postgres_url, config.dwBPEnrollmentsTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("BP Enrolments table updated")

            print("📚 Step 5: Loading Content Resource Data...")
            content_resource = spark.read.parquet(f"{warehouse_path}/{config.dwContentResourceTable}")
            self.write_postgres_table(content_resource, postgres_url, config.dwContentResourceTable,
                                      config.dwPostgresUsername, config.dwPostgresCredential)
            print("Content resource table updated")

            print("📚 Step 6: Loading CB Plan Data...")
            cb_plan = spark.read.parquet(f"{warehouse_path}/{config.dwCBPlanTable}")
            self.write_postgres_table(cb_plan, postgres_url, config.dwCBPlanTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("CB Plan table updated")

            print("📚 Step 8: Loading Org Hierarchy Data...")
            org_hierarchy = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwOrgTable}")
            self.write_postgres_table(org_hierarchy, postgres_url, config.dwOrgTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("Org hierarchy table updated")

            print("📚 Step 9: Loading KCM Content Data...")
            kcm_content = spark.read.parquet(f"{warehouse_path}/{config.dwKcmContentTable}") \
                .select("course_id", "competency_area_id", "competency_theme_id", "competency_sub_theme_id",
                        "data_last_generated_on")
            self.write_postgres_table(kcm_content, postgres_url, config.dwKcmContentTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("KCM Content table updated")

            print("📚 Step 10: Loading KCM Dict Data...")
            kcm_dict = spark.read.parquet(f"{warehouse_path}/{config.dwKcmDictionaryTable}")
            self.write_postgres_table(kcm_dict, postgres_url, config.dwKcmDictionaryTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("KCM Dict table updated")

            print("📚 Step 11: Loading Events Data...")
            events = spark.read.parquet(f"{config.warehouseReportDir}/event_details")
            self.write_postgres_table(events, postgres_url, config.dwEventsTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("Events table updated")

            print("📚 Step 12: Loading Events Enrolments Data...")
            eventsEnrolmentDataDFWithKarmaPoints = spark.read.parquet(f"{config.warehouseReportDir}/event_enrolment_details")
            self.write_postgres_table(eventsEnrolmentDataDFWithKarmaPoints, postgres_url, config.dwEventsEnrolmentTable,
                                      config.dwPostgresUsername, config.dwPostgresCredential)
            #eventsEnrolmentDataDFWithKarmaPoints.show()
            print("Events Enrolments table updated")

            print("📚 Step 7: Loading User Enrolments Data...")
            enrolments = spark.read.parquet(f"{warehouse_path}/{config.dwEnrollmentsTable}") \
                .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float")) \
                .withColumn("user_rating", col("user_rating").cast("float")) \
                .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int")) \
                .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean")) \
                .filter(col("content_id").isNotNull())
            self.write_postgres_table(enrolments, postgres_url, config.dwEnrollmentsTable, config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            print("User enrolments table updated")

            #course_completion_survey_details_df = spark.read.parquet(f"{warehouse_path}/{config.dwCourseCompletionSurveryTable}")
            #.withColumn("improvement_suggestions",regexp_replace(col("improvement_suggestions"), "[\\x00]", "")).withColumn("improvement_suggestions",regexp_replace(col("improvement_suggestions"), "\\r", ""))
            #self.write_postgres_table(course_completion_survey_details_df, postgres_url, config.dwCourseCompletionSurveryTable,
            #                          config.dwPostgresUsername, config.dwPostgresCredential)

            print("✅ Processing completed successfully!")


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
            .mode(mode) \
            .save()


def create_spark_session_with_packages(config):
    # Set environment variables for PySpark to find packages
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    spark = SparkSession.builder \
        .appName('DataWarehousetModel') \
        .master("local[*]") \
        .config("spark.executor.memory", '180g') \
        .config("spark.driver.memory", '30g') \
        .config("spark.executor.memoryFraction", '0.7') \
        .config("spark.storage.memoryFraction", '0.2') \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", 'snappy') \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .config("es.nodes", config.sparkElasticsearchConnectionHost) \
        .config("es.port", config.sparkElasticsearchConnectionPort) \
        .config("es.index.auto.create", "false") \
        .config("es.nodes.wan.only", "true") \
        .config("es.nodes.discovery", "false") \
        .getOrCreate()

    return spark


def main():
    # Initialize Spark Session with optimized settings for caching
    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Initialize Spark Session with optimizations from config
    spark = create_spark_session_with_packages(config)
    start_time = datetime.now()
    print(f"[START] DataWarehouse processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = DataWarehouseModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] DataWarehouse completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")


if __name__ == "__main__":
    main()
