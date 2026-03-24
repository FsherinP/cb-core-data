import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, date_format, date_add, to_date,lit,explode, date_sub, size, from_unixtime,when)
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
    
    def write_parquet(self, df: "DataFrame", path: str, partition_cols: list = None, mode: str = "overwrite"):
        """Write DataFrame to Parquet with optimization"""
        writer = df.coalesce(16) 
        
        if partition_cols:
            writer = writer.write.partitionBy(*partition_cols)
        else:
            writer = writer.write
            
        writer.mode(mode) \
              .option("compression", "snappy") \
              .parquet(path)
    
    def process_data(self,output_path):
        try:
            print("Step 1: Loading Forms State Data...")
            notifiedUsersDF = self.read_postgres_table(self.config.dwnotifiedUsersTable)
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
                "fs-forms",
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
            print("✅ Step 2 Complete")
            print("Step 3: Joining Forms with History and Filtering Eligible Forms...")
            formsDF = formsDF.join(formHistoryDF, on="form_id", how="left") \
                .withColumn("published_date",col("createdDate").cast("date")) \
                    .filter(
                (col("last_processed_date").isNull()) | 
                (date_sub(col("endDate"), col("min_trigger_days")) > col("last_processed_date"))
            ).drop(formHistoryDF.form_id)
            print("✅ Step 3 Complete")
            print("Step 4: Splitting Forms into SPV and MDO and Unifying...")
            spvForms = formsDF.filter(col("is_spv_created") == True).withColumn("form_org_id", lit(None)).drop("createdFor")
            mdoForms = formsDF.filter((col("is_spv_created") == False) & (size(col("createdFor")) > 0)).withColumn("org", explode(col("createdFor"))) \
                .withColumn("form_org_id", col("org.orgId")).drop("org","createdFor")
            peervalidationForms = spvForms.union(mdoForms)
            print("✅ Step 4 Complete")
            print("Step 5: Calculating Trigger Windows for Eligible Forms...")
            peervalidationForms = peervalidationForms.withColumn('first_trigger_start', when(col('last_processed_date').isNull(), date_sub(col("published_date"), col("max_trigger_days"))) \
                                                                 .otherwise(date_add(col("last_processed_date"), 1))) \
                                                                 .withColumn('first_trigger_end', when(col('last_processed_date').isNull(), date_sub(col("published_date"), col("min_trigger_days"))) \
                                                                             .otherwise(date_add(col("last_processed_date"), 1)))
            print("✅ Step 5 Complete")
            print("Step 6: Loading User, Course and Enrolment Data...")
            courseDetailsDF = self.spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).select(col("courseID").alias("course_id"),col("courseName").alias("course_name"))
            enrolmentDF = self.spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).select(
                    col("userID").alias("user_id"), 
                    col("courseID").alias("course_id"),
                    col("firstCompletedOn"),
                    col("certificateID"),
                    col("dbCompletionStatus")
                ) \
                .withColumn("firstCompletedOn_date",to_date(date_format(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT))) \
                    .filter((col("certificateID").isNotNull()) & (col("certificateID") != "") & (col('dbCompletionStatus') == '2'))
            userOrgDF = self.spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select(col("userID").alias("user_id"), col("userOrgID"), col("fullName"))
            print("✅ Step 6 Complete")
            print("Step 7: Joining User Enrolments with Forms and Filtering Eligible Users...")
            userEnrolmentOrgDF = enrolmentDF.alias("left").join(userOrgDF.alias("right"), on= "user_id", how = "left").select("left.*","right.userOrgID","right.fullName")

            eligibleUsersDF = userEnrolmentOrgDF.join(peervalidationForms, on="course_id", how="inner") \
                .filter(
                (col("form_org_id").isNull()) | ((col("form_org_id").isNotNull()) & (col("userOrgID") == col("form_org_id"))))
            
            eligibleUsersDF = eligibleUsersDF.filter(
                (col("firstCompletedOn_date").between(col("first_trigger_start"), col("first_trigger_end")))
            )
            print("✅ Step 7 Complete")
            print("Step 8: Removing Already Notified Users and Joining with Course Details...")
            eligibleUsersDF = eligibleUsersDF.alias("eu").join(
                notifiedUsersDF.alias("nu").select("user_id", "form_id"),
                on=["user_id", "form_id"],
                how="left_anti"
            ).select("eu.*")
            eligibleUsersDF = eligibleUsersDF.alias("eu").join(
                courseDetailsDF.alias("cd").select(col("course_id"),col("course_name")),
                on="course_id",
                how="left"
            ).select("eu.*","cd.course_name")
            print("✅ Step 8 Complete")
            print("Step 9: Selecting and Renaming Final Columns to Save...")
            eligibleUsersDF =  eligibleUsersDF.select(
                                col("user_id"),
                                col("form_id"),
                                col("course_id"),
                                col("title"),
                                col("createdBy").alias("created_by"),
                                col("endDate").alias("survey_end_date"),
                                col("firstCompletedOn_date"),
                                col("fullName").alias("user_full_name"),
                                col("course_name"),
                                col("form_org_id"),
                                col("thumbnail"),
                                col("first_trigger_end")
                            )
            self.write_parquet(eligibleUsersDF, f"{output_path}/peerValidationEligibleUsers")
            print("✅ Step 9 Complete - Eligible users data saved to Parquet.")
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