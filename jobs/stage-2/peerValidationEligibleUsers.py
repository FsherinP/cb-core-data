import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, current_timestamp, to_date,lit,explode, current_date, date_sub, size, from_unixtime)
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
            yesterday = date_sub(current_date(), 1)

            notifiedUsersDF = self.read_postgres_table(self.config.dwnotifiedUsersTable)
            context_type = ["peerValidationSurvey"]
            must_clause = ",".join([f'{{"match":{{"contextType":"{pc}"}}}}' for pc in context_type])
            fields = ["formId","contextType","title","version", "status", "createdBy", "additionalProperties","createdFor","endDate","createdDate"]
            fields_clause = ",".join([f'"{f}"' for f in fields])
            query = {"bool": {"must": [{"match": {"contextType": pc}} for pc in context_type]}}

            formsDF = utils.read_elasticsearch_data_scroll(
                self.spark, 
                self.config.sparkElasticsearchConnectionHost,
                self.config.sparkElasticsearchConnectionPort,
                "fs-forms",
                fields = fields,
                query = query
            )
            formsDF = formsDF.withColumn("endDate",from_unixtime(col("endDate")/1000).cast("timestamp")) \
                .withColumn("createdDate",from_unixtime(col("createdDate")/1000).cast("timestamp")) \
                    .filter(
                (col("status") == "Active") &
                (col("endDate") >= current_timestamp())
            ).select(
                "formId",
                "title",
                "createdBy",
                "createdFor",
                "endDate",
                "createdDate",
                col("additionalProperties.identifier").alias("course_id"),
                col("additionalProperties.triggerAfter").cast("int").alias("minTriggerDays"),
                col("additionalProperties.completionLookBack").cast("int").alias("maxTriggerDays")
            )

            spvForms = formsDF.filter(size(col("createdFor")) == 0).withColumn("formOrgId", lit(None))
            mdoForms = formsDF.filter(size(col("createdFor")) > 0).withColumn("org", explode(col("createdFor"))) \
                .withColumn("formOrgId", col("org.orgId")).drop("org")
            peervalidationForms = spvForms.unionByName(mdoForms)

            peervalidationForms = peervalidationForms.withColumn("last_trigger_date", date_sub(col("endDate").cast("date"), col("minTriggerDays").cast("int"))) \
                .filter(col("last_trigger_date") >= yesterday)

            peervalidationForms = peervalidationForms.withColumn("publishedDate",col("createdDate")).cast("date")
            
            courseDetailsDF = self.spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            enrolmentDF = self.spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).withColumn("firstCompletedOn",
                            to_date(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT)).filter(col("certificateID").isNotNull())
            userOrgDF = self.spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            userEnrolmentOrgDF = enrolmentDF.alias("left").join(userOrgDF.alias("right"), on= "userID", how = "left").select("left.*","right.userOrgID","right.fullName")

            eligibleUsersDF = userEnrolmentOrgDF.join(peervalidationForms, userEnrolmentOrgDF.content_id == peervalidationForms.course_id) \
                .filter(
                (col("formOrgId").isNull()) | ((col("formOrgId").isNotNull()) & (col("userOrgID") == col("formOrgId"))))
            
            eligibleUsersDF = eligibleUsersDF.withColumn("first_trigger_start", date_sub(col("publishedDate"), col("maxTriggerDays").cast("int"))) \
                                     .withColumn("first_trigger_end", date_sub(col("publishedDate"), 1))
            
            eligibleUsersDF = eligibleUsersDF.filter(
                (col("firstCompletedOn").between(col("first_trigger_start"), col("first_trigger_end"))) |  
                ((col("firstCompletedOn") == yesterday) & (yesterday <= col("last_trigger_date"))))

            eligibleUsersDF = eligibleUsersDF.join(
                notifiedUsersDF.select("userID", "formId"),
                on=["userID", "formId"],
                how="left_anti"
            )
            eligibleUsersDF = eligibleUsersDF.join(
                courseDetailsDF.select(col("courseID"),col("courseName")),
                on="courseID",
                how="left"
            )
            eligibleUsersDF =  eligibleUsersDF.select(
                                col("userID").alias("user_id"),
                                col("formId").alias("formId"),
                                col("courseID").alias("course_id"),
                                col("title").alias("title"),
                                col("createdBy").alias("createdBy"),
                                col("endDate").alias("surveyEndDate"),
                                col("firstCompletedOn").alias("first_completed_on"),
                                col("fullName").alias("full_name"),
                                "courseName"
                            )
            self.write_parquet(eligibleUsersDF, f"{output_path}/peerValidationEligibleUsers")

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