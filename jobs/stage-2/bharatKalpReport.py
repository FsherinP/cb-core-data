import findspark
findspark.init()

import os
import sys
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, broadcast


sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.config import get_environment_config
from jobs.default_config import create_config
from constants.ParquetFileConstants import ParquetFileConstants


class BharatKalpReport:
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.BharatKalpModel"

    def name(self):
        return "BharatKalpReport"
    
    def read_bharat_kalp_courses(self):

        df = (self.spark.read.json(self.config.bharatKalpCoursesPath)
            .select(explode(col("individualSection.weekProgress.weeks")).alias("week_key", "week_value"))
            .select(explode(col("week_value.doIds")).alias("content_id"))
            .distinct()
            )

        if df.limit(1).count() == 0:
            raise ValueError("No Bharat Kalp Courses found in the provided path. Please check the configuration.")
        return df
    
    def read_warehouse_data(self, table_name):
        return self.spark.read.parquet(f"{self.config.warehouseReportDir}/{table_name}")
        
    def build_event_report(self, eventsDF, eventEnrolmentsDF, userDF):
        eventEnrolmentsDF = eventEnrolmentsDF.join(broadcast(userDF), eventEnrolmentsDF.user_id == userDF.user_id, "inner").select(eventEnrolmentsDF["*"])
        eventWarehouseDF = eventsDF.join(eventEnrolmentsDF, eventsDF.event_id == eventEnrolmentsDF.event_id, "inner").select(eventEnrolmentsDF["*"])
        return eventWarehouseDF
    
    def build_course_report(self, enrolmentDF, bharatKalpCoursesDF, userDF):
        enrolmentDF = enrolmentDF.join(broadcast(userDF), enrolmentDF.user_id == userDF.user_id, "inner").select(enrolmentDF["*"])
        courseWarehouseDF = enrolmentDF.join(broadcast(bharatKalpCoursesDF), enrolmentDF.content_id == bharatKalpCoursesDF.content_id, "inner").select(enrolmentDF["*"])
        return courseWarehouseDF
    
    def process_data(self):
        print("Step 1: Loading Data ")
        bharatKalpCoursesDF = self.read_bharat_kalp_courses()
        enrolmentDF = self.read_warehouse_data(self.config.dwEnrollmentsTable)
        eventsDF = self.read_warehouse_data("event_details").filter(col("event_tag").isin(self.config.bharat_kalp_event_tags))
        eventEnrolmentsDF = self.read_warehouse_data("event_enrolment_details")
        userDF = self.spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE).filter(col("isBharatKalpMember") == True).select(col("userID").alias("user_id")).distinct()

        print("Step 1: Complete")
        print("Step 2: Processing Bharat Kalp Events with Event Enrolments")
        eventWarehouseDF = self.build_event_report(eventsDF, eventEnrolmentsDF, userDF)
        print("Step 2: Complete")
        print("Step 3: Processing Bharat Kalp Courses with Enrolments")
        courseWarehouseDF = self.build_course_report(enrolmentDF, bharatKalpCoursesDF, userDF)
        print("Step 3: Complete")
        print("Step 4: Writing Bharat Kalp Report to Warehouse")
        courseWarehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{self.config.warehouseReportDir}/bharat_kalp_courses")
        eventWarehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{self.config.warehouseReportDir}/bharat_kalp_events")
        print("Step 4: Complete")
def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Bharat Kalp Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Bharat Kalp Report processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = BharatKalpReport(spark,config)
    model.process_data()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Bharat Kalp Report completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()