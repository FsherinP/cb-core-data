import findspark
findspark.init()
import os
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (col, lit, coalesce, when, expr, format_string, date_format, current_timestamp, to_date, from_json, explode_outer, greatest, size,min as F_min, max as F_max, count as F_count, sum as F_sum,broadcast, trim)
from pyspark.sql.types import ( LongType, DoubleType, DateType)
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils import utils
from util import schemas
from jobs.config import get_environment_config
from jobs.default_config import create_config


from constants.ParquetFileConstants import ParquetFileConstants

class CourseReportModel:    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.CourseReportModel"
        
    def name(self):
        return "CourseReportModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    @staticmethod
    def duration_format(df, in_col, out_col=None):
        out_col_name = out_col if out_col is not None else in_col
        
        return df.withColumn(out_col_name,
            when(col(in_col).isNull(), lit(""))
            .otherwise(
                format_string("%02d:%02d:%02d",
                    expr(f"{in_col} / 3600").cast("int"),
                    expr(f"{in_col} % 3600 / 60").cast("int"),
                    expr(f"{in_col} % 60").cast("int")
                )
            )
        )
    def process_data(self, spark,config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            course_categories= config.courseCategoriesToSelect

            
            allCourseProgramDetailsDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(col("courseCategory").isin(course_categories))
            contentHierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE).withColumnRenamed("identifier", "courseID")
            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE) 

            getContentResourceWithCategoryDF = contentHierarchyDF \
                .join(allCourseProgramDetailsDF, ["courseID"], "inner") \
                .select(
                    *[col(c) for c in contentHierarchyDF.columns],
                    allCourseProgramDetailsDF["category"]
                )
            
            resultDF = getContentResourceWithCategoryDF \
                .filter(col("category").isin(["Program", "Curated Program", "Course"])) \
                .withColumn("hierarchy", from_json(col("hierarchy"), schemas.hierarchySchema)) \
                .select(
                    col("courseID").alias("content_id"),
                    col("category"),
                    explode_outer(col("hierarchy.children")).alias("first_level_child")
                ).withColumn("second_level_child", explode_outer(col("first_level_child.children"))) \
                .select(
                    col("content_id"),
                    
                    # Resource ID logic
                    when(col("category").isin(["Program", "Curated Program"]), 
                        col("first_level_child.identifier"))
                    .otherwise(
                        when(col("first_level_child.primaryCategory") == "Course Unit", 
                            col("second_level_child.identifier"))
                        .otherwise(col("first_level_child.identifier"))
                    ).alias("resource_id"),
                    
                    # Resource Name logic
                    when(col("category").isin(["Program", "Curated Program"]), 
                        col("first_level_child.name"))
                    .otherwise(
                        when(col("first_level_child.primaryCategory") == "Course Unit", 
                            col("second_level_child.name"))
                        .otherwise(col("first_level_child.name"))
                    ).alias("resource_name"),
                    
                    # Resource Type logic
                    when(col("category").isin(["Program", "Curated Program"]), 
                        col("first_level_child.primaryCategory"))
                    .otherwise(
                        when(col("first_level_child.primaryCategory") == "Course Unit", 
                            col("second_level_child.primaryCategory"))
                        .otherwise(col("first_level_child.primaryCategory"))
                    ).alias("resource_type"),
                    
                    # Resource Duration logic
                    when(col("category").isin(["Program", "Curated Program"]), 
                        col("first_level_child.duration"))
                    .otherwise(
                        when(col("first_level_child.primaryCategory") == "Course Unit",
                            coalesce(col("second_level_child.duration"), 
                                    col("second_level_child.expectedDuration")))
                        .otherwise(coalesce(col("first_level_child.duration"), 
                                        col("first_level_child.expectedDuration")))
                    ).alias("resource_duration")
                )
            
            distinctDF  = contentDFUtil.duration_format(resultDF, "resource_duration") \
                .filter((col("resource_id").isNotNull()) & (col("resource_id") != "")) \
                .dropDuplicates() \
                .withColumn("data_last_generated_on", currentDateTime) \
                .cache()  # Cache the final result since it's used multiple times
            # Generate report path
            report_path=f"{config.localReportDir}/{config.courseReportPath}/{today}"

            # Write to warehouse - single coalesce operation
            distinctDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwContentResourceTable}")

            courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")
            userEnrolmentDF=enrolmentDF.join(
                courseResCountDF, 
                on=["courseID"], 
                how="left"
            )
            allCBPCompletionWithDetailsDF = enrolmentDFUtil.calculateCourseProgress(userEnrolmentDF).persist()

           # Aggregate course completion details
            aggregatedDF = allCBPCompletionWithDetailsDF.groupBy("courseID") \
                .agg(
                    F_min("courseCompletedTimestamp").alias("earliestCourseCompleted"),
                    F_max("courseCompletedTimestamp").alias("latestCourseCompleted"),
                    F_count(lit(1)).alias("enrolledUserCount"),
                    F_sum(when(col("userCourseCompletionStatus") == "in-progress", 1).otherwise(0)).alias("inProgressCount"),
                    F_sum(when(col("userCourseCompletionStatus") == "not-started", 1).otherwise(0)).alias("notStartedCount"),
                    F_sum(when(col("userCourseCompletionStatus") == "completed", 1).otherwise(0)).alias("completedCount"),
                    F_sum("issuedCertificateCountPerContent").alias("totalCertificatesIssued")
                ) \
                .withColumn("firstCompletedOn", to_date(col("earliestCourseCompleted"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("lastCompletedOn", to_date(col("latestCourseCompleted"),  ParquetFileConstants.DATE_FORMAT))
            

            allCBPAndAggDF = allCourseProgramDetailsDF.join(aggregatedDF, ["courseID"], "left")

            courseBatchDF=spark.read.parquet(ParquetFileConstants.BATCH_SELECT_PARQUET_FILE) 

            curatedCourseDataDFWithBatchInfo = allCBPAndAggDF \
            .join(
                broadcast(
                    allCourseProgramDetailsDF
                    .filter(col("category") == "Blended Program")
                    .select("courseID","difficultyLevel")
                    .join(courseBatchDF, ["courseID"], "inner")
                    .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
                ), 
                ["courseID"], 
                "left"
            )

            fullDF = contentDFUtil.duration_format(curatedCourseDataDFWithBatchInfo,"courseDuration") \
            .filter(col("courseStatus").isin(["Live", "Draft", "Retired", "Review"])) \
            .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastStatusChangedOn", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("ArchivedOn", when(col("courseStatus") == "Retired", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT))) \
            .withColumn("Report_Last_Generated_On", currentDateTime)

            marketPlaceEnrolmentsDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE) \
            .withColumn("issuedCertificateCountPerContent", 
                        when(size(col("issued_certificates")) > 0, lit(1)).otherwise(lit(0))) \
            .groupBy("content_id") \
            .agg(
                F_count(lit(1)).alias("enrolledUserCount"),
                F_sum(when(col("status") == 1, 1).otherwise(0)).alias("inProgressCount"),
                F_sum(when(col("status") == 0, 1).otherwise(0)).alias("notStartedCount"),
                F_sum(when(col("status") == 2, 1).otherwise(0)).alias("completedCount"),
                F_sum(col("issuedCertificateCountPerContent")).alias("totalCertificatesIssued"),
                F_min("completedon").alias("earliestCompletedOn"),
                F_max("completedon").alias("latestCompletedOn")
            ) 

            marketPlaceContentsDF= spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
            
            marketPlaceContentWithEnrolmentsDF = contentDFUtil.duration_format(marketPlaceContentsDF, "courseDuration") \
                .join(marketPlaceEnrolmentsDF, ["content_id"], "outer") \
                .withColumn("firstCompletedOn", to_date(col("earliestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("lastCompletedOn", to_date(col("latestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                    col("content_id"),
                    col("courseName"),
                    col("courseDuration"),
                    col("courseLastPublishedOn"),
                    col("courseOrgID"),
                    col("courseOrgName"),
                    col("category"),
                    col("enrolledUserCount"), 
                    col("completedCount"), 
                    col("notStartedCount"), 
                    col("inProgressCount"),
                    col("courseStatus"),
                    col("totalCertificatesIssued"),
                    col("firstCompletedOn"), 
                    col("lastCompletedOn"),
                    col("data_last_generated_on")
                )
            

            marketPlaceContentWarehouseDF = marketPlaceContentWithEnrolmentsDF.select(
                col("content_id"),
                col("courseOrgID").alias("content_provider_id"),
                col("courseOrgName").alias("content_provider_name"),
                col("courseName").alias("content_name"),
                col("category").alias("content_type"),
                lit("Not Available").alias("batch_id"),  
                lit("Not Available").alias("batch_name"),  
                lit(None).cast(DateType()).alias("batch_start_date"),  
                lit(None).cast(DateType()).alias("batch_end_date"),  
                col("courseDuration").alias("content_duration"),
                lit("Not Available").alias("content_rating"),  
                to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("last_published_on"),
                lit(None).cast(DateType()).alias("content_retired_on"),  
                col("courseStatus").alias("content_status"),
                lit("Not Available").alias("resource_count"),  
                col("totalCertificatesIssued").alias("total_certificates_issued"),
                lit("Not Available").alias("content_substatus"),  
                lit("Not Available").alias("language"),  
                lit("External Content").alias("content_sub_type"),
                lit("0").alias("scorm_flag"),
                lit(None).alias("difficulty_level"),
                col("data_last_generated_on")
            )

            marketPlaceContentMdoReportDF = marketPlaceContentWithEnrolmentsDF \
            .select(
                col("courseStatus").alias("Content_Status"),
                col("courseOrgName").alias("Content_Provider"),
                col("courseName").alias("Content_Name"),
                col("category").alias("Content_Type"),
                lit("Not Available").alias("Batch_Id"),  
                lit("Not Available").alias("Batch_Name"),  
                lit(None).cast(DateType()).alias("Batch_Start_Date"),  
                lit(None).cast(DateType()).alias("Batch_End_Date"),  
                col("courseDuration").alias("Content_Duration"),
                col("enrolledUserCount").cast(LongType()).alias("Enrolled"),
                col("notStartedCount").cast(LongType()).alias("Not_Started"),
                col("inProgressCount").cast(LongType()).alias("In_Progress"),
                col("completedCount").cast(LongType()).alias("Completed"),
                lit(None).cast(DoubleType()).alias("Content_Rating"),  # New column with null
                to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("Last_Published_On"),
                col("firstCompletedOn").alias("First_Completed_On"),
                col("lastCompletedOn").alias("Last_Completed_On"),
                lit(None).cast(DateType()).alias("Content_Retired_On"),  
                col("totalCertificatesIssued").cast(LongType()).alias("Total_Certificates_Issued"),
                col("courseOrgID").alias("mdoid"),
                col("data_last_generated_on").alias("Report_Last_Generated_On")
            ) 
            

            platformContentMdoReportDF = fullDF \
                .select(
                    col("courseStatus").alias("Content_Status"),
                    when(
                        col("courseOrgName").isNotNull() & 
                        (col("courseOrgName") != "") &
                        (col("courseOrgName") != " "),
                        col("courseOrgName")
                    ).otherwise(col("contentCreator")).alias("Content_Provider"),
                    col("courseName").alias("Content_Name"),
                    col("category").alias("Content_Type"),
                    col("batchID").alias("Batch_Id"),
                    col("courseBatchName").alias("Batch_Name"),
                    col("courseBatchStartDate").alias("Batch_Start_Date"),
                    col("courseBatchEndDate").alias("Batch_End_Date"),
                    col("courseDuration").alias("Content_Duration"),
                    col("enrolledUserCount").alias("Enrolled"),
                    col("notStartedCount").alias("Not_Started"),
                    col("inProgressCount").alias("In_Progress"),
                    col("completedCount").alias("Completed"),
                    col("rating").alias("Content_Rating"),
                    col("courseLastPublishedOn").alias("Last_Published_On"),
                    col("firstCompletedOn").alias("First_Completed_On"),
                    col("lastCompletedOn").alias("Last_Completed_On"),
                    col("ArchivedOn").alias("Content_Retired_On"),
                    col("totalCertificatesIssued").alias("Total_Certificates_Issued"),
                    col("courseOrgID").alias("mdoid"),
                    col("Report_Last_Generated_On")
                ) 
            
            mdoReportDF = platformContentMdoReportDF.union(marketPlaceContentMdoReportDF)

            distinct_orgids = mdoReportDF \
                      .select("mdoid") \
                      .distinct() \
                      .collect()  

            orgid_list = [row.mdoid for row in distinct_orgids]

            print("📝 Writing CSV reports...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoReportDF, 
                report_path,
                'mdoid',
                f"{config.localReportDir}/temp/course_report/{today}",
                orgid_list,
                csv_filename=config.courseReport
            )
            contentHierarchyExploded = contentHierarchyDF.withColumn("hierarchy", from_json(col("hierarchy"), schemas.hierarchySchema))

            scorm_detection_df = contentHierarchyExploded \
                .withColumn("level1_child", explode_outer(col("hierarchy.children"))) \
                .withColumn("level2_child", explode_outer(col("level1_child.children"))) \
                .withColumn("lvl1_scorm", when(col("level1_child.mimeType").endswith("html-archive"), 1).otherwise(0)) \
                .withColumn("lvl2_scorm", when(col("level2_child.mimeType").endswith("html-archive"), 1).otherwise(0)) \
                .groupBy("courseID") \
                .agg(
                    F_max("lvl1_scorm").alias("lvl1_flag"),
                    F_max("lvl2_scorm").alias("lvl2_flag")
                ) \
                .withColumn("scorm_flag", greatest(col("lvl1_flag"), col("lvl2_flag"))) \
                .select("courseID", "scorm_flag")

            platformContentWarehouseDF = fullDF \
                .join(scorm_detection_df, ["courseID"], "left") \
                .na.fill(0, ["scorm_flag"]) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                    col("courseID").alias("content_id"),
                    col("courseOrgID").alias("content_provider_id"),
                    when(
                        col("courseOrgName").isNotNull() & 
                        (col("courseOrgName") != "") &
                        (col("courseOrgName") != " "),
                        col("courseOrgName")
                    ).otherwise(col("contentCreator")).alias("content_provider_name"),
                    col("courseName").alias("content_name"),
                    col("category").alias("content_type"),
                    col("batchID").alias("batch_id"),
                    col("courseBatchName").alias("batch_name"),
                    col("courseBatchStartDate").alias("batch_start_date"),
                    col("courseBatchEndDate").alias("batch_end_date"),
                    col("courseDuration").alias("content_duration"),
                    col("rating").alias("content_rating"),
                    date_format(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("last_published_on"),
                    col("ArchivedOn").alias("content_retired_on"),
                    col("courseStatus").alias("content_status"),
                    col("courseResourceCount").alias("resource_count"),
                    col("totalCertificatesIssued").alias("total_certificates_issued"),
                    col("courseReviewStatus").alias("content_substatus"),
                    col("contentLanguage").alias("language"),
                    col("courseCategory").alias("content_sub_type"),
                    col("scorm_flag"),
                    col('difficultyLevel').alias('difficulty_level'),
                    col("data_last_generated_on")
                )
            orgComputedDF = spark.read.parquet(ParquetFileConstants.ORG_SELECT_PARQUET_FILE) \
                .select(col("orgId").alias("content_provider_id"), 
                        col("orgName").alias("content_provider_name"))
            es_final_assessment_df = spark.read.parquet(ParquetFileConstants.FINAL_ASSESSMENT_PARQUET_FILE)
            es_final_assessment_df = es_final_assessment_df.select(
                col("Identifier").alias("content_id"),
                col("createdFor").getItem(0).alias("content_provider_id"),
                col("name").alias("content_name"), 
                col("primaryCategory").alias("content_type"),
                lit(None).alias("batch_id"),
                lit(None).alias("batch_name"),
                lit(None).alias("batch_start_date"),
                lit(None).alias("batch_end_date"),
                col("expectedDuration").alias("content_duration"),
                lit(None).alias("content_rating"),
                col("lastPublishedOn").alias("last_published_on"),
                lit(None).alias("content_retired_on"),
                col("status").alias("content_status"),
                lit(None).alias("resource_count"),
                lit(None).alias("total_certificates_issued"), 
                col("reviewStatus").alias("content_substatus"),
                col("language").getItem(0).alias("language"),
                col("contextCategory").alias("content_sub_type"),
                lit(0).alias("scorm_flag"),
                lit(None).alias("difficulty_level"),
                lit(currentDateTime).alias("data_last_generated_on")
            )
            es_final_assessment_df = es_final_assessment_df.join(
                orgComputedDF,
                on="content_provider_id",
                how="left"
            ).select(
                es_final_assessment_df["*"],orgComputedDF["content_provider_name"]
            )
            platformContentWarehouseDF = platformContentWarehouseDF.unionByName(es_final_assessment_df)

            df_warehouse = platformContentWarehouseDF.union(marketPlaceContentWarehouseDF)
            df_warehouse.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwCourseTable}")

        except Exception as e:
            print(f"❌ Error occurred during CourseReportModel processing: {str(e)}")
            raise e
            

def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("Course Report Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "30g") \
        .config("spark.driver.memory", "25g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("es.nodes", config.sparkElasticsearchConnectionHost) \
        .config("es.port", config.sparkElasticsearchConnectionPort) \
        .config("es.index.auto.create", "false") \
        .config("es.nodes.wan.only", "true") \
        .config("es.nodes.discovery", "false") \
        .getOrCreate()
    
    start_time = datetime.now()
    print(f"[START] CourseReportModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = CourseReportModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] CourseReportModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()
if __name__ == "__main__":
   main()