from datetime import datetime
import findspark

findspark.init()
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, max as spark_max,
    expr, current_timestamp, broadcast, date_format
)

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.assessment import assessmentDFUtil
from dfutil.content import contentDFUtil
from jobs.config import get_environment_config
from jobs.default_config import create_config


class UserAssessmentModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.UserAssessmentModel"

    def name(self):
        return "UserAssessmentModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_report(self, spark, config):
        """
        Assessment Report Generation with minimal logging for performance
        """
        total_start_time = time.time()

        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            # Stage 1: Load Assessment Data
            print("Stage 1: Loading assessment data...")
            assessmentDF = spark.read.parquet(ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE).filter(
                col("assessCategory").isin("Standalone Assessment"))
            hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
            organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
            print("Stage 1: Complete")

            # Stage 2: Add Hierarchy Information
            print("Stage 2: Adding hierarchy information...")
            assWithHierarchyData = assessmentDFUtil.add_hierarchy_column(
                assessmentDF,
                hierarchyDF,
                id_col="assessID",
                as_col="data",
                spark=spark,
                children=True,
                competencies=True,
                l2_children=True
            )
            print("Stage 2: Complete")

            # Stage 3: Transform Assessment Data
            print("Stage 3: Transforming assessment data...")
            assessWithHierarchyDF = assessmentDFUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
            assessWithDetailsDF = assessWithHierarchyDF.drop("children")
            # kafkaDispatch(withTimestamp(assessWithDetailsDF, timestamp), config.assessmentTopic)

            print("Stage 3: Complete")

            # Stage 4: Process Assessment Children
            print("Stage 4: Processing assessment children...")
            assessChildrenDF = assessmentDFUtil.assessment_children_dataframe(assessWithHierarchyDF)
            print("Stage 4: Complete")

            # Stage 5: Process User Assessment Data
            print("Stage 5: Processing user assessment data...")
            userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE)
            userAssessChildrenDF = assessmentDFUtil.user_assessment_children_dataframe(userAssessmentDF,
                                                                                       assessChildrenDF)
            print("User Assessment Children DataFrame Schema:")
            categories = ["Course", "Program", "Blended Program", "Curated Program", "Moderated Course",
                          "Standalone Assessment", "CuratedCollections"]
            allCourseProgramDetailsWithCompDF = assessmentDFUtil.all_course_program_details_with_competencies_json_dataframe(
                spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(
                    col("category").isin(categories)), hierarchyDF, organizationDF, spark)
            print("All Course Program Details with Competencies JSON DataFrame Schema:")
            allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
            print("Stage 6: Complete")

            # Stage 7: Add Rating Information
            print("Stage 7: Adding rating information...")
            allCourseProgramDetailsWithRatingDF = assessmentDFUtil.all_course_program_details_with_rating_df(
                allCourseProgramDetailsDF,
                spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)
            )
            print("Stage 7: Complete")

            # Stage 8: Generate User Assessment Details
            print("Stage 8: Generating user assessment details...")
            userAssessChildrenDetailsDF = assessmentDFUtil.user_assessment_children_details_dataframe(
                userAssessChildrenDF,
                assessWithDetailsDF,
                allCourseProgramDetailsWithRatingDF,
                spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            )

            # kafkaDispatch(withTimestamp(userAssessChildrenDetailsDF, timestamp), conf.userAssessmentTopic)

            print("Stage 8: Complete")

            # Stage 9: Process Final Report Data
            print("Stage 9: Processing final report data...")

            # Get latest attempt per user per child assessment
            latest = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
                spark_max("assessEndTimestamp").alias("assessEndTimestamp"),
                expr("COUNT(*)").alias("noOfAttempts")
            )

            # Define status expressions
            case_expr = """
                CASE 
                    WHEN assessPass = 1 AND assessUserStatus = 'SUBMITTED' THEN 'Pass' 
                    WHEN assessPass = 0 AND assessUserStatus = 'SUBMITTED' THEN 'Fail' 
                    ELSE 'N/A' 
                END
            """
            completion_status_expr = """
                CASE 
                    WHEN assessUserStatus = 'SUBMITTED' THEN 'Completed' 
                    ELSE 'In progress' 
                END
            """

            # Join and transform data
            original_df = userAssessChildrenDetailsDF.join(
                broadcast(latest),
                on=["assessChildID", "userID", "assessEndTimestamp"],
                how="inner"
            ).filter(col("userStatus").cast("int") == 1).withColumn("Assessment_Status", expr(case_expr)) \
                .withColumn("Overall_Status", expr(completion_status_expr)) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .dropDuplicates(["userID", "assessID"]) \
                .select(
                col("userID").alias("User_ID"),
                col("fullName").alias("Full_Name"),
                col("employmentDetails.employeeCode").alias("Employee_Id"),
                col("assessName").alias("Assessment_Name"),
                col("Overall_Status"),
                col("Assessment_Status"),
                col("assessOverallResult").alias("Percentage_Of_Score"),
                col("noOfAttempts").alias("Number_of_Attempts"),
                col("maskedEmail").alias("Email"),
                col("maskedPhone").alias("Phone"),
                col("assessOrgID").alias("mdoid"),
                col("Report_Last_Generated_On")
            ).coalesce(1)

            print("Stage 9: Complete")

            # Stage 10: Generate Final Report
            print("Stage 10: Generating final report...")
            # Export report
            dfexportutil.write_csv_per_mdo_id(original_df,
                                              f"{config.localReportDir}/{config.standaloneAssessmentReportPath}/{today}",
                                              'mdoid', csv_filename=config.userAssessmentReport)
            print("Stage 10: Complete")

            # Performance Summary
            total_duration = time.time() - total_start_time
            print(f"Total processing time: {total_duration:.2f} seconds ({total_duration / 60:.1f} minutes)")

        except Exception as e:
            print(f"Error occurred during processing: {str(e)}")
            raise


def main():
    spark = SparkSession.builder \
        .appName("AssessmentReportGenerator") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    print("Starting Assessment Report Generation...")
    start_time = time.time()
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = UserAssessmentModel()
    model.process_report(spark, config)
    end_time = time.time()
    total_time = end_time - start_time

    print(f"Assessment report generation completed in {total_time:.2f} seconds")
    spark.stop()


if __name__ == "__main__":
    main()
