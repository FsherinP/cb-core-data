from datetime import datetime
import findspark
findspark.init()
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, expr, max as spark_max, 
    expr, current_timestamp, broadcast, date_format, when, lit, explode, to_date,
    collect_set, concat_ws, monotonically_increasing_id, coalesce, row_number
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



class L2AssessmentModel:    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.L2AssessmentModel"
        
    def name(self):
        return "L2AssessmentModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_report(self,spark,config):
        """
        Assessment Report Generation with minimal logging for performance
        """
        total_start_time = time.time()
        
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            # Stage 1: Load Assessment Data
            print("Stage 1: Loading assessment data...")
            assessmentDF = spark.read.parquet(ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE)#.filter(col("assessCategory").isin("Standalone Assessment"))
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
            kcmDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")
            enrolmentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")
            cbPlanDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            contentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCourseTable}")
            contentResourceDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwContentResourceTable}")
            finalAssessmentDF = spark.read.parquet(ParquetFileConstants.FINAL_ASSESSMENT_PARQUET_FILE)
            userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE)
            userAssessmentDF = userAssessmentDF.withColumn("assessment_date", col("assessStartTimestamp").cast("timestamp"))
            userAssessChildrenDF = assessmentDFUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
            print("User Assessment Children DataFrame Schema:")
            categories = ["Course","Program","Blended Program","Curated Program","Moderated Course","Standalone Assessment","CuratedCollections", "Course Assessment"]
            allCourseProgramDetailsWithCompDF = assessmentDFUtil.all_course_program_details_with_competencies_json_dataframe(spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(col("category").isin(categories)), hierarchyDF, organizationDF, spark)
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
            
            #enrolment df
            print("KCM")
            kcmCompetencyDF = kcmDF.select(
                col("competency_area_id"),
                col("competency_area")
            ).distinct()

            kcmCompetencyDF.printSchema()
            kcmCompetencyDF.show(5, truncate=False)

            print("Enrolment DF Schema:")
            enrolmentDF.printSchema()
            print(f"Total enrolments found: {enrolmentDF.count()}")
            #
            userEnrolmentDF = enrolmentDF.select(
                col("user_id"),
                col("content_id").alias("enrol_content_id"),
                col("batch_id").alias("enrol_batch_id"),
                col("enrolled_on"),
                col("content_progress_percentage"),
                col("certificate_id"),
                col("content_last_accessed_on"),
                col("first_completed_on"),
                col("user_consumption_status"),
                col("live_cbp_plan_mandate")
            )
            #cbplan df
            cbPlanDF2 = cbPlanDF \
            .filter((col("status") == "Live") & (col("isapar"))) \
            .select(
                col("org_id"),
                col("cb_plan_id"),
                col("plan_name"),
                col("allotment_type"),
                col("allotment_to"),
                col("content_id").alias("cbp_content_id"),
                col("allocated_on"),
                col("due_by"),
                col("status").alias("cbp_status"),
                col("isapar"))
            
            #cbPlanDF.show(5, truncate=False)

            print(f"Total cb plans found: {cbPlanDF2.count()}")
            cbPlanDF2.printSchema()
            cbPlanDF2.show(5, truncate=False)

            print(f"Total user assessments found: {userAssessmentDF.count()}")
            userAssessmentDF.printSchema()
            userAssessmentDF.show(5, truncate=False)

            print(f"Total final assessments found: {finalAssessmentDF.count()}")
            finalAssessmentDF.printSchema()         
            finalAssessmentDF.show(5, truncate=False)

            print("contentResourceDF Schema:")
            contentResourceDF.printSchema()
            print(f"Total content resources found: {contentResourceDF.count()}")
            contentResourceDF.show(5, truncate=False)

            print("total final assessments found:")
            finalProgramResourcesDF = finalAssessmentDF.join(contentResourceDF, finalAssessmentDF.identifier == contentResourceDF.resource_id, "inner")
            print(f"Total final program resources found: {finalProgramResourcesDF.count()}")
            finalProgramResourcesDF.show(5, truncate=False)

            print(f"Total contents found: {contentDF.count()}")
            contentDF.printSchema()
            contentDF.join(cbPlanDF2, contentDF.content_id == cbPlanDF2.cbp_content_id, "inner").show(5, truncate=False)

            contentResourceDF = contentResourceDF.withColumnRenamed("content_id", "cr_content_id")

            #finding courses with final program assessment
            finalProgramAssessmentsContentIds = contentDF.join(contentResourceDF, contentDF.content_id == contentResourceDF.cr_content_id, "inner") \
            .join(finalAssessmentDF, finalAssessmentDF.identifier == contentResourceDF.resource_id, "inner") \
            .filter((col("content_sub_type") == "Comprehensive Assessment Program") & (col("contextCategory") == "Final Program Assessment")) \
            .select(col("content_id")).distinct()

            print(f"Total final program assessments content IDs found: {finalProgramAssessmentsContentIds.count()}")
            finalProgramAssessmentsContentIds.printSchema()

            finalProgramAssessmentsContentIds.join(cbPlanDF2, finalProgramAssessmentsContentIds.content_id == cbPlanDF2.cbp_content_id, "inner").show(5, truncate=False)
            finalProgramAssessments = finalProgramAssessmentsContentIds.join(cbPlanDF2, finalProgramAssessmentsContentIds.content_id == cbPlanDF2.cbp_content_id, "inner")

            print(f"Total final program assessments found: {finalProgramAssessments.count()}")
            finalProgramAssessments.show(5, truncate=False)
            
            # Join and transform data
            original_df = userAssessChildrenDetailsDF.join(
                latest,
                on=["assessChildID", "userID", "assessEndTimestamp"],
                how="inner"
            ).filter((col("userStatus").cast("int") == 1) & (col("userOrgID") == "0135502316148080641003")) \
            .withColumn("Assessment_Status", expr(case_expr)) \
            .withColumn("Overall_Status", expr(completion_status_expr)) \
            .withColumn("Report_Last_Generated_On",currentDateTime) \
            .dropDuplicates(["userID", "assessID"])
            
            print(f"Total original_df records found: {original_df.count()}")
            original_df.printSchema()

            # Step a: Add unique row identifier to original_df
            original_df = self.create_kcm_mapping_str(kcmDF, original_df)

            #exploded_df = original_df.withColumn("competency_area_id_exploded", explode(col("competencyAreaRefId")))
            
            #assessChildIds = contentDF.join(contentResourceDF, contentDF.content_id == contentResourceDF.cr_content_id, "inner") \
            #    .join(cbPlanDF2, original_df.courseID == cbPlanDF2.cbp_content_id, "inner") \
            #    .join(finalAssessmentDF, finalAssessmentDF.identifier == contentResourceDF.resource_id, "inner") \
            #    .join(original_df, original_df.assessChildID == finalAssessmentDF.identifier, "inner") \
            #    .join(userEnrolmentDF, (userEnrolmentDF.enrol_content_id == original_df.courseID) & (userEnrolmentDF.user_id == original_df.userID), "inner") \
            #    .filter((col("content_sub_type") == "Comprehensive Assessment Program") & (col("contextCategory") == "Final Program Assessment") & (col("isapar"))) \
            #    .select(col("assessChildID")).distinct()
            
            #print(f"Total assessChildIds found: {assessChildIds.count()}")
            #assessChildIds.show(5, truncate=False)  

            assessmentsDF = self.get_final_assessment_data(finalAssessmentDF, original_df, userEnrolmentDF, cbPlanDF2, contentDF, contentResourceDF)
            
            #print(f"Total final assessments found: {assessmentsDF.count()}")
            #assessmentsDF.printSchema()
            #assessmentsDF.show(5, truncate=False)

            courseAssessmentDF = self.create_assessment_data(contentDF, contentResourceDF, userEnrolmentDF, cbPlanDF2, original_df)

            #print(f"Total courseAssessmentDF found: {courseAssessmentDF.count()}")
            #courseAssessmentDF.printSchema()
            #courseAssessmentDF.show(5, truncate=False)

            userAparData = assessmentsDF.unionByName(courseAssessmentDF)

            #print(f"Total userAparData records found: {userAparData.count()}")
            #userAparData.printSchema()
            #userAparData.show(5, truncate=False)
            
            
            print("Stage 9: Complete")

            # Stage 10: Generate Final Report
            print("Stage 10: Generating final report...")
            # Export report
            userAparData.coalesce(1).write.mode("overwrite").parquet("/mount/data/analytics/igot-reports/assessment-report-apar/parquet")
            print("Stage 10: Complete")

            # Performance Summary
            total_duration = time.time() - total_start_time
            print(f"Total processing time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")

        except Exception as e:
            print(f"Error occurred during processing: {str(e)}")
            raise

    def get_final_assessment_data(self, finalAssessmentDF, original_df, userEnrolmentDF, cbPlanDF2, contentDF, contentResourceDF):
        window_spec = Window.partitionBy("user_id", "content_id", "assessment_id").orderBy(col("assessment_date").desc())

        assessmentsDF = contentDF.join(contentResourceDF, contentDF.content_id == contentResourceDF.cr_content_id, "inner") \
            .join(cbPlanDF2, contentDF.content_id == cbPlanDF2.cbp_content_id, "inner") \
            .join(finalAssessmentDF, finalAssessmentDF.identifier == contentResourceDF.resource_id, "inner") \
            .join(original_df, original_df.assessChildID == finalAssessmentDF.identifier, "inner") \
            .join(userEnrolmentDF, (userEnrolmentDF.enrol_content_id == original_df.courseID) & (userEnrolmentDF.user_id == original_df.userID), "inner") \
            .filter((col("content_sub_type") == "Comprehensive Assessment Program") & (col("contextCategory") == "Final Program Assessment") & (col("isapar"))) \
            .select(
                col("userID").alias("user_id"),
                col("userOrgID").alias("mdo_id"),
                col("userOrgName").alias("mdo_name"),
                col("fullName").alias("full_name"),
                col("assessChildID").alias("assessment_id"),
                col("assessName").alias("assessment_name"),
                col("assessPrimaryCategory").alias("assessment_type"),
                col("assessOverallResult").alias("score_achieved"),
                col("enrol_content_id").alias("content_id"),
                col("enrol_batch_id").alias("batch_id"),
                col("content_name"),
                col("content_type"),
                col("content_sub_type"),
                col("enrolled_on").cast("timestamp").alias("enrolled_on"),
                col("content_progress_percentage"),
                col("certificate_id"),
                when(col("certificate_id").isNotNull(), True).otherwise(False).alias("certificate_generated"),
                col("content_last_accessed_on").cast("timestamp").alias("content_last_accessed_on"),
                col("first_completed_on").cast("timestamp").alias("first_completed_on"),
                col("content_duration"),
                col("content_status"),
                col("competency_areas").alias("competency_type"),
                col("userMobile").alias("phone"),
                col("userPrimaryEmail").alias("email"),
                col("cadreName").alias("cadre"),
                col("group").alias("groups"),
                col("designation"),
                col("additionalProperties.externalSystemId").alias("external_system_id"),
                col("isapar").alias("isApar"),
                col("cb_plan_id").alias("cbp_plan_id"),
                col("allocated_on").cast("timestamp").alias("allocated_on"),
                when(col("assessPass") == 1, lit("Pass")).otherwise(lit("Fail")).alias("comprehensive_level_assessment_status"),
                col("allocated_on").cast("timestamp").alias("cbp_plan_start_date"),
                col("due_by").cast("timestamp").alias("cbp_plan_end_date"),
                lit(None).cast("string").alias("parichay_id"),
                lit(None).cast("string").alias("consumption_status"),
                col("assessment_date").cast("timestamp").alias("assessment_date")
                )\
                .withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn") \
                .orderBy(col("assessment_date").desc())
            
        return assessmentsDF

    def create_assessment_data(self, contentDF, contentResourceDF, userEnrolmentDF, cbPlanDF2, original_df):
        window_spec = Window.partitionBy("user_id", "content_id").orderBy(col("assessment_date").desc())

        courseAssessmentDF = contentDF.join(contentResourceDF, contentDF.content_id == contentResourceDF.cr_content_id, "inner") \
            .join(cbPlanDF2, contentDF.content_id == cbPlanDF2.cbp_content_id, "inner") \
            .join(original_df, original_df.courseID == contentDF.content_id, "inner") \
            .join(userEnrolmentDF, (userEnrolmentDF.enrol_content_id == contentDF.content_id) & (userEnrolmentDF.user_id == original_df.userID), "inner") \
            .filter(col("isapar")) \
            .select(
                col("userID").alias("user_id"),
                col("userOrgID").alias("mdo_id"),
                col("userOrgName").alias("mdo_name"),
                col("fullName").alias("full_name"),
                lit(None).cast("string").alias("assessment_id"),
                lit(None).cast("string").alias("assessment_name"),
                lit(None).cast("string").alias("assessment_type"),
                lit(None).cast("string").alias("score_achieved"),
                col("enrol_content_id").alias("content_id"),
                col("enrol_batch_id").alias("batch_id"),
                col("content_name"),
                col("content_type"),
                col("content_sub_type"),
                col("enrolled_on").cast("timestamp").alias("enrolled_on"),
                col("content_progress_percentage"),
                col("certificate_id"),
                when(col("certificate_id").isNotNull(), True).otherwise(False).alias("certificate_generated"),
                col("content_last_accessed_on").cast("timestamp").alias("content_last_accessed_on"),
                col("first_completed_on").cast("timestamp").alias("first_completed_on"),
                col("content_duration"),
                col("content_status"),
                col("competency_areas").alias("competency_type"),
                col("userMobile").alias("phone"),
                col("userPrimaryEmail").alias("email"),
                col("cadreName").alias("cadre"),
                col("group").alias("groups"),
                col("designation"),
                col("additionalProperties.externalSystemId").alias("external_system_id"),
                col("isapar").alias("isApar"),
                col("cb_plan_id").alias("cbp_plan_id"),
                col("allocated_on").cast("timestamp").alias("allocated_on"),
                lit(None).cast("string").alias("comprehensive_level_assessment_status"),
                col("allocated_on").cast("timestamp").alias("cbp_plan_start_date"),
                col("due_by").cast("timestamp").alias("cbp_plan_end_date"),
                lit(None).cast("string").alias("parichay_id"),
                col("user_consumption_status").alias("consumption_status"),
                col("assessment_date").cast("timestamp").alias("assessment_date")
                )\
                .withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn") \
                .orderBy(col("assessment_date").desc())
            
        return courseAssessmentDF

    def create_kcm_mapping_str(self, kcmDF, original_df):
        df_with_id = original_df.withColumn("row_id", monotonically_increasing_id())

            # Step b: Explode and join
        exploded_df = df_with_id.withColumn(
                "single_competency_id", 
                explode(col("competencyAreaRefId"))
            )

        joined_df = exploded_df.join(
                kcmDF,
                col("single_competency_id") == col("competency_area_id"),
                "left_outer"
            )

            # Step c: Aggregate to get comma-separated string
        aggregated_df = joined_df.groupBy("row_id").agg(
                concat_ws(", ", collect_set("competency_area")).alias("competency_areas")
            )

            # Step d: Join back to original_df
        original_df = df_with_id.join(
                aggregated_df,
                on="row_id",
                how="left_outer"
            ).drop("row_id")

            # Step e: Handle nulls (optional - replace null with empty string)
        original_df = original_df.withColumn(
                "competency_areas",
                coalesce(col("competency_areas"), lit(""))
            )
        
        return original_df
    
    # Write to CSV
    def write_to_csv(df, output_path, header=True, mode="overwrite"):
        """
        Write DataFrame to CSV
        
        Args:
            df: DataFrame to write
            output_path: Path where CSV will be saved
            header: Include header row (default: True)
            mode: Write mode - "overwrite", "append", "ignore", "error" (default: "overwrite")
        """
        df.write \
            .mode(mode) \
            .option("header", header) \
            .option("encoding", "UTF-8") \
            .csv(output_path)
        
        print(f"CSV written to: {output_path}")


    # Write to Parquet
    def write_to_parquet(df, output_path, mode="overwrite", partition_by=None):
        """
        Write DataFrame to Parquet
        
        Args:
            df: DataFrame to write
            output_path: Path where Parquet will be saved
            mode: Write mode - "overwrite", "append", "ignore", "error" (default: "overwrite")
            partition_by: Column(s) to partition by (optional)
        """
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        writer.parquet(output_path)
        
        print(f"Parquet written to: {output_path}")


    # Write to both formats
    def write_to_csv_and_parquet(df, base_path, file_name):
        """
        Write DataFrame to both CSV and Parquet
        
        Args:
            df: DataFrame to write
            base_path: Base directory path
            file_name: Base file name (without extension)
        """
        csv_path = f"{base_path}/csv/{file_name}"
        parquet_path = f"{base_path}/parquet/{file_name}"
        
        # Write CSV
        df.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(csv_path)
        
        # Write Parquet
        df.write \
            .mode("overwrite") \
            .parquet(parquet_path)
        
        print(f"Data written to:")
        print(f"  CSV: {csv_path}")
        print(f"  Parquet: {parquet_path}")

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
    model = L2AssessmentModel()
    model.process_report(spark,config)
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Assessment report generation completed in {total_time:.2f} seconds")
    spark.stop()
if __name__ == "__main__":
    main()