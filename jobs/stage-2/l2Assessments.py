from datetime import datetime
from unittest import case
import findspark
findspark.init()
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,explode,split,regexp_replace,trim,
    when, lit, row_number
)

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config
from dfutil.content import contentDFUtil

class L2AssessmentReport:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.L2AssessmentReport"
        
    def name(self):
        return "L2AssessmentReport"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

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
    
    def process_report(self,spark,config):
        """
        Assessment Report Generation with minimal logging for performance
        """
        total_start_time = time.time()
    
        try:
            today = self.get_date()
            primary_categories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]
            
            # Load dataframes
            print("Loading base dataframes...")
            kcmDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")
            kcmMappingDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmContentTable}")
            dwEnrolmentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")
            acbpAllEnrolDF = spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)
            contentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCourseTable}")
            assessmentDetailDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwAssessmentTable}")
            dwOrgDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwOrgTable}")
            userDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwUserTable}")
            userDF = userDF.join(dwOrgDF.select("mdo_id", "mdo_name"), "mdo_id", "left")
            dwcbPlanDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            assessmentMinPassDF = spark.read.parquet(f"{config.baseCachePath}/esCourseAssessment")
            
            #assessment Minimum Pass DF
            assessmentMinPassDF.printSchema()
            assessMinPassDF = assessmentMinPassDF.filter(col('minimumPassPercentage').isNotNull())\
                .select(
                "identifier",
                "minimumPassPercentage"
            )

            assessMinPassDF.show(5, truncate=False)

            assessmentDetailDF = assessmentDetailDF.join(assessMinPassDF,
                assessmentDetailDF.assessment_id == assessMinPassDF.identifier,
                "left"
            ).select(
                assessmentDetailDF["*"],
                assessMinPassDF["minimumPassPercentage"]
            )
            # If minimumPassPercentage is available for a matching assessment, prefer it
            # over the existing cut_off_percentage value.
            assessmentDetailDF = assessmentDetailDF.withColumn(
                "cut_off_percentage",
                when(col("minimumPassPercentage").isNotNull(), col("minimumPassPercentage").cast("float"))
                .otherwise(col("cut_off_percentage"))
            ).dropDuplicates("user_id", "content_id", "assessment_id")

            kcmCourseDF = kcmDF.join(kcmMappingDF, kcmDF.competency_area_id == kcmMappingDF.competency_area_id, "inner") \
                .select(
                    col("course_id").alias("course_id"),
                    kcmMappingDF["competency_area_id"],
                    col("competency_area").alias("competency_area")
                ).dropDuplicates()
            
            kcmCourseDF.show(5, truncate=False)

            resultDF = (
                kcmMappingDF
                    .join(
                        kcmDF,
                        kcmDF.competency_area_id == kcmMappingDF.competency_area_id,
                        "left"
                    )
                    .select(
                        "course_id",
                        kcmMappingDF["competency_area_id"],
                        "competency_area"
                    )
                    .distinct()
                    .groupBy("course_id")
                    .agg(
                        F.concat_ws(
                            ", ",
                            F.collect_set("competency_area")
                        ).alias("competency_areas")
                    )
            )

            resultDF.show(5, truncate=False)
            
            print("PART 1: PROCESSING APAR CONSUMPTION DATA")
            
            # ==================== APAR CONSUMPTION PROCESSING ====================
            print("\nStage 1: Processing APAR plans...")
            apar_plans_exploded = acbpAllEnrolDF \
                .filter(col("isapar") == 'true') \
                .withColumn("courseID", explode(col("acbpCourseIDList"))) \
                .withColumn("courseID", regexp_replace(col("courseID"), r"^\s*\[|\]\s*$|\s+", "")) \
                .select(
                    col("userID").alias("apar_user_id"),
                    col("acbpID").alias("apar_cbp_plan_id"),
                    col("courseID").alias("apar_content_id"),
                    col("isapar").alias("apar_isApar"),
                    col("allocatedOn").alias("apar_allocated_on"),
                    col("cbPlanName").alias("apar_cbPlanName"),
                    col("userOrgID").alias("apar_mdo_id"),
                    col("userOrgName").alias("apar_mdo_name"),
                    col("fullName").alias("apar_full_name")
                )

            # Step 2: Join with enrolment data
            print("\nStage 2: Joining with enrolment data...")
            apar_with_consumption = apar_plans_exploded \
                .join(
                    dwEnrolmentDF,
                    (apar_plans_exploded.apar_user_id == dwEnrolmentDF.user_id) &
                    (apar_plans_exploded.apar_content_id == dwEnrolmentDF.content_id),
                    "left"
                ) \
                .select(
                    col("apar_user_id"),
                    col("apar_content_id"),
                    col("apar_cbp_plan_id"),
                    col("apar_isApar"),
                    col("apar_allocated_on"),
                    col("apar_cbPlanName"),
                    col("apar_mdo_id"),
                    col("apar_mdo_name"),
                    col("apar_full_name"),
                    col("batch_id").alias("enrol_batch_id"),
                    col("enrolled_on").alias("enrol_enrolled_on"),
                    col("content_progress_percentage").alias("enrol_content_progress_percentage"),
                    col("certificate_id").alias("enrol_certificate_id"),
                    col("content_last_accessed_on").alias("enrol_content_last_accessed_on"),
                    col("first_completed_on").alias("enrol_first_completed_on"),
                    col("user_consumption_status").alias("enrol_user_consumption_status")
                )

            # Step 3: Join with content data
            print("\nStage 3: Joining with content data...")
            apar_with_content = apar_with_consumption \
                .join(
                    contentDF,
                    apar_with_consumption.apar_content_id == contentDF.content_id,
                    "left"
                ) \
                .select(
                    col("apar_user_id"),
                    col("apar_content_id"),
                    col("apar_cbp_plan_id"),
                    col("apar_isApar"),
                    col("apar_allocated_on"),
                    col("apar_cbPlanName"),
                    col("apar_mdo_id"),
                    col("apar_mdo_name"),
                    col("apar_full_name"),
                    col("enrol_batch_id"),
                    col("enrol_enrolled_on"),
                    col("enrol_content_progress_percentage"),
                    col("enrol_certificate_id"),
                    col("enrol_content_last_accessed_on"),
                    col("enrol_first_completed_on"),
                    col("enrol_user_consumption_status"),
                    col("content_name").alias("content_content_name"),
                    col("content_type").alias("content_content_type"),
                    col("content_sub_type").alias("content_content_sub_type"),
                    col("content_duration").alias("content_content_duration"),
                    col("content_status").alias("content_content_status")
                )

            # Step 4: Join with user data
            print("\nStage 4: Joining with user data...")
            apar_with_user = apar_with_content \
                .join(
                    userDF,
                    apar_with_content.apar_user_id == userDF.user_id,
                    "inner"
                ) \
                .select(
                    col("apar_user_id"),
                    col("apar_content_id"),
                    col("apar_cbp_plan_id"),
                    col("apar_isApar"),
                    col("apar_allocated_on"),
                    col("apar_cbPlanName"),
                    col("apar_mdo_id"),
                    col("apar_mdo_name"),
                    col("apar_full_name"),
                    col("enrol_batch_id"),
                    col("enrol_enrolled_on"),
                    col("enrol_content_progress_percentage"),
                    col("enrol_certificate_id"),
                    col("enrol_content_last_accessed_on"),
                    col("enrol_first_completed_on"),
                    col("enrol_user_consumption_status"),
                    col("content_content_name"),
                    col("content_content_type"),
                    col("content_content_sub_type"),
                    col("content_content_duration"),
                    col("content_content_status"),
                    col("external_system_id").alias("user_external_system_id"),
                    col("external_system").alias("user_external_system"),
                    col("phone_number").alias("user_phone_number"),
                    col("email").alias("user_email"),
                    col("cadre").alias("user_cadre"),
                    col("groups").alias("user_groups"),
                    col("designation").alias("user_designation")
                )

            # Step 5: Join with CB Plan data
            print("\nStage 5: Joining with CB plan data...")
            apar_with_cbplan = apar_with_user \
                .join(
                    dwcbPlanDF,
                    apar_with_user.apar_cbp_plan_id == dwcbPlanDF.cb_plan_id,
                    "left"
                ) \
                .select(
                    col("apar_user_id"),
                    col("apar_content_id"),
                    col("apar_cbp_plan_id"),
                    col("apar_isApar"),
                    col("apar_allocated_on"),
                    col("apar_cbPlanName"),
                    col("apar_mdo_id"),
                    col("apar_mdo_name"),
                    col("apar_full_name"),
                    col("enrol_batch_id"),
                    col("enrol_enrolled_on"),
                    col("enrol_content_progress_percentage"),
                    col("enrol_certificate_id"),
                    col("enrol_content_last_accessed_on"),
                    col("enrol_first_completed_on"),
                    col("enrol_user_consumption_status"),
                    col("content_content_name"),
                    col("content_content_type"),
                    col("content_content_sub_type"),
                    col("content_content_duration"),
                    col("content_content_status"),
                    col("user_external_system_id"),
                    col("user_external_system"),
                    col("user_phone_number"),
                    col("user_email"),
                    col("user_cadre"),
                    col("user_groups"),
                    col("user_designation"),
                    col("due_by").alias("cbplan_due_by"),
                    col("apar_allocated_on").alias("cbplan_start_date")
                )

            # Step 6: Join with KCM mapping
            print("\nStage 6: Joining with KCM mapping...")
            apar_final = apar_with_cbplan \
                .join(
                    resultDF,
                    apar_with_cbplan.apar_content_id == resultDF.course_id,
                    "left"
                ) \
                .select(
                    col("apar_user_id"),
                    col("apar_content_id"),
                    col("apar_cbp_plan_id"),
                    col("apar_isApar"),
                    col("apar_allocated_on"),
                    col("apar_cbPlanName"),
                    col("apar_mdo_id"),
                    col("apar_mdo_name"),
                    col("apar_full_name"),
                    col("enrol_batch_id"),
                    col("enrol_enrolled_on"),
                    col("enrol_content_progress_percentage"),
                    col("enrol_certificate_id"),
                    col("enrol_content_last_accessed_on"),
                    col("enrol_first_completed_on"),
                    col("enrol_user_consumption_status"),
                    col("content_content_name"),
                    col("content_content_type"),
                    col("content_content_sub_type"),
                    col("content_content_duration"),
                    col("content_content_status"),
                    col("user_external_system_id"),
                    col("user_external_system"),
                    col("user_phone_number"),
                    col("user_email"),
                    col("user_cadre"),
                    col("user_groups"),
                    col("user_designation"),
                    col("cbplan_due_by"),
                    col("cbplan_start_date"),
                    col("competency_areas").alias("kcm_competency_type")
                )

            # Filter for valid APAR consumption
            validAparConsumptionDF = apar_final \
                .filter(col("enrol_user_consumption_status").isin("in-progress", "completed")) \
                .dropDuplicates()
            
            print("PART 2: PROCESSING CAP (COMPREHENSIVE ASSESSMENT PROGRAM) DATA")
            
            # ==================== CAP ASSESSMENT PROCESSING ====================
            print("\nStage 1: Getting CAP content...")
            capContentDF = contentDF \
                .drop(col("data_last_generated_on")) \
                .filter(
                    (col("content_sub_type") == "Comprehensive Assessment Program") &
                    (col("content_status") == "Live")
                ) \
                .select(
                    col("content_id").alias("cap_content_id"),
                    col("content_name").alias("cap_content_name"),
                    col("content_type").alias("cap_content_type"),
                    col("content_sub_type").alias("cap_content_sub_type"),
                    col("content_duration").alias("cap_content_duration"),
                    col("content_status").alias("cap_content_status")
                )
            cap_with_enrolment = capContentDF \
                .join(
                    dwEnrolmentDF.filter(col("user_consumption_status").isin("in-progress", "completed")),
                    capContentDF.cap_content_id == dwEnrolmentDF.content_id,
                    "inner"
                ) \
                .select(
                    col("cap_content_id"),
                    col("cap_content_name"),
                    col("cap_content_type"),
                    col("cap_content_sub_type"),
                    col("cap_content_duration"),
                    col("cap_content_status"),
                    col("user_id").alias("cap_user_id"),
                    col("batch_id").alias("cap_enrol_batch_id"),
                    col("enrolled_on").alias("cap_enrol_enrolled_on"),
                    col("content_progress_percentage").alias("cap_enrol_content_progress_percentage"),
                    col("certificate_id").alias("cap_enrol_certificate_id"),
                    col("content_last_accessed_on").alias("cap_enrol_content_last_accessed_on"),
                    col("first_completed_on").alias("cap_enrol_first_completed_on"),
                    col("user_consumption_status").alias("cap_enrol_user_consumption_status")
                ) \
                .dropDuplicates()
            print(f"CAP with enrolment count: {cap_with_enrolment.count()}")

            # Step 3: Join with assessment details (left join - in-progress may not have assessment data)
            print("\nStage 3: Joining with assessment details...")
            cap_with_assessment = cap_with_enrolment \
                .join(
                    assessmentDetailDF,
                    (cap_with_enrolment.cap_user_id == assessmentDetailDF.user_id) &
                    (cap_with_enrolment.cap_content_id == assessmentDetailDF.content_id),
                    "left"  # Left join so in-progress records without assessment data are kept
                ) \
                .select(
                    col("cap_content_id"),
                    col("cap_content_name"),
                    col("cap_content_type"),
                    col("cap_content_sub_type"),
                    col("cap_content_duration"),
                    col("cap_content_status"),
                    col("cap_user_id"),
                    col("cap_enrol_batch_id"),
                    col("cap_enrol_enrolled_on"),
                    col("cap_enrol_content_progress_percentage"),
                    col("cap_enrol_certificate_id"),
                    col("cap_enrol_content_last_accessed_on"),
                    col("cap_enrol_first_completed_on"),
                    col("cap_enrol_user_consumption_status"),
                    col("assessment_id").alias("assess_assessment_id"),
                    col("assessment_name").alias("assess_assessment_name"),
                    col("assessment_type").alias("assess_assessment_type"),
                    col("score_achieved").alias("assess_score_achieved"),                    
                    col("cut_off_percentage").alias("assess_cut_off_percentage"),
                    col("completion_date").alias("assess_assessment_date"),
                    col("pass").alias("assess_pass")
                ) \
                .dropDuplicates()

            print(f"CAP with assessment count: {cap_with_assessment.count()}")
            print("\nStage 4: Joining with user data...")
            validL2AssessmentConsumptionDF = cap_with_assessment \
                .join(
                    userDF,
                    cap_with_assessment.cap_user_id == userDF.user_id,
                    "inner"
                ) \
                .select(
                    col("cap_user_id").alias("assess_user_id"),
                    col("cap_content_id").alias("assess_content_id"),
                    col("assess_assessment_id"),
                    col("assess_assessment_name"),
                    col("assess_assessment_type"),
                    col("assess_score_achieved"),
                    col("assess_cut_off_percentage"),
                    col("assess_assessment_date"),
                    col("external_system_id").alias("assess_user_external_system_id"),
                    col("external_system").alias("assess_user_external_system"),
                    col("phone_number").alias("assess_user_phone_number"),
                    col("email").alias("assess_user_email"),
                    col("cadre").alias("assess_user_cadre"),
                    col("groups").alias("assess_user_groups"),
                    col("designation").alias("assess_user_designation"),
                    col("full_name").alias("assess_user_full_name"),
                    col("mdo_id").alias("assess_user_mdo_id"),
                    col("mdo_name").alias("assess_user_mdo_name"),
                    col("cap_content_name"),
                    col("cap_content_type"),
                    col("cap_content_sub_type"),
                    col("cap_content_duration"),
                    col("cap_content_status"),
                    col("cap_enrol_batch_id"),
                    col("cap_enrol_enrolled_on"),
                    col("cap_enrol_content_progress_percentage"),
                    col("cap_enrol_certificate_id"),
                    col("cap_enrol_content_last_accessed_on"),
                    col("cap_enrol_first_completed_on"),
                    col("cap_enrol_user_consumption_status"),
                    col("assess_pass")
                ) \
                .dropDuplicates()

            print("PART 3: CREATING MASTER DATAFRAME")
            
            # ==================== UNION BOTH DATAFRAMES ====================
            print("\nCreating unified dataframe structure...")
            
            # Map APAR columns to final schema
            apar_unified = validAparConsumptionDF.select(
                col("apar_user_id").alias("user_id"),
                col("apar_mdo_id").alias("mdo_id"),
                col("apar_mdo_name").alias("mdo_name"),
                col("apar_full_name").alias("full_name"),
                lit(None).cast("string").alias("assessment_id"),
                lit(None).cast("string").alias("assessment_name"),
                lit(None).cast("string").alias("assessment_type"),
                lit(None).cast("double").alias("score_achieved"),
                col("apar_content_id").alias("content_id"),
                col("enrol_batch_id").alias("batch_id"),
                col("content_content_name").alias("content_name"),
                col("content_content_type").alias("content_type"),
                col("content_content_sub_type").alias("content_sub_type"),
                col("enrol_enrolled_on").cast("timestamp").alias("enrolled_on"),
                col("enrol_content_progress_percentage").alias("content_progress_percentage"),
                col("enrol_certificate_id").alias("certificate_id"),
                when(
                    (col('enrol_user_consumption_status') == 'completed') & (col("enrol_certificate_id").isNotNull()) & (trim(col("enrol_certificate_id")) != ""), lit(True)
                ).when(
                    (col('enrol_user_consumption_status') == 'in-progress'), lit(False)
                ).otherwise(
                    lit(False)
                ).alias("certificate_generated"),
                col("enrol_content_last_accessed_on").cast("timestamp").alias("content_last_accessed_on"),
                col("enrol_first_completed_on").cast("timestamp").alias("first_completed_on"),
                col("content_content_duration").alias("content_duration"),
                col("content_content_status").alias("content_status"),
                col("kcm_competency_type").alias("competency_type"),
                col("user_phone_number").alias("phone"),
                col("user_email").alias("email"),
                col("user_cadre").alias("cadre"),
                col("user_groups").alias("groups"),
                col("user_designation").alias("designation"),
                col("user_external_system_id").alias("external_system_id"),
                col("user_external_system").alias("external_system"),
                col("apar_isApar").cast("string").alias("isApar"),
                col("apar_cbp_plan_id").alias("cbp_plan_id"),
                col("apar_allocated_on").cast("timestamp").alias("allocated_on"),
                lit(None).cast("string").alias("comprehensive_level_assessment_status"),
                col("cbplan_start_date").cast("timestamp").alias("cbp_plan_start_date"),
                col("cbplan_due_by").cast("timestamp").alias("cbp_plan_end_date"),
                lit(None).cast("string").alias("parichay_id"),  # Assuming parichay_id is same as external_system_id
                col("enrol_user_consumption_status").alias("consumption_status"),
                lit(None).alias("assessment_date")
            )

            # Map CAP columns to final schema
            cap_unified = validL2AssessmentConsumptionDF.select(
                col("assess_user_id").alias("user_id"),
                col("assess_user_mdo_id").alias("mdo_id"),
                col("assess_user_mdo_name").alias("mdo_name"),
                col("assess_user_full_name").alias("full_name"),
                col("assess_assessment_id").alias("assessment_id"),
                col("assess_assessment_name").alias("assessment_name"),
                col("assess_assessment_type").alias("assessment_type"),
                col("assess_score_achieved").alias("score_achieved"),
                col("assess_content_id").alias("content_id"),
                col("cap_enrol_batch_id").alias("batch_id"),
                col("cap_content_name").alias("content_name"),
                col("cap_content_type").alias("content_type"),
                col("cap_content_sub_type").alias("content_sub_type"),
                col("cap_enrol_enrolled_on").cast("timestamp").alias("enrolled_on"),
                col("cap_enrol_content_progress_percentage").alias("content_progress_percentage"),
                col("cap_enrol_certificate_id").alias("certificate_id"),
                when(
                    (col('cap_enrol_user_consumption_status') == 'completed') & (col("cap_enrol_certificate_id").isNotNull()) & (trim(col("cap_enrol_certificate_id")) != ""), lit(True)
                ).when(
                    (col('cap_enrol_user_consumption_status') == 'in-progress'), lit(False)
                ).otherwise(
                    lit(False)
                ).alias("certificate_generated"),
                col("cap_enrol_content_last_accessed_on").cast("timestamp").alias("content_last_accessed_on"),
                col("cap_enrol_first_completed_on").cast("timestamp").alias("first_completed_on"),
                col("cap_content_duration").alias("content_duration"),
                col("cap_content_status").alias("content_status"),
                lit(None).cast("string").alias("competency_type"),
                col("assess_user_phone_number").alias("phone"),
                col("assess_user_email").alias("email"),
                col("assess_user_cadre").alias("cadre"),
                col("assess_user_groups").alias("groups"),
                col("assess_user_designation").alias("designation"),
                col("assess_user_external_system_id").alias("external_system_id"),
                col("assess_user_external_system").alias("external_system"),
                lit("false").alias("isApar"),
                lit(None).alias("cbp_plan_id"),
                lit(None).alias("allocated_on"),
                when(
                    col("assess_pass") == 'Yes', 
                    lit("Pass")
                ).when(
                    col("assess_pass") == 'No', 
                    lit("Fail")
                ).otherwise(lit(None)).alias("comprehensive_level_assessment_status"),
                lit(None).alias("cbp_plan_start_date"),
                lit(None).alias("cbp_plan_end_date"),
                lit(None).alias("parichay_id"),
                col("cap_enrol_user_consumption_status").alias("consumption_status"),
                col("assess_assessment_date").cast("timestamp").alias("assessment_date")
            )

            # Union both dataframes
            print("\nUnioning APAR and CAP dataframes...")
            masterFinalDF = apar_unified.unionByName(cap_unified).dropDuplicates()

            # Print schema and sample data
            print("\nFinal Schema:")
            masterFinalDF.printSchema()
            
            postgres_url = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"
            print("write final data to warehouse path - user_content_assessment")
            self.write_postgres_table(masterFinalDF, postgres_url, "user_content_assessment", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            
            print("\nSample data (10 rows):")
            #masterFinalDF.show(10, truncate=False)
            #masterFinalDF.filter(col("user_id") == "b39b6202-1718-4a26-afa8-dcd141756efe").show(20, truncate=False)
            
            
            print("\nReport generation completed successfully!")

            # Export report
            masterFinalDF.coalesce(1).write.mode("overwrite").parquet("/mount/data/analytics/igot-reports/assessment-report-apar/parquet")
            #csv
            #apar_assessment_data.coalesce(1).write.mode("overwrite").option("header", "true").csv("/home/analytics/shishir/assessment-report-apar/csv")

        except Exception as e:
            print(f"Error occurred during processing: {str(e)}")
            raise
    
def main():
    spark = SparkSession.builder \
    .appName("L2AssessmentReport") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
    print("Starting L2 Report Generation...")
    start_time = time.time()
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = L2AssessmentReport()
    model.process_report(spark,config)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"L2 report generation completed in {total_time:.2f} seconds")
    spark.stop()
if __name__ == "__main__":
    main()
