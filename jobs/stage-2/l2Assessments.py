from datetime import datetime
from unittest import case
import findspark
findspark.init()
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,explode,split,regexp_replace, 
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

    def process_report(self,spark,config):
        """
        Assessment Report Generation with minimal logging for performance
        """
        total_start_time = time.time()
    
        try:
            today = self.get_date()
            primary_categories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]
            
            # load dataframes
            kcmDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")
            kcmMappingDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmContentTable}")
            dwEnrolmentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")
            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
            acbpAllEnrolDF = spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)
            contentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCourseTable}")
            finalAssessmentDF = spark.read.parquet(ParquetFileConstants.FINAL_ASSESSMENT_PARQUET_FILE)
            userAssessmentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwAssessmentTable}")
            userDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwUserTable}")
            orgHierarchyDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwOrgTable}")
            dwcbPlanDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            contentHierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
            allCourseProgramESDF = spark.read.parquet(
                ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))
            allCourseProgramDetailsDF = contentDFUtil.allCourseProgramDetailsWithCompetenciesJsonDataFrame(
                allCourseProgramESDF, contentHierarchyDF,
                spark.read.parquet(ParquetFileConstants.ORG_SELECT_PARQUET_FILE)).drop("competenciesJson")

            #print("userAssessmentDF count:", userAssessmentDF.count())
            #userAssessmentDF.show(5, truncate=False)

            #finalAssessmentDF.printSchema()
            #print("finalAssessmentDF count:", finalAssessmentDF.count())
            #finalAssessmentDF.show(5, truncate=False)
            
            # kcm dictionary dataframe
            kcmMappingDF = kcmMappingDF.join(kcmDF, kcmDF.competency_area_id == kcmMappingDF.competency_area_id, "left").select(
                col("course_id"),
                kcmMappingDF["competency_area_id"],
                col("competency_area")
            ).distinct()

            kcmDF.unpersist()
            #print("kcmMappingDF count:", kcmMappingDF.count())
            #kcmMappingDF.show(5, truncate=False)

            # process cb_plan dataframe
            cbPlanDF = acbpAllEnrolDF\
                .withColumn("courseID", explode(split(col("acbpCourseIDList"), ",")))\
                .withColumn("courseID", regexp_replace(col("courseID"), r"^\s*\[|\]\s*$|\s+", ""))\
                .join(allCourseProgramDetailsDF, ["courseID"], "left")\
                .join(enrolmentDF, ["courseID", "userID"], "left")\
                .na.drop(subset=["userID", "courseID"])\
                .drop("acbpCourseIDList")\
            
            #print("cbPlanDF count:", cbPlanDF.count())
            #cbPlanDF.show(5, truncate=False)

            # process content dataframe
            contentDF = contentDF \
            .filter(col("content_status") == "Live")

            #print("contentDF count:", contentDF.count())
            #contentDF.show(5, truncate=False)
        
            # user details dataframe
            #userDF = userDF.filter((col("status") == 1) & (col("mdo_id") == '0135502316148080641003'))
            userDF = userDF.filter(col("status") == 1)

            #print("userDF count:", userDF.count())
            #userDF.show(5, truncate=False)

            # organization hierarchy dataframe
            orgHierarchyDF = orgHierarchyDF.select(
                col("mdo_id"),
                col("is_cca"))
            
            print("orgHierarchyDF count:", orgHierarchyDF.count())

            #enrolment dataframe
            dwEnrolmentDF = dwEnrolmentDF.withColumnRenamed("user_id", "enrol_user_id") \
                            .withColumnRenamed("content_id", "enrol_content_id")
            
            #print("enrolmentDF count:", enrolmentDF.count())
            #enrolmentDF.show(5, truncate=False)

            cbPlanDF = cbPlanDF.join(dwcbPlanDF, dwcbPlanDF.cb_plan_id == cbPlanDF.acbpID, "left") \
                .select(cbPlanDF["*"],dwcbPlanDF["due_by"])

            # cb_plan (cbp_org_id, cbp_content_id, allotment_type, allotment_to, isapar) 
            # JOIN content (content_id, content_name, content_status)
            # join userDF with cp_plan when(allotment_type == 'User') then allotment_to == user_id 
            # | when (allotment_type == 'Designation') then allotment_to == user_designation
            # | when (allotment_type == 'all') then allotment_to == org_id


            #join1 = contentDF.join(cbPlanDF, cbPlanDF.cbp_content_id == contentDF.content_id, "inner") \
            #.join(kcmMappingDF, kcmMappingDF.course_id == contentDF.content_id, "left") \
            #.select(cbPlanDF["*"], contentDF["content_name"], contentDF["content_status"], 
            #        contentDF["content_type"], contentDF["content_sub_type"],contentDF["content_duration"], kcmMappingDF["competency_area"]) \
            #.withColumnRenamed("competency_area", "competency_type")

            join1 = cbPlanDF.join(kcmMappingDF, kcmMappingDF.course_id == cbPlanDF.courseID, "left") \
            .select(cbPlanDF["*"], kcmMappingDF["competency_area"]) \
            .withColumnRenamed("competency_area", "competency_type")

            #unpersist
            #contentDF.unpersist()
            #cbPlanDF.unpersist()
            #kcmMappingDF.unpersist()

            print("join1 count after join:", join1.count())
            #join1.show(5, truncate=False)

            # join userDF with cbPlanDF
            join2 = join1.join(contentDF, join1.courseID == contentDF.content_id, "left") \
                    .join(userDF, join1.userID == userDF.user_id, "inner") \
                    .select(join1["*"], userDF["external_system_id"], userDF["external_system"], contentDF["content_sub_type"], contentDF["content_type"],contentDF["content_status"],contentDF["content_duration"])

            #unpersist userDF
            #userDF.unpersist()
            #join1.unpersist()
            
            print("join2 count after userDF join:", join2.count())
            #join2.show(5, truncate=False)

            # join with orgHierarchyDF to get mdo_name and is_cca
            join3 = join2.join(orgHierarchyDF, join2.userOrgID == orgHierarchyDF.mdo_id, "left") \
            .select(join2["*"], orgHierarchyDF.is_cca)

            #unpersist orgHierarchyDF
            #orgHierarchyDF.unpersist()
            #join2.unpersist()

            print("join3 count after orgHierarchyDF join:", join3.count())
            #join3.printSchema()

            content_consumption_window_spec = Window.partitionBy("user_id", "content_id").orderBy(col("content_last_accessed_on").desc())

            #print("dwEnrolmentDF schema")
            #dwEnrolmentDF.printSchema()

            #join with enrolmentDF to get enrolment details
            join4 = dwEnrolmentDF.join(join3, (join3.userID == dwEnrolmentDF.enrol_user_id) &
            (join3.courseID == dwEnrolmentDF.enrol_content_id), "left") \
                .filter((col("userID").isNotNull()) & (col("isapar") == 'true') & (col("enrol_content_id").isNotNull())) \
                .select(
                    join3["userID"].alias("user_id"),
                    col("userOrgID").alias("mdo_id"),
                    col("userOrgName").alias("mdo_name"),
                    col("fullName").alias("full_name"),
                    lit(None).cast("string").alias("assessment_id"),
                    lit(None).cast("string").alias("assessment_name"),
                    lit(None).cast("string").alias("assessment_type"),
                    lit(None).cast("string").alias("score_achieved"),
                    join3["courseID"].alias("content_id"),
                    join3["batchID"].alias("batch_id"),
                    join3["courseName"].alias("content_name"),
                    join3["content_type"],
                    join3["content_sub_type"],
                    dwEnrolmentDF["enrolled_on"].cast("timestamp").alias("enrolled_on"),
                    dwEnrolmentDF["content_progress_percentage"],
                    dwEnrolmentDF["certificate_id"],
                    when(((dwEnrolmentDF["certificate_id"].isNotNull()) & (dwEnrolmentDF["certificate_id"] != "")), True).otherwise(False).alias("certificate_generated"),
                    dwEnrolmentDF["content_last_accessed_on"].cast("timestamp").alias("content_last_accessed_on"),
                    dwEnrolmentDF["first_completed_on"].cast("timestamp").alias("first_completed_on"),
                    join3["content_duration"],
                    join3["content_status"],
                    join3["competency_type"],
                    join3["userMobile"].alias("phone"),
                    join3["userPrimaryEmail"].alias("email"),
                    join3["cadreName"].alias("cadre"),
                    join3["group"].alias("groups"),
                    join3["designation"],
                    join3["external_system_id"],
                    join3["external_system"],
                    join3["isapar"].alias("isApar"),
                    join3["acbpID"].alias("cbp_plan_id"),
                    join3["allocatedOn"].cast("timestamp").alias("allocated_on"),
                    lit(None).cast("string").alias("comprehensive_level_assessment_status"),
                    join3["allocatedOn"].cast("timestamp").alias("cbp_plan_start_date"),
                    join3["due_by"].cast("timestamp").alias("cbp_plan_end_date"),
                    lit(None).cast("string").alias("parichay_id"),
                    dwEnrolmentDF["user_consumption_status"].alias("consumption_status"),
                    lit(None).cast("timestamp").alias("assessment_date")
                    ) \
                .withColumn("rn", row_number().over(content_consumption_window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn") \
                .orderBy(col("content_last_accessed_on").desc())
            
            join3.printSchema()


            #unpersist 
            join3.unpersist()
            #enrolmentDF.printSchema()
            

            print("join4 count after enrolmentDF join:", join4.count())
            #join4.show(5, truncate=False)
            #join4.printSchema()

            #join4.unpersist()
            
            assessment_window_spec = Window.partitionBy("user_id", "assessment_id").orderBy(col("assessment_date").desc())

            join5 = userAssessmentDF.join(join4, (userAssessmentDF.user_id == join4.user_id) & (userAssessmentDF.content_id == join4.content_id), "left") \
            .join(finalAssessmentDF, finalAssessmentDF.identifier == userAssessmentDF.assessment_id, "left") \
            .filter((col("content_sub_type") == "Comprehensive Assessment Program") & (col("contextCategory") == "Final Program Assessment") & (col("isapar")== 'true')) \
            .select(
                join4["user_id"],
                col("mdo_id").alias("mdo_id"),
                col("mdo_name").alias("mdo_name"),
                col("full_name").alias("full_name"),
                userAssessmentDF["assessment_id"],
                userAssessmentDF["assessment_name"],
                userAssessmentDF["assessment_type"],
                userAssessmentDF["score_achieved"],
                join4["content_id"].alias("content_id"),
                join4["batch_id"].alias("batch_id"),
                join4["content_name"].alias("content_name"),
                join4["content_type"],
                join4["content_sub_type"],
                join4["enrolled_on"].cast("timestamp").alias("enrolled_on"),
                join4["content_progress_percentage"],
                join4["certificate_id"],
                when(((join4["certificate_id"].isNotNull()) & (join4["certificate_id"] != "")), True).otherwise(False).alias("certificate_generated"),
                join4["content_last_accessed_on"].cast("timestamp").alias("content_last_accessed_on"),
                join4["first_completed_on"].cast("timestamp").alias("first_completed_on"),
                join4["content_duration"],
                join4["content_status"],
                join4["competency_type"],
                join4["phone"].alias("phone"),
                join4["email"].alias("email"),
                join4["cadre"].alias("cadre"),
                join4["groups"].alias("groups"),
                join4["designation"],
                join4["external_system_id"],
                join4["external_system"],
                join4["isApar"].alias("isApar"),
                join4["cbp_plan_id"].alias("cbp_plan_id"),
                join4["allocated_on"].cast("timestamp").alias("allocated_on"),
                when(userAssessmentDF["score_achieved"] >= userAssessmentDF["cut_off_percentage"], lit("Pass")).otherwise(lit("Fail")).cast("string").alias("comprehensive_level_assessment_status"),
                join4["allocated_on"].cast("timestamp").alias("cbp_plan_start_date"),
                join4["cbp_plan_end_date"].cast("timestamp").alias("cbp_plan_end_date"),
                lit(None).cast("string").alias("parichay_id"),
                join4["consumption_status"].alias("consumption_status"),
                userAssessmentDF["completion_date"].cast("timestamp").alias("assessment_date")
                ) \
                .withColumn("rn", row_number().over(assessment_window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn") \
                .orderBy(col("assessment_date").desc())
            
            print("join5 count after userAssessmentDF done")

            apar_assessment_data = join4.union(join5)

            print("Stage 10: Generating final report...")
            # Export report
            apar_assessment_data.coalesce(1).write.mode("overwrite").parquet("/mount/data/analytics/igot-reports/assessment-report-apar/parquet")
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