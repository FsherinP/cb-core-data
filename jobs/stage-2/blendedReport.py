import findspark

findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType,FloatType
from pyspark.sql.functions import (col, lower, when, lit, expr, concat_ws, explode_outer, from_json, to_date,
                                   current_timestamp, date_format, round, coalesce, broadcast, size, map_keys,
                                   map_values,format_string)
from pyspark.sql.functions import col, expr
from pyspark.sql import Column, DataFrame, Row, SparkSession


from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from jobs.config import get_environment_config
from jobs.default_config import create_config
from util import schemas

class BlendedModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.BlendedModel"

    def name(self):
        return "BlendedModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
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

            primary_categories = ["Blended Program"]
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

            userOrgHierarchyDataDF = (spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
                         .select("userID", "fullName", "userGender", "userCategory", "maskedPhone",
                                 "maskedEmail", "userPrimaryEmail", "userMobile", "userStatus",
                                 "designation", "group", "Tag", "ministry_name", "dept_name",
                                 "userOrgID", "userOrgName", col("employmentDetails.employeeCode").alias("Employee_Id"))
                         ).cache()
            
            bpWithOrgDF = (spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
                 .filter(col("category").isin(primary_categories))
                 .where(expr("courseStatus IN ('Live', 'Retired')"))
                 .where(col("courseLastPublishedOn").isNotNull())
                 .select(
                     col("courseID").alias("bpID"),
                     col("category").alias("bpCategory"),
                     col("courseName").alias("bpName"),
                     col("courseStatus").alias("bpStatus"),
                     col("courseReviewStatus").alias("bpReviewStatus"),
                     col("courseChannel").alias("bpChannel"),
                     col("courseLastPublishedOn").alias("bpLastPublishedOn"),
                     col("courseDuration").cast(FloatType()).alias("bpDuration"),
                     col("courseResourceCount").alias("bpResourceCount"),
                     col("lastStatusChangedOn").alias("bpLastStatusChangedOn"),
                     col("programDirectorName").alias("bpProgramDirectorName"),
                     col("courseOrgID").alias("bpOrgID"),
                     col("courseOrgName").alias("bpOrgName"),
                 )
                 ).cache()


            bpBatchDF, bpBatchSessionDF = bpBatchDataframe(spark)

            batchCreatedByDF = userOrgHierarchyDataDF.select(
                col("userID").alias("bpBatchCreatedBy"),
                col("fullName").alias("bpBatchCreatedByName")
            )

            bpWithBatchDF = bpWithOrgDF \
                .join(bpBatchDF, on="bpID", how="left") \
                .join(batchCreatedByDF, on=["bpBatchCreatedBy"], how="left")
            

            userEnrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).cache()

            bpUserEnrolmentDF = userEnrolmentDF \
                           .select(
                    col("userID"),
                    col("courseID").alias("bpID"),
                    col("batchID").alias("bpBatchID"),
                    col("courseEnrolledTimestamp").alias("bpEnrolledTimestamp"),
                    col("issuedCertificateCount").alias("bpIssuedCertificateCount"),
                    col("courseContentStatus").alias("bpContentStatus"),
                    col("dbCompletionStatus").alias("bpUserCompletionStatus"),
                    col("lastContentAccessTimestamp").alias("bpLastContentAccessTimestamp"),
                    col("courseCompletedTimestamp").alias("bpCompletedTimestamp")
                )

            bpCompletionDF = bpWithBatchDF.join(
                bpUserEnrolmentDF.drop("bpContentStatus"), 
                ["bpID", "bpBatchID"], 
                "inner"
            ).cache()

        # Create BP User Content Status DataFrame
            bpUserContentStatusDF = bpUserEnrolmentDF \
                .select(col("userID"), col("bpID"), col("bpBatchID"), explode_outer(col("bpContentStatus"))) \
                .withColumnRenamed("key", "bpChildID") \
                .withColumnRenamed("value", "bpContentStatus")

            # Add user and user org info
            bpCompletionWithUserDetailsDF = userOrgHierarchyDataDF.join(bpCompletionDF, ["userID"], "right")
            bpCompletionDF.unpersist(blocking=True)

            hierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
            parsedHierarchyDF = hierarchyDF.withColumn("data", from_json(col("hierarchy"), schemas.get_hierarchy_schema())) \
                .select("identifier", "data.*")
            bpChildDF = bpChildDataFrame(bpWithOrgDF, hierarchyDF, parsedHierarchyDF,spark)

            bpCompletionWithChildrenDF = bpCompletionWithUserDetailsDF.join(bpChildDF, ["bpID"], "left") \
                .withColumn("bpChildMode", expr("CASE WHEN LOWER(bpChildCategory) = 'offline session' THEN 'Offline' ELSE '' END")) \
                .join(bpBatchSessionDF, ["bpID", "bpBatchID", "bpChildID"], "left")

            userOrgHierarchyDataDF.unpersist(blocking=True)
            bpWithOrgDF.unpersist(blocking=True)


            # Add children batch info - create child batch DataFrame
            bpChildBatchDF = bpBatchDF.select(
                col("bpID").alias("bpChildID"),
                col("bpBatchID").alias("bpChildBatchID"),
                col("bpBatchName").alias("bpChildBatchName"),
                col("bpBatchStartDate").alias("bpChildBatchStartDate"),
                col("bpBatchEndDate").alias("bpChildBatchEndDate"),
                col("bpBatchLocation").alias("bpChildBatchLocation"),
                col("bpBatchCurrentSize").alias("bpChildBatchCurrentSize")
            )

            # Create child batch session DataFrame
            bpChildBatchSessionDF = bpBatchSessionDF.select(
                col("bpChildID").alias("bpChildChildID"),
                col("bpID").alias("bpChildID"),
                col("bpBatchID").alias("bpChildBatchID"),
                col("bpBatchSessionType").alias("bpChildBatchSessionType"),
                col("bpBatchSessionFacilators").alias("bpChildBatchSessionFacilators"),
                col("bpBatchSessionStartDate").alias("bpChildBatchSessionStartDate"),
                col("bpBatchSessionStartTime").alias("bpChildBatchSessionStartTime"),
                col("bpBatchSessionEndTime").alias("bpChildBatchSessionEndTime")
            )

            relevantChildBatchInfoDF = bpChildDF.select("bpChildID") \
                .join(bpChildBatchDF, ["bpChildID"], "left") \
                .join(bpChildBatchSessionDF, ["bpChildID", "bpChildBatchID"], "left") \
                .select("bpChildID", "bpChildBatchID", "bpChildBatchName", "bpChildBatchStartDate", 
                        "bpChildBatchEndDate", "bpChildBatchLocation", "bpChildBatchCurrentSize", 
                        "bpChildBatchSessionType", "bpChildBatchSessionFacilators")

            # Join with completion data to add child batch info
            bpCompletionWithChildBatchInfoDF = bpCompletionWithChildrenDF.join(relevantChildBatchInfoDF, ["bpChildID"], "left")

            bpChildUserEnrolmentDF = userEnrolmentDF.select(
                col("userID"),
                col("courseID").alias("bpChildID"),
                col("courseProgress").alias("bpChildProgress"),
                col("dbCompletionStatus").alias("bpChildUserStatus"),
                col("lastContentAccessTimestamp").alias("bpChildLastContentAccessTimestamp"),
                col("courseCompletedTimestamp").alias("bpChildCompletedTimestamp")
            )

            userEnrolmentDF.unpersist(blocking=True)

            # Create BP Children With Progress DataFrame and cache it
            bpChildrenWithProgress = bpCompletionWithChildBatchInfoDF.join(bpChildUserEnrolmentDF, ["userID", "bpChildID"], "left") \
                .na.fill(0, ["bpChildResourceCount", "bpChildProgress"]) \
                .join(bpUserContentStatusDF, ["userID", "bpID", "bpChildID", "bpBatchID"], "left") \
                .withColumn("bpChildUserStatus", coalesce(col("bpChildUserStatus"), col("bpContentStatus"))) \
                .withColumn("completionPercentage", expr("CASE WHEN bpChildUserStatus=2 THEN 100.0 WHEN bpChildProgress=0 OR bpChildResourceCount=0 OR bpChildUserStatus=0 THEN 0.0 ELSE 100.0 * bpChildProgress / bpChildResourceCount END")) \
                .withColumn("completionPercentage", expr("CASE WHEN completionPercentage > 100.0 THEN 100.0 WHEN completionPercentage < 0.0 THEN 0.0 ELSE completionPercentage END")) \
                .withColumnRenamed("completionPercentage", "bpChildProgressPercentage") \
                .withColumn("bpChildAttendanceStatus", expr("CASE WHEN bpChildUserStatus=2 THEN 'Attended' ELSE 'Not Attended' END")) \
                .withColumn("bpChildOfflineAttendanceStatus", expr("CASE WHEN bpChildMode='Offline' THEN bpChildAttendanceStatus ELSE '' END")) \
                .drop("bpContentStatus").cache()

            fullDF = bpChildrenWithProgress \
                .withColumn("bpEnrolledOn", to_date(col("bpEnrolledTimestamp"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpBatchStartDate", to_date(col("bpBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpBatchEndDate", to_date(col("bpBatchEndDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpChildBatchStartDate", to_date(col("bpChildBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpChildCompletedTimestamp", to_date(col("bpChildBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpChildCompletedOn", expr("CASE WHEN bpChildMode='Offline' AND bpChildUserStatus=2 THEN bpBatchSessionStartDate ELSE bpChildCompletedTimestamp END")) \
                .withColumn("bpChildLastContentAccessTimestamp", to_date(col("bpChildLastContentAccessTimestamp"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("bpChildLastAccessedOn", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartDate ELSE bpChildLastContentAccessTimestamp END")) \
                .withColumn("bpChildProgressPercentage", round(col("bpChildProgressPercentage"), 2)) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .withColumn("Certificate_Generated", expr("CASE WHEN bpIssuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END")) \
                .withColumn("bpChildOfflineStartDate", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartDate ELSE '' END")) \
                .withColumn("bpChildOfflineStartTime", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionStartTime ELSE '' END")) \
                .withColumn("bpChildOfflineEndTime", expr("CASE WHEN bpChildMode='Offline' THEN bpBatchSessionEndTime ELSE '' END")) \
                .withColumn("bpChildUserStatus", expr("CASE WHEN bpChildUserStatus=2 THEN 'Completed' ELSE 'Not Completed' END")) \
                .cache()

            # Apply duration formatting
            fullDF = self.duration_format(fullDF, "bpChildDuration").distinct()
            bpChildrenWithProgress.unpersist(blocking=True)
            fullReportDF = fullDF \
                .filter(col("userStatus").cast("int") == 1) \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", when(col("ministry_name").isNull(), col("userOrgName")).otherwise(col("ministry_name"))) \
                .withColumn("Department", when((col("Ministry").isNotNull()) & 
                                            (col("Ministry") != col("userOrgName")) & 
                                            ((col("dept_name").isNull()) | (col("dept_name") == "")), 
                                            col("userOrgName")).otherwise(col("dept_name"))) \
                .withColumn("Organization", when((col("Ministry") != col("userOrgName")) & 
                                            (col("Department") != col("userOrgName")), 
                                            col("userOrgName")).otherwise(lit(""))) \
                .select(
                    col("userID"),
                    col("userOrgID"),
                    col("bpID"),
                    col("bpOrgID"),
                    col("bpChildID"),
                    col("bpBatchID"),
                    col("bpIssuedCertificateCount"),
                    col("fullName").alias("Name"),
                    col("userPrimaryEmail").alias("Email"),
                    col("userMobile").alias("Phone_Number"),
                    col("MDO_Name"),
                    col("maskedEmail"),
                    col("maskedPhone"),
                    col("designation").alias("Designation"),  
                    col("group").alias("Group"),              
                    col("userGender").alias("Gender"),
                    col("userCategory").alias("Category"),
                    col("Tag").alias("Tag"),                  
                    col("Ministry"),
                    col("Department"),
                    col("Organization"),
                    col("Employee_Id"),
                    
                    col("bpOrgName").alias("Provider_Name"),
                    col("bpName").alias("Program_Name"),
                    col("bpBatchName").alias("Batch_Name"),
                    col("bpBatchLocation").alias("Batch_Location"),
                    col("bpBatchStartDate").alias("Batch_Start_Date"),
                    col("bpBatchEndDate").alias("Batch_End_Date"),
                    col("bpEnrolledOn").alias("Enrolled_On"),
                    col("bpChildName").alias("Component_Name"),
                    col("bpChildCategory").alias("Component_Type"),
                    col("bpChildMode").alias("Component_Mode"),
                    col("bpChildUserStatus").alias("Status"),
                    col("bpChildDuration").alias("Component_Duration"),
                    col("bpChildProgressPercentage").alias("Component_Progress_Percentage"),
                    col("bpChildCompletedOn").alias("Component_Completed_On"),
                    col("bpChildLastAccessedOn").alias("Last_Accessed_On"),
                    col("bpChildOfflineStartDate").alias("Offline_Session_Date"),
                    col("bpChildOfflineStartTime").alias("Offline_Session_Start_Time"),
                    col("bpChildOfflineEndTime").alias("Offline_Session_End_Time"),
                    col("bpChildOfflineAttendanceStatus").alias("Offline_Attendance_Status"),
                    col("bpBatchSessionFacilators").alias("Instructor(s)_Name"),
                    col("bpBatchCreatedByName").alias("Program_Coordinator_Name"),
                    col("bpProgramDirectorName"),
                    col("Certificate_Generated"),
                    col("userOrgID").alias("mdoid"),
                    col("bpID").alias("contentid"),
                    col("Report_Last_Generated_On")
                ) \
                .orderBy("bpID", "userID") 
            
            reportPath = f"{config.blendedReportPath}/{today}"

            mdo_report_columns = [
                "Name", "Email", "Phone_Number", "MDO_Name", "Designation", "Group", "Gender",
                "Category", "Tag", "Ministry", "Department", "Organization", "Employee_Id", "Provider_Name",
                "Program_Name", "Batch_Name", "Batch_Location", "Batch_Start_Date", "Batch_End_Date", 
                "Enrolled_On", "Component_Name", "Component_Type", "Component_Mode", "Status", 
                "Component_Duration", "Component_Progress_Percentage", "Component_Completed_On",
                "Last_Accessed_On", "Offline_Session_Date", "Offline_Session_Start_Time", 
                "Offline_Session_End_Time", "Offline_Attendance_Status", "Instructor(s)_Name", 
                "Program_Coordinator_Name", "Certificate_Generated", "mdoid", "Report_Last_Generated_On",
            ]

            mdoReportDF = fullReportDF \
                .select(*mdo_report_columns) 

            print("📝 Writing MDO reports...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoReportDF, 
                f"{config.localReportDir}/{config.blendedReportPath}-mdo/{today}", 
                'mdoid',
                f"{config.localReportDir}/temp/blended_mdo_report/{today}",
                csv_filename=config.blendedProgramReport
            )

            # Create CBP Report DataFrame
            cbpReportDF = fullReportDF.select(
                col("bpOrgID").alias("mdoid"),
                col("Name"),
                col("maskedEmail").alias("Email"),
                col("maskedPhone").alias("Phone_Number"),
                col("MDO_Name"),
                col("Designation"),
                col("Group"),
                col("Gender"),
                col("Category"),
                col("Tag"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("Employee_Id"),
                col("Provider_Name"),
                col("Program_Name"),
                col("Batch_Name"),
                col("Batch_Location"),
                col("Batch_Start_Date"),
                col("Batch_End_Date"),
                col("Enrolled_On"),
                col("Component_Name"),
                col("Component_Type"),
                col("Component_Mode"),
                col("Status"),
                col("Component_Duration"),
                col("Component_Progress_Percentage"),
                col("Component_Completed_On"),
                col("Last_Accessed_On"),
                col("Offline_Session_Date"),
                col("Offline_Session_Start_Time"),
                col("Offline_Session_End_Time"),
                col("Offline_Attendance_Status"),
                col("Instructor(s)_Name"),
                col("Program_Coordinator_Name"),
                col("Certificate_Generated"),
                col("Report_Last_Generated_On")
            )

            print("📝 Writing CBP reports...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                cbpReportDF, 
                f"{config.localReportDir}/{config.blendedReportPath}-cbp/{today}", 
                'mdoid',
                f"{config.localReportDir}/temp/blended_cbp_report/{today}",
                csv_filename=config.blendedProgramReport
            )

            # Create warehouse DataFrame with snake_case column names
            df_warehouse = fullDF \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                    col("userID").alias("user_id"),
                    col("bpID").alias("content_id"),
                    col("bpBatchID").alias("batch_id"),
                    col("bpBatchLocation").alias("batch_location"),
                    col("bpChildName").alias("component_name"),
                    col("bpChildID").alias("component_id"),
                    col("bpChildCategory").alias("component_type"),
                    col("bpChildBatchSessionType").alias("component_mode"),
                    col("bpChildUserStatus").alias("component_status"),
                    col("bpChildDuration").alias("component_duration"),
                    col("bpChildProgressPercentage").alias("component_progress_percentage"),
                    col("bpChildCompletedOn").alias("component_completed_on"),
                    col("bpChildLastAccessedOn").alias("last_accessed_on"),
                    col("bpChildOfflineStartDate").alias("offline_session_date"),
                    col("bpChildOfflineStartTime").alias("offline_session_start_time"),
                    col("bpChildOfflineEndTime").alias("offline_session_end_time"),
                    col("bpChildAttendanceStatus").alias("offline_attendance_status"),
                    col("bpBatchSessionFacilators").alias("instructors_name"),
                    col("bpProgramDirectorName").alias("program_coordinator_name"),
                    col("data_last_generated_on")
                )
            
            fullDF.unpersist(blocking=True)

            warehouseDF = df_warehouse.coalesce(1).distinct()
            warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwBPEnrollmentsTable}")

            print("✅ Warehouse DataFrame created")


        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

            

def bpChildDataFrame(blendedProgramESDF: DataFrame, hierarchyDF: DataFrame, parsedHierarchyDF: DataFrame, spark: SparkSession) -> DataFrame:
    # Cache frequently used DataFrames
    bpIDsDF = blendedProgramESDF.select("bpID").cache()
    
    # Use broadcast for smaller DataFrame (parsedHierarchyDF is typically smaller)
    currentDF = bpIDsDF \
        .join(broadcast(parsedHierarchyDF), bpIDsDF.bpID == parsedHierarchyDF.identifier, "left") \
        .withColumn("bpChild", explode_outer(col("children"))) \
        .select(col("bpID"), col("bpChild.*")) \
        .cache()  # Cache as it's used multiple times
    
    # Define common select columns to avoid repetition
    childSelectCols = [
        col("bpID"),
        col("identifier").alias("bpChildID"),
        col("name").alias("bpChildName"),
        col("primaryCategory").alias("bpChildCategory"),
        col("duration").alias("bpChildDuration"),
        col("leafNodesCount").alias("bpChildResourceCount")
    ]
    
    # Case 1: Not Course or Course Unit → pick directly
    level1DirectDF = currentDF \
        .filter(~col("primaryCategory").isin("Course", "Course Unit") & col("identifier").isNotNull()) \
        .select(*childSelectCols)
    
    # Case 2: Course → Course Unit → LR
    # Filter and cache courseDF as it's used multiple times
    courseDF = currentDF \
        .filter((col("primaryCategory") == "Course") & col("children").isNotNull()) \
        .withColumn("courseUnit", explode_outer(col("children"))) \
        .select(col("bpID"), col("courseUnit.*")) \
        .cache()
    
    nonCourseUnitInCourseDF = courseDF \
        .filter((col("primaryCategory") != "Course Unit") & col("identifier").isNotNull()) \
        .select(*childSelectCols)
    
    # Course Unit → Learning Resource
    courseUnitInCourseDF = courseDF \
        .filter((col("primaryCategory") == "Course Unit") & col("children").isNotNull()) \
        .withColumn("courseUnitChild", explode_outer(col("children"))) \
        .select(
            col("bpID"),
            col("courseUnitChild.identifier").alias("bpChildID"),
            col("courseUnitChild.name").alias("bpChildName"),
            col("courseUnitChild.primaryCategory").alias("bpChildCategory"),
            col("courseUnitChild.duration").alias("bpChildDuration"),
            col("courseUnitChild.leafNodesCount").alias("bpChildResourceCount")
        )
    
    # Case 3: Course Unit → LR
    courseUnitDF = currentDF \
        .filter((col("primaryCategory") == "Course Unit") & col("children").isNotNull()) \
        .withColumn("unitChild", explode_outer(col("children"))) \
        .select(
            col("bpID"),
            col("unitChild.identifier").alias("bpChildID"),
            col("unitChild.name").alias("bpChildName"),
            col("unitChild.primaryCategory").alias("bpChildCategory"),
            col("unitChild.duration").alias("bpChildDuration"),
            col("unitChild.leafNodesCount").alias("bpChildResourceCount")
        )
    
    # Union all DataFrames with coalesce for better partitioning
    resultDF = level1DirectDF \
        .unionByName(nonCourseUnitInCourseDF) \
        .unionByName(courseUnitInCourseDF) \
        .unionByName(courseUnitDF) \
        .coalesce(spark.sparkContext.defaultParallelism) \
    
    # Clean up intermediate cached DataFrames
    currentDF.unpersist()
    courseDF.unpersist()
    bpIDsDF.unpersist()
    
    return resultDF
def bpBatchDataframe(spark):
    batch_df = spark.read.parquet(ParquetFileConstants.BATCH_SELECT_PARQUET_FILE) \
        
    bp_batch_df = batch_df.select(
        col("courseID").alias("bpID"),
        col("batchID").alias("bpBatchID"),
        col("courseBatchCreatedBy").alias("bpBatchCreatedBy"),
        col("courseBatchName").alias("bpBatchName"),
        col("courseBatchStartDate").alias("bpBatchStartDate"),
        col("courseBatchEndDate").alias("bpBatchEndDate"),
        col("courseBatchAttrs").alias("bpBatchAttrs")
    ) \
    .withColumn("bpBatchAttrs", from_json(col("bpBatchAttrs"), schemas.batch_attrs_schema)) \
    .withColumn("bpBatchLocation", col("bpBatchAttrs.batchLocationDetails")) \
    .withColumn("bpBatchCurrentSize", col("bpBatchAttrs.currentBatchSize"))
    
    bp_batch_session_df = bp_batch_df.select("bpID", "bpBatchID", "bpBatchAttrs") \
        .withColumn("bpBatchSessionDetails", explode_outer(col("bpBatchAttrs.sessionDetails_v2"))) \
        .withColumn("bpChildID", col("bpBatchSessionDetails.sessionId")) \
        .withColumn("bpBatchSessionType", col("bpBatchSessionDetails.sessionType")) \
        .withColumn("bpBatchSessionFacilators", concat_ws(", ", col("bpBatchSessionDetails.facilatorDetails.name"))) \
        .withColumn("bpBatchSessionStartDate", col("bpBatchSessionDetails.startDate")) \
        .withColumn("bpBatchSessionStartTime", col("bpBatchSessionDetails.startTime")) \
        .withColumn("bpBatchSessionEndTime", col("bpBatchSessionDetails.endTime")) \
        .drop("bpBatchAttrs", "bpBatchSessionDetails")
    
    # Remove bpBatchAttrs from the main BP batch DataFrame
    bp_batch_df = bp_batch_df.drop("bpBatchAttrs")
    
    return bp_batch_df, bp_batch_session_df
    
def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Blended Program Report") \
        .config("spark.executor.memory", "25g") \
        .config("spark.driver.memory", "20g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.shuffle.io.connectionTimeout", "300s") \
        .config("spark.shuffle.io.maxRetries", "20") \
        .config("spark.shuffle.io.retryWait", "10s") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] BlendedModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = BlendedModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] BlendedModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()
