import findspark

findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import bround, col, broadcast, concat_ws, coalesce, lit, when, from_unixtime, split, regexp_replace
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, broadcast, get_json_object, rtrim
from pyspark.sql.functions import col, from_json, explode_outer, coalesce, lit, format_string, split
from pyspark.sql.types import StructType, ArrayType, StringType, BooleanType, StructField
from pyspark.sql.types import MapType, StringType, StructType, StructField, FloatType, LongType, DateType, IntegerType
from pyspark.sql.functions import col, when, size, lit, expr, unix_timestamp, date_format, from_json, current_timestamp, \
    to_date, round, explode, to_utc_timestamp, from_utc_timestamp, to_timestamp, sum as spark_sum

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.default_config import create_config
from jobs.config import get_environment_config


class UserUnenrolmentModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.UserUnenrolmentModel"

    def name(self):
        return "UserUnenrolmentModel"

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

    def process_data(self, spark, config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

            print("📥 Loading base DataFrames...")
            primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]

            # Load and cache base DataFrames that are used multiple times
            '''
            CREATE TABLE sunbird_courses.enrollment_history_by_action (
                userid text,
                action text,
                actiondate timestamp,
                batchid text,
                comment text,
                courseid text,
                progress int,
                reason list<text>,
                updatedby text,
                PRIMARY KEY (userid, action, actiondate)
            ) WITH CLUSTERING ORDER BY (action ASC, actiondate DESC)
            '''
            unenrolmentAuditDF = spark.read.parquet(ParquetFileConstants.UNENROLMENT_AUDIT_PARQUET_FILE) \
            .filter(col('action') == 'UNENROLL') \
            .select(
                col("userid").alias("userID"),
                col("courseid").alias("courseID"),
                col("batchid").alias("batchID"),
                col("actiondate").alias("unenrolledOn"),
                col("comment").alias("unenrolmentComment"),
                col("progress").alias("unenrolemntProgress"),
                col("reason").alias("unenrolmentReason"),
                col("updatedby").alias("unenrolmentUpdatedBy"),
                col("action")
            ).cache()
            unenrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).filter(col('enrolment_status') == 'enrolled')
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            contentOrgDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))

            print("🔄 Processing platform unenrolments...")

            # Compute and cache the main platform join result
            allCourseProgramCompletionWithDetailsDFWithRating = enrolmentDFUtil.preComputeUserOrgEnrolment(unenrolmentDF,
                                                                                                           contentOrgDF,
                                                                                                           userOrgDF,
                                                                                                           spark)
            # Process platform data and cache the result
            df = (
                UserUnenrolmentModel.duration_format(allCourseProgramCompletionWithDetailsDFWithRating, "courseDuration")
                .withColumn("badge_details", explode_outer("issued_badges"))
                .withColumn("completedOn",
                            date_format(col("courseCompletedTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("enrolledOn",
                            date_format(col("courseEnrolledTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("firstCompletedOn",
                            date_format(col("firstCompletedOn"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("lastContentAccessTimestamp",
                            date_format(col("lastContentAccessTimestamp"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("courseLastPublishedOn",
                            to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("courseBatchStartDate",
                            to_date(col("courseBatchStartDate"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("completionPercentage", round(col("completionPercentage"), 2))
                .withColumn("Report_Last_Generated_On", currentDateTime)
                .withColumn("Certificate_Generated",
                            when(col("issuedCertificateCount") > 0, "Yes").otherwise("No"))
                .withColumn("ArchivedOn",
                            when(col("courseStatus") == "Retired", col("lastStatusChangedOn")).otherwise(""))
                .withColumn("ArchivedOn", to_date(col("ArchivedOn"), ParquetFileConstants.DATE_FORMAT))
                .withColumn("Certificate_ID", col("certificateID"))
                .dropDuplicates(["userID", "courseID", "batchID"])
            )
            
            print("🔄 Processing ACBP data...")

            # Load and process ACBP data
            acbpAllEnrolmentDF = (spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)
                                  .withColumn("courseID", explode(col("acbpCourseIDList")))\
                                  .withColumn("courseID", regexp_replace(col("courseID"), r"^\s*\[|\]\s*$|\s+", ""))\
                                  .withColumn("liveCBPlan", lit(True))
                                  .select(col("userOrgID"), col("courseID"), col("userID"),
                                          col("designation"), col("liveCBPlan")))

            # Join platform data with ACBP and cache result
            enrolmentWithACBP = (df.join(acbpAllEnrolmentDF, ["userID", "userOrgID", "courseID"], "left")
                                 .withColumn("live_cbp_plan_mandate",
                                             when(col("liveCBPlan").isNull(), False)
                                             .otherwise(col("liveCBPlan"))))

            # join with unenrolment audit data to get the unenrolment reason and timestamp
            enrolmentWithACBP = (enrolmentWithACBP.join(unenrolmentAuditDF, ["userID", "courseID", "batchID"], "inner")
                                 .withColumn("unenrolment_reason", col("unenrolmentReason"))
                                 .withColumn("unenrolment_timestamp", date_format(col("unenrolledOn"), ParquetFileConstants.DATE_TIME_FORMAT))
                                 .withColumn("unenrolment_comment", col("unenrolmentComment"))
                                 .withColumn("unenrolment_progress", col("unenrolemntProgress"))
                                 .withColumn("unenrolment_updated_by", col("unenrolmentUpdatedBy"))
                                 .drop("unenrolmentReason", "unenrolledOn", "unenrolmentComment", "unenrolemntProgress", "unenrolmentUpdatedBy"))
            
            print("🔄 Generating reports...")

            mdoPlatformReport = (enrolmentWithACBP
                                 .withColumn("MDO_Name", col("userOrgName"))
                                 .withColumn("Ministry",
                                             when(col("ministry_name").isNull(), col("userOrgName"))
                                             .otherwise(col("ministry_name")))
                                 .withColumn("Department",
                                             when((col("Ministry").isNotNull()) &
                                                  (col("Ministry") != col("userOrgName")) &
                                                  ((col("dept_name").isNull()) | (col("dept_name") == "")),
                                                  col("userOrgName"))
                                             .otherwise(col("dept_name")))
                                 .withColumn("Organization",
                                             when((col("Ministry") != col("userOrgName")) &
                                                  (col("Department") != col("userOrgName")),
                                                  col("userOrgName"))
                                             .otherwise(lit("")))
                                 .filter(col("userStatus").cast("int") == 1)
                                 .withColumn("enrolment_status",
                                             when((col("userCourseCompletionStatus").isNull()) |
                                                  (col("userCourseCompletionStatus") == "not-enrolled"),
                                                  "unenrolled")
                                             .otherwise("enrolled"))
                                 .select(
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("employmentDetails.employeeCode").alias("Employee Id"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("courseOrgName").alias("Content_Provider"),
                col("courseName").alias("Content_Name"),
                col("category").alias("Content_Type"),
                col("courseDuration").alias("Content_Duration"),
                col("batchID").alias("Batch_Id"),
                col("courseBatchName").alias("Batch_Name"),
                col("courseBatchStartDate").alias("Batch_Start_Date"),
                col("courseBatchEndDate").alias("Batch_End_Date"),
                col("enrolledOn").alias("Enrolled_On"),
                col("userCourseCompletionStatus").alias("Status"),
                col("completionPercentage").alias("Content_Progress_Percentage"),
                col("courseLastPublishedOn").alias("Last_Published_On"),
                col("ArchivedOn").alias("Content_Retired_On"),
                col("completedOn").alias("Completed_On"),
                col("Certificate_Generated"),
                col("userRating").alias("User_Rating"),
                col("personalDetails.gender").alias("Gender"),
                col("personalDetails.category").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("userOrgID").alias("mdoid"),
                col("Certificate_ID"),
                col("userID"),
                col("courseID"),
                col("enrolment_status").alias("Enrolment_Status"),
                col("unenrolment_reason").alias("Unenrolment_Reason"),
                col("unenrolment_timestamp").alias("Unenrolment_On"),
                col("unenrolment_comment").alias("Unenrolment_Comment"),
                col("unenrolment_progress").alias("Unenrolment_Progress"),
                col("unenrolment_updated_by").alias("Unenrolment_Updated_By"),
                col("action").alias("Unenrolment_Action"),
                col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate"),
                col("karma_points").alias("Karma_Points"),
                col("Report_Last_Generated_On"),
                )
                                 .dropDuplicates(["userID", "Batch_Id", "courseID"])
                                 .drop("userID", "courseID")
                                 )

            platformWarehouseDF = (enrolmentWithACBP
                                   .withColumn("certificate_generated_on",
                                               date_format(
                                                   from_utc_timestamp(
                                                       to_utc_timestamp(
                                                           to_timestamp(col("certificateGeneratedOn"),
                                                                        ParquetFileConstants.DATE_TIME_WITH_MILLI_SEC_FORMAT),
                                                           "UTC"
                                                       ),
                                                       "IST"
                                                   ),
                                                   ParquetFileConstants.DATE_TIME_FORMAT
                                               ))
                                   .withColumn("data_last_generated_on", currentDateTime)
                                   .select(
                col("userID").alias("user_id"),
                col("batchID").alias("batch_id"),
                col("courseID").alias("content_id"),
                col("enrolledOn").alias("enrolled_on"),
                col("completionPercentage").alias("content_progress_percentage"),
                col("courseProgress").alias("resource_count_consumed"),
                col("userCourseCompletionStatus").alias("user_consumption_status"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("firstCompletedOn").alias("first_certificate_generated_on"),
                col("completedOn").alias("last_completed_on"),
                col("certificate_generated_on").alias("last_certificate_generated_on"),
                col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                col("Certificate_Generated").alias("certificate_generated"),
                col("issuedCertificateCount").alias("number_of_certificate"),
                col("userRating").alias("user_rating"),
                col("Certificate_ID").alias("certificate_id"),
                col("live_cbp_plan_mandate"),
                col("data_last_generated_on"),
                col("karma_points"),
                col("badge_details")["badgeId"].alias("badge_id"),
                col("action").alias("unenrolment_action"),
                col("unenrolment_reason"),
                col("unenrolment_timestamp").alias("unenrolment_on"),
                col("unenrolment_comment"),
                col("unenrolment_progress"),
                col("unenrolment_updated_by")
            )
                                   .fillna(0, subset=["karma_points"])
                                   .dropDuplicates(["user_id", "batch_id", "content_id"])
                                   )

            print("🔄 Combining and writing final outputs...")

            platform_enrolments_df = mdoPlatformReport
            
            print("===== igot and marketplace enrolments summary =====")
            print(f"----- igot enrolled count: {platform_enrolments_df.filter(col('enrolment_status') == 'enrolled').count()} ---")
            print(f"----- igot unenrolled count: {platform_enrolments_df.filter(col('enrolment_status') == 'unenrolled').count()} ---")
            print(f"----- igot count: {platform_enrolments_df.count()} ---")
            print("===== end of summary =====")

            print("📝 Writing CSV reports...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoPlatformReport,
                f"{config.localReportDir}/{config.userUnenrolmentReportPath}/{today}",
                'mdoid',
                f"{config.localReportDir}/temp/user_unenrolment_report/{today}",
                csv_filename=config.userUnenrolmentReport
            )

            print("📦 Writing warehouse data...")
            warehouseDF = platformWarehouseDF
            warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwUnenrollmentsTable}")

            print("✅ Processing completed successfully!")

        except Exception as e:
            print(f"❌ Error occurred during UserEnrolmentModel processing: {str(e)}")
            raise e
            sys.exit(1)


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("User Unenrolment Report Model - Cached") \
        .config("spark.master", "local[16]") \
        .config("spark.sql.shuffle.partitions", "240") \
        .config("spark.executor.memory", "30g") \
        .config("spark.driver.memory", "128g") \
        .config("spark.driver.memoryOverhead", "20g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] UserUnenrolmentModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = UserUnenrolmentModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserUnenrolmentModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


# Example usage:
if __name__ == "__main__":
    main()