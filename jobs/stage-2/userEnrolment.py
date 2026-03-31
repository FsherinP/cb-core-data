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


class UserEnrolmentModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.UserEnrolmentModel"

    def name(self):
        return "UserEnrolmentModel"

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
            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            contentOrgDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))

            print("🔄 Processing platform enrolments...")

            # Compute and cache the main platform join result
            allCourseProgramCompletionWithDetailsDFWithRating = enrolmentDFUtil.preComputeUserOrgEnrolment(enrolmentDF,
                                                                                                           contentOrgDF,
                                                                                                           userOrgDF,
                                                                                                           spark)
            # Process platform data and cache the result
            df = (
                UserEnrolmentModel.duration_format(allCourseProgramCompletionWithDetailsDFWithRating, "courseDuration")
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
            print("🔄 Processing external/marketplace enrolments...")

            # Load external data
            externalEnrolmentDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE).withColumn("badge_details", explode_outer("issued_badges"))
            externalContentOrgDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

            # Process marketplace data and cache
            marketPlaceContentEnrolmentsDF = (
                UserEnrolmentModel.duration_format(externalContentOrgDF, "courseDuration")
                .join(externalEnrolmentDF, "content_id", "inner")
                .withColumn("courseCompletedTimestamp",
                            date_format(col("completedon"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("courseEnrolledTimestamp",
                            date_format(col("enrolled_date"), ParquetFileConstants.DATE_TIME_FORMAT))
                .withColumn("lastContentAccessTimestamp", lit("Not Available"))
                .withColumn("userRating", lit("Not Available"))
                .withColumn("live_cbp_plan_mandate", lit(False))
                .withColumn("batchID", lit("Not Available"))
                .withColumn("issuedCertificateCount", size(col("issued_certificates")))
                .withColumn("certificate_generated",
                            expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END"))
                .withColumn("certificateGeneratedOn",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(
                                col("issued_certificates")[size(col("issued_certificates")) - 1]["lastIssuedOn"]))
                .withColumn("firstCompletedOn",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(
                                when(size(col("issued_certificates")) > 0,
                                     col("issued_certificates")[0]["lastIssuedOn"])
                                .otherwise("")))
                .withColumn("certificateID",
                            when(col("issued_certificates").isNull(), "")
                            .otherwise(col("issued_certificates")[size(col("issued_certificates")) - 1]["identifier"]))
                .withColumn("Report_Last_Generated_On", currentDateTime)
                .withColumnRenamed("userid", "userID")
                .withColumnRenamed("content_id", "courseID")
                .withColumnRenamed("progress", "courseProgress")
                .withColumnRenamed("status", "dbCompletionStatus")
                .fillna(0, subset=["courseProgress", "issuedCertificateCount"])
                .fillna("", subset=["certificateGeneratedOn"])
            )
            marketPlaceEnrolmentsWithUserDetailsDF = marketPlaceContentEnrolmentsDF.join(userOrgDF, ["userID"], "left")

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

            print("🔄 Generating reports...")

            # Generate marketplace report
            mdoMarketplaceReport = (marketPlaceEnrolmentsWithUserDetailsDF
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
                lit("Not Available").alias("Batch_Name"),
                lit(None).cast(DateType()).alias("Batch_Start_Date"),
                lit(None).cast(DateType()).alias("Batch_End_Date"),
                col("courseEnrolledTimestamp").alias("Enrolled_On"),
                when(col("dbCompletionStatus").isNull(), "not-enrolled")
                .when(col("dbCompletionStatus") == 0, "not-started")
                .when(col("dbCompletionStatus") == 1, "in-progress")
                .otherwise("completed")
                .alias("Status"),
                col("completionpercentage").alias("Content_Progress_Percentage"),
                to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT).alias("Last_Published_On"),
                lit(None).cast(DateType()).alias("Content_Retired_On"),
                col("courseCompletedTimestamp").alias("Completed_On"),
                col("certificate_generated").alias("Certificate_Generated"),
                col("userRating").alias("User_Rating"),
                col("personalDetails.gender").alias("Gender"),
                lit("External Content").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("userOrgID").alias("mdoid"),
                col("certificateID").alias("Certificate_ID"),
                col("Report_Last_Generated_On"),
                col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
            )
            )

            marketPlaceWarehouseDF = (marketPlaceEnrolmentsWithUserDetailsDF
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
                                      .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"),
                                                                                  ParquetFileConstants.DATE_TIME_FORMAT))
                                      .select(
                col("userID").alias("user_id"),
                col("batchID").alias("batch_id"),
                col("courseID").alias("content_id"),
                col("courseEnrolledTimestamp").alias("enrolled_on"),
                col("completionpercentage").alias("content_progress_percentage"),
                col("courseProgress").alias("resource_count_consumed"),
                when(col("dbCompletionStatus").isNull(), "not-enrolled")
                .when(col("dbCompletionStatus") == 0, "not-started")
                .when(col("dbCompletionStatus") == 1, "in-progress")
                .otherwise("completed")
                .alias("user_consumption_status"),
                col("firstCompletedOn").alias("first_completed_on"),
                col("firstCompletedOn").alias("first_certificate_generated_on"),
                col("courseCompletedTimestamp").alias("last_completed_on"),
                col("certificate_generated_on").alias("last_certificate_generated_on"),
                col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                col("certificate_generated").alias("certificate_generated"),
                col("issuedCertificateCount").alias("number_of_certificate"),
                col("userRating").alias("user_rating"),
                col("certificateID").alias("certificate_id"),
                col("live_cbp_plan_mandate"),
                col("badge_details.badgeId").alias("badge_id"),
                col("data_last_generated_on")
            )
                                      .withColumn("karma_points", lit(0).cast(IntegerType()))
                                      .dropDuplicates(["user_id", "batch_id", "content_id"])
                                      )

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
                col("Report_Last_Generated_On"),
                col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate"),
                col("userID"),
                col("courseID")
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
            )
                                   .fillna(0, subset=["karma_points"])
                                   .dropDuplicates(["user_id", "batch_id", "content_id"])
                                   )

            print("🔄 Combining and writing final outputs...")

            mdoReportDF = (
                mdoPlatformReport
                .union(mdoMarketplaceReport)
            )

            print("📝 Writing CSV reports...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoReportDF,
                f"{config.localReportDir}/{config.userEnrolmentReportPath}/{today}",
                'mdoid',
                f"{config.localReportDir}/temp/user_enrolment_report/{today}",
                csv_filename=config.userEnrollmentReport
            )

            print("📦 Writing warehouse data...")
            warehouseDF = platformWarehouseDF.unionByName(marketPlaceWarehouseDF)
            warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")

            print("✅ Processing completed successfully!")

        except Exception as e:
            print(f"❌ Error occurred during UserEnrolmentModel processing: {str(e)}")
            raise e
            sys.exit(1)


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("User Enrolment Report Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "15g") \
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
    print(f"[START] UserEnrolmentModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = UserEnrolmentModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserEnrolmentModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


# Example usage:
if __name__ == "__main__":
    main()