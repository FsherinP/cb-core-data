from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user.userDFUtil import exportDFToParquet
from dfutil.content import contentDFUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer,from_json, lit,when, format_string, expr,lower,avg,count
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, lit, when, expr, format_string, date_format, current_timestamp, to_date, from_json, explode_outer, greatest, size,min as F_min, max as F_max, count as F_count, sum as F_sum,broadcast, trim)
from pyspark.sql.types import ( DateType)
from constants.ParquetFileConstants import ParquetFileConstants
from util import schemas
from dfutil.enrolment import enrolmentDFUtil

def esContentDataFrame(
    spark: SparkSession
) -> DataFrame:
    return spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE)

def preComputeAllCourseProgramESDataFrame(spark: SparkSession) -> DataFrame:
  
    contentDF = esContentDataFrame(spark) \
        .withColumn("courseOrgID", explode_outer(col("createdFor"))) \
        .withColumn("contentLanguage", explode_outer(col("language"))) \
        .withColumn("contentCreator", explode_outer(col("organisation"))) \
        .withColumn("competencyAreaRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyAreaRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencyThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyThemeRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencySubThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencySubThemeRefId"])
                   .otherwise(F.array())) \
        .select(
            col("identifier").alias("courseID"),
            col("primaryCategory").alias("category"),
            col("name").alias("courseName"),
            col("status").alias("courseStatus"),
            col("reviewStatus").alias("courseReviewStatus"),
            col("channel").alias("courseChannel"),
            col("lastPublishedOn").alias("courseLastPublishedOn"),
            col("duration").cast(FloatType()).alias("courseDuration"),
            col("leafNodesCount").alias("courseResourceCount"),
            col("lastStatusChangedOn").alias("lastStatusChangedOn"),
            col("programDirectorName"),
            col("courseOrgID"),
            F.col("competencyAreaRefId"),
            F.col("competencyThemeRefId"),
            F.col("competencySubThemeRefId"),
            col("contentLanguage"),
            col("courseCategory"),
            col("contentCreator")
        ) \
        .dropDuplicates(["courseID", "category"]) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"])
        
    exportDFToParquet(contentDF,ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)

def preComputeRatingAndSummaryDataFrame(spark):
    ratingSummaryDF = spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_PARQUET_FILE) \
        .where(expr("total_number_of_ratings > 0")) \
        .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings")) \
        .select(
            col("activityid").alias("courseID"),
            col("activitytype").alias("categoryLower"),
            col("sum_of_total_ratings").alias("ratingSum"),
            col("total_number_of_ratings").alias("ratingCount"),
            col("ratingAverage"),
            col("totalcount1stars").alias("count1Star"),
            col("totalcount2stars").alias("count2Star"),
            col("totalcount3stars").alias("count3Star"),
            col("totalcount4stars").alias("count4Star"),
            col("totalcount5stars").alias("count5Star")
        ) \
        .withColumn("categoryLower", lower(col("categoryLower"))) \
        .dropDuplicates(["courseID", "categoryLower"])
    
    exportDFToParquet(ratingSummaryDF,ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)

    ratingRawDF = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)
    ratingDF = ratingRawDF \
        .select(
            col("activityid").alias("courseID"),
            col("userid").alias("userID"), 
            col("rating").alias("userRating"),
            col("activitytype").alias("cbpType"),
            col("createdon").alias("createdOn")
        )
    exportDFToParquet(ratingDF,ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE)


    contentRating = ratingDF.filter(col("userRating").isNotNull()) \
    .groupBy("courseID") \
    .agg(
        count("userRating").alias("totalRatings"),
        avg("userRating").alias("rating")
    ) \
    .select("courseID", "totalRatings", "rating")

    exportDFToParquet(contentRating,ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)

def preComputeContentDataFrame(spark: SparkSession):
    contentRatingDF=spark.read.parquet(ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)    
    contentWithRatingDF=spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).join(
        contentRatingDF,"courseID","left").drop(contentRatingDF["courseID"])
    
    orgDF=spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE).select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    contentWithOrgRatingDF=contentWithRatingDF.join(orgDF, contentWithRatingDF["courseOrgID"] == orgDF["courseOrgID"], "left") \
        .drop(orgDF["courseOrgID"]) \
        
    exportDFToParquet(contentWithOrgRatingDF,ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)


def preComputeExternalContentDataFrame(spark) -> DataFrame:
        df = (
            spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_PARQUET_FILE)
            .withColumn("parsed_data", from_json(col("cios_data"), schemas.cios_data_schema))
            .withColumnRenamed("courseid", "content_id")
            .select(
                col("content_id"),
                col("parsed_data.content.name").alias("courseName"),
                col("parsed_data.content.duration").alias("courseDuration"),
                col("parsed_data.content.lastUpdatedOn").alias("courseLastPublishedOn"),
                col("parsed_data.content.contentPartner.id").alias("courseOrgID"),
                col("parsed_data.content.contentPartner.contentPartnerName").alias("courseOrgName"),
                lit("External Content").alias("category"),
                lit("LIVE").alias("courseStatus")
            )
        )
        exportDFToParquet(df,ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

def precomputeContentHierarchyDataFrame(spark: SparkSession) -> DataFrame:
    contentHierarchydf = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE).select(col("identifier"), col("hierarchy"))
    exportDFToParquet(contentHierarchydf, ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
    
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

def allCourseProgramDetailsWithCompetenciesJsonDataFrame(
    allCourseProgramESDF: DataFrame, 
    hierarchyDF: DataFrame, 
    orgDF: DataFrame, 
) -> DataFrame:
    # Add hierarchy column with competencies
    df = addHierarchyColumn(
        allCourseProgramESDF, 
        hierarchyDF, 
        "courseID", 
        "data", 
        competencies=True
    ).withColumn("competenciesJson", col("data.competencies_v3"))
    
    # Add course organization details and clean up
    courseOrgDetailsDF = addCourseOrgDetails(df, orgDF) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"]) \
        .drop("data")
    
    return courseOrgDetailsDF

def addHierarchyColumn(
    df: DataFrame, 
    hierarchyDF: DataFrame, 
    idCol: str, 
    asCol: str,
    children: bool = False, 
    competencies: bool = False, 
    l2Children: bool = False,
) -> DataFrame:
    # Get the hierarchy schema based on the flags
    hierarchySchema = schemas.make_hierarchy_schema(children, competencies, l2Children)
    
    result_df = df.join(
        hierarchyDF, 
        df[idCol] == hierarchyDF["identifier"], 
        "left"
    ) \
    .fillna("{}", subset=["hierarchy"]) \
    .withColumn(asCol, from_json(col("hierarchy"), hierarchySchema)) \
    .drop("hierarchy")
    
    return result_df

def addCourseOrgDetails(
    courseDF: DataFrame, 
    orgDF: DataFrame
) -> DataFrame:
    # Prepare organization DataFrame with renamed columns
    joinOrgDF = orgDF.select(
        col("orgID").alias("courseOrgID"),
        col("orgName").alias("courseOrgName"),
        col("orgStatus").alias("courseOrgStatus")
    )
    df = courseDF.join(joinOrgDF, ["courseOrgID"], "left")
    
    return df

def preComputeContentWarehouseData(spark):
    """
    Generate ONLY content warehouse data 
    Removes dependency on full course report job
    """
    try:
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
        primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
        
        print("Starting Content Warehouse Data Generation...")
        
        # Load required data
        print("Loading platform data...")
        allCourseProgramDetailsDF = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE).filter(col("category").isin(primary_categories))
        contentHierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE).withColumnRenamed("identifier", "courseID")
        enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
        courseBatchDF = spark.read.parquet(ParquetFileConstants.BATCH_SELECT_PARQUET_FILE)
        
        print("Loading marketplace data...")
        marketPlaceEnrolmentsDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)
        marketPlaceContentsDF = spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)
        
        # Process Platform Content Data
        print("Processing platform course progress and enrollments...")
        courseResCountDF = allCourseProgramDetailsDF.select("courseID", "courseResourceCount")
        userEnrolmentDF = enrolmentDF.join(courseResCountDF, on=["courseID"], how="left")
        allCBPCompletionWithDetailsDF = enrolmentDFUtil.calculateCourseProgress(userEnrolmentDF)

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
            .withColumn("lastCompletedOn", to_date(col("latestCourseCompleted"), ParquetFileConstants.DATE_FORMAT))

        allCBPAndAggDF = allCourseProgramDetailsDF.join(aggregatedDF, ["courseID"], "left")

        # Add batch information
        curatedCourseDataDFWithBatchInfo = allCBPAndAggDF \
            .join(
                broadcast(
                    allCourseProgramDetailsDF
                    .filter(col("category") == "Blended Program")
                    .select("courseID")
                    .join(courseBatchDF, ["courseID"], "inner")
                    .select("courseID", "batchID", "courseBatchName", "courseBatchStartDate", "courseBatchEndDate")
                ), 
                ["courseID"], 
                "left"
            )

        # Format and filter platform data
        fullDF = contentDFUtil.duration_format(curatedCourseDataDFWithBatchInfo, "courseDuration") \
            .filter(col("courseStatus").isin(["Live", "Draft", "Retired", "Review"])) \
            .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastStatusChangedOn", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("ArchivedOn", when(col("courseStatus") == "Retired", to_date(col("lastStatusChangedOn"), ParquetFileConstants.DATE_FORMAT)))

        # Process Marketplace Content Data
        print("Processing marketplace course data...")
        marketPlaceEnrolmentsAggDF = marketPlaceEnrolmentsDF \
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

        marketPlaceContentWithEnrolmentsDF = contentDFUtil.duration_format(marketPlaceContentsDF, "courseDuration") \
            .join(marketPlaceEnrolmentsAggDF, ["content_id"], "outer") \
            .withColumn("firstCompletedOn", to_date(col("earliestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("lastCompletedOn", to_date(col("latestCompletedOn"), ParquetFileConstants.DATE_FORMAT)) \
            .withColumn("data_last_generated_on", currentDateTime)

        # Generate SCORM Detection for Platform Content
        print("Detecting SCORM content...")
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

        # Generate Platform Content Warehouse Data
        print("Generating platform content warehouse data...")
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
                col("data_last_generated_on")
            )

        # Generate Marketplace Content Warehouse Data
        print("Generating marketplace content warehouse data...")
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
            col("data_last_generated_on")
        )

        # Combine Platform and Marketplace Data & Write to Warehouse
        print("Writing content warehouse data...")
        df_warehouse = platformContentWarehouseDF.union(marketPlaceContentWarehouseDF)
        exportDFToParquet(df_warehouse.coalesce(1), ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE)

    except Exception as e:
        print(f"Content warehouse data generation error: {str(e)}")
        raise

def writeWarehouseParquetFiles(spark, config):
    """
    Write parquet files for org hierarchy, events, and event enrollments
    This can be decoupled and run as a separate job if needed
    """
    try:
        print("📦 Writing parquet files to warehouse...")
        
        output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
        warehouse_path = config.warehouseReportDir
        
        # Read and write org hierarchy
        org_hierarchy = spark.read.parquet(f"{output_path}/orgHierarchy") \
            .withColumn("mdo_created_on", to_date(col("mdo_created_on")).cast("string"))
        
        org_hierarchy.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/{config.dwOrgTable}")
        print(f"✓ Written: {warehouse_path}/{config.dwOrgTable}")
        
        # Read and write events
        events = spark.read.parquet(f"{output_path}/eventDetails") \
            .select("event_id", "event_name", "event_provider_mdo_id", "event_start_datetime",
                    F.col("duration").cast("string").alias("duration"), "event_status", "event_type", 
                    "presenters", "video_link", "recording_link","event_tag", F.col("typeofEvent").alias("type_of_event"),
                    F.col("maxEnrolments").alias("max_enrolments"), F.col("meetingAgenda").alias("meeting_agenda"),
                    F.col("creatorDetails").alias("creator_details"), F.col("recordedMediaLink".alias("recorded_media_link")), 
                    F.col("noOfAttendes").alias("no_of_attendes"),F.col("eventDuration").alias("event_duration"), 
                    F.col("meetingSummary").alias("meeting_summary"), F.col("courseLinked").alias("course_linked")
    )
 
        
        events.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/event_details")
        print(f"✓ Written: {warehouse_path}/event_details")
        
        # Read and process event enrollments with karma points
        eventEnrolmentsDF = spark.read.parquet(f"{output_path}/eventEnrolmentDetails")
        
        karmaPointsData = spark.read.parquet(f"{output_path}/userKarmaPoints") \
            .select(F.col("userid").alias("user_id"),
                    F.col("context_id").alias("event_id"),
                    F.col("points")) \
            .withColumn("points",
                        F.when(F.col("points").cast("int").isNotNull(), F.col("points").cast("int")).otherwise(F.lit(0))) \
            .groupBy("user_id", "event_id") \
            .agg(F.sum("points").alias("karma_points"))
        
        eventsEnrolmentDataDFWithKarmaPoints = eventEnrolmentsDF.join(karmaPointsData, ["user_id", "event_id"], "left")
        
        eventsEnrolmentDataDFWithKarmaPoints.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            f"{warehouse_path}/event_enrolment_details")
        print(f"✓ Written: {warehouse_path}/event_enrolment_details")
        
        print("✅ All parquet files written successfully!")
        
    except Exception as e:
        print(f"❌ Error writing parquet files: {str(e)}")
        raise