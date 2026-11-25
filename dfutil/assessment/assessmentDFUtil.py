import time
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.content import contentDFUtil
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, explode_outer, from_json,
    unix_timestamp, expr, lit, concat_ws, when, coalesce, row_number , first, count, sum
)
from pyspark.sql.types import FloatType, IntegerType
from util import schemas


def print_dataframe_info(df: DataFrame, df_name: str, show_sample: bool = True, sample_rows: int = 5):
    """
    Utility function to print comprehensive DataFrame information.
    """
    print(f"\n{'=' * 60}")
    print(f"📊 DataFrame Info: {df_name}")
    print(f"{'=' * 60}")

    try:
        # row_count = df.count()
        # print(f"📈 Row Count: {row_count:,}")

        # print(f"\n🔧 Schema:")
        # df.printSchema()

        # if show_sample and row_count > 0:
        #     print(f"\n📋 Sample Data (First {min(sample_rows, row_count)} rows):")
        #     df.show(sample_rows, truncate=False)

        print(f"📊 Column Count: {len(df.columns)}")
        print(f"📝 Columns: {', '.join(df.columns)}")

    except Exception as e:
        print(f"❌ Error getting DataFrame info: {str(e)}")

    print(f"{'=' * 60}\n")


def precomputeAssessmentEsDataframe(spark: SparkSession) -> DataFrame:
    """
    Creates assessment DataFrame from Elasticsearch data with comprehensive logging.
    """
    print(f"\n🚀 Starting assessment_es_dataframe function")
    start_time = time.time()

    try:
        print("📥 Fetching data from Elasticsearch...")
        raw_df = contentDFUtil.esContentDataFrame(spark)
        # print(f"✅ Raw ES data loaded - Row count: {raw_df.count():,}")

        print("🔄 Exploding createdFor column...")
        exploded_df = raw_df.withColumn("assessOrgID", explode_outer(col("createdFor")))
        # print(f"✅ After exploding createdFor - Row count: {exploded_df.count():,}")

        print("🔧 Selecting and aliasing columns...")
        assessmentdf = exploded_df.select(
            col("identifier").alias("assessID"),
            col("primaryCategory").alias("assessCategory"),
            col('courseCategory').alias("assessCourseCategory"),
            col("name").alias("assessName"),
            col("status").alias("assessStatus"),
            col("reviewStatus").alias("assessReviewStatus"),
            col("channel").alias("assessChannel"),
            col("duration").cast(FloatType()).alias("assessDuration"),
            col("leafNodesCount").alias("assessChildCount"),
            col("lastPublishedOn").alias("assessLastPublishedOn"),
            col("assessOrgID")
        )

        # print("🗑️ Removing duplicates...")
        # before_dedup = assessmentdf.count()
        assessmentdf = assessmentdf.dropDuplicates(["assessID", "assessCategory"])
        # after_dedup = assessmentdf.count()
        # print(f"📊 Deduplication: {before_dedup:,} → {after_dedup:,} rows (removed {before_dedup - after_dedup:,})")

        print("🔧 Filling null values...")
        assessmentdf = assessmentdf.na.fill({"assessDuration": 0.0, "assessChildCount": 0})

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(assessmentdf, "Assessment ES DataFrame")

        exportDFToParquet(assessmentdf, ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE)


    except Exception as e:
        print(f"❌ Error in assessment_es_dataframe: {str(e)}")
        raise


def precomputeOldAssessmentDataframe(spark: SparkSession) -> DataFrame:
    """
    Creates old assessment DataFrame from cassandra data with comprehensive logging.
    """
    print(f"\n🚀 Starting old_assessment_dataframe function")
    start_time = time.time()

    try:
        print("📥 Fetching data from cassandra cache...")
        raw_df = spark.read.parquet(ParquetFileConstants.OLD_ASSESSMENT_PARQUET_FILE) \
            .withColumnRenamed("user_id", "userID") \
            .withColumnRenamed("parent_source_id", "courseID")

        print("🔧 Selecting and aliasing columns...")
        oldassessmentdf = raw_df \
            .join(spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE), on="courseID",
                  how="left") \
            .join(spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE), on="userID", how="left") \
            .withColumn("assessment_type", lit("Learning Resource")) \
            .withColumn("total_questions", col("correct_count") + col("incorrect_count") + col("not_answered_count")) \
            .withColumn("assessment_publish_date", lit(None).cast("date")) \
            .withColumn("assessment_duration", lit(None).cast("string")) \
            .withColumn("last_attempted_date", lit(None).cast("date")) \
            .withColumn("retakes", lit(None).cast("integer")) \
            .withColumn("assessEndTime", lit(None).cast("bigint")) \
            .withColumn("assessChildID", lit(None).cast("string")) \
            .withColumn("Pass", when(col("result_percent") >= col("pass_percent"), lit("Yes")).otherwise(lit("No"))) \
            .withColumn("Tags", concat_ws(", ", col("additionalProperties.tag")))

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        exportDFToParquet(oldassessmentdf, ParquetFileConstants.OLD_ASSESSMENT_COMPUTED_PARQUET_FILE)


    except Exception as e:
        print(f"❌ Error in assessment_es_dataframe: {str(e)}")
        raise


def transform_assessment_data(assess_with_hierarchy_data: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Joins assessment data with org data, flattens nested 'data' fields,
    and converts timestamp strings to long with comprehensive logging.
    """
    print(f"\n🚀 Starting transform_assessment_data function")
    start_time = time.time()

    try:
        print("📊 Input DataFrames info:")
        # print(f"   - Assessment with hierarchy rows: {assess_with_hierarchy_data.count():,}")
        # print(f"   - Organization rows: {org_df.count():,}")

        print("🔄 Preparing organization DataFrame for join...")
        org_join_df = org_df.select(
            col("orgID").alias("assessOrgID"),
            col("orgName").alias("assessOrgName"),
            col("orgStatus").alias("assessOrgStatus")
        )
        print(f"✅ Organization DataFrame prepared - Columns: {org_join_df.columns}")

        print("🔗 Joining assessment data with organization data...")
        df = assess_with_hierarchy_data.join(org_join_df, on="assessOrgID", how="left")
        # join_result_count = df.count()
        # print(f"✅ Join completed - Result rows: {join_result_count:,}")

        print("📋 Flattening nested 'data' fields...")
        df = df \
            .withColumn("children", col("data.children")) \
            .withColumn("assessPublishType", col("data.publish_type")) \
            .withColumn("assessIsExternal", col("data.isExternal")) \
            .withColumn("assessContentType", col("data.contentType")) \
            .withColumn("assessObjectType", col("data.objectType")) \
            .withColumn("assessUserConsent", col("data.userConsent")) \
            .withColumn("assessVisibility", col("data.visibility")) \
            .withColumn("assessCreatedOn", col("data.createdOn")) \
            .withColumn("assessLastUpdatedOn", col("data.lastUpdatedOn")) \
            .withColumn("assessLastSubmittedOn", col("data.lastSubmittedOn")) \
            .drop("data")

        print("✅ Data flattening completed")

        print("📅 Converting timestamp columns to Unix timestamps...")
        timestamp_cols = [
            "assessCreatedOn", "assessLastUpdatedOn",
            "assessLastPublishedOn", "assessLastSubmittedOn"
        ]

        for i, col_name in enumerate(timestamp_cols, 1):
            print(f"   {i}. Converting {col_name}...")
            df = df.withColumn(
                col_name,
                unix_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss").cast("long")
            )

        print("✅ All timestamp conversions completed")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Transformed Assessment Data")

        return df

    except Exception as e:
        print(f"❌ Error in transform_assessment_data: {str(e)}")
        raise

def transform_assessment_data_flat(assessment_df: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Flat-hierarchy version of transform_assessment_data: org join + publish timestamp only.
    The nested 'data' fields are no longer needed.
    """
    org_join_df = org_df.select(
        col("orgID").alias("assessOrgID"),
        col("orgName").alias("assessOrgName"),
        col("orgStatus").alias("assessOrgStatus")
    )
    return assessment_df.join(org_join_df, on="assessOrgID", how="left") \
        .withColumn("assessLastPublishedOn",
                    unix_timestamp(col("assessLastPublishedOn"), "yyyy-MM-dd'T'HH:mm:ss").cast("long"))

def assessment_children_from_flat_hierarchy(assessment_df: DataFrame, flat_hierarchy_df: DataFrame) -> DataFrame:
    """
    Direct (level-1) children of each assessment from the flat content hierarchy.
    Same output columns as assessment_children_dataframe.
    """
    children_df = flat_hierarchy_df \
        .filter((col("parent_id") == col("root_content_id")) &
                (col("identifier") != col("root_content_id"))) \
        .select(
        col("root_content_id").alias("assessID"),
        col("identifier").alias("assessChildID"),
        col("name").alias("assessChildName"),
        col("duration").cast(FloatType()).alias("assessChildDuration"),
        col("primaryCategory").alias("assessChildPrimaryCategory"),
        lit(None).cast("string").alias("assessChildContentType"),
        lit(None).cast("string").alias("assessChildObjectType"),
        lit(None).cast("string").alias("assessChildShowTimer"),
        lit(None).cast("string").alias("assessChildAllowSkip")
    )

    # Keep only children of assessments in scope (old version derived children from assessment_df itself)
    return children_df \
        .join(assessment_df.select("assessID").distinct(), "assessID", "inner") \
        .dropDuplicates(["assessID", "assessChildID"])

def add_hierarchy_column(
        df: DataFrame,
        hierarchy_df: DataFrame,
        id_col: str,
        as_col: str,
        spark: SparkSession,
        children: bool = False,
        competencies: bool = False,
        l2_children: bool = False
) -> DataFrame:
    """
    Adds a hierarchy struct column to the input DataFrame by joining with the hierarchy DataFrame.
    """
    print(f"\n🚀 Starting add_hierarchy_column function")
    print(f"📋 Parameters:")
    print(f"   - ID Column: {id_col}")
    print(f"   - As Column: {as_col}")
    print(f"   - Children: {children}")
    print(f"   - Competencies: {competencies}")
    print(f"   - L2 Children: {l2_children}")

    start_time = time.time()

    try:
        print("📊 Input DataFrames info:")
        # print(f"   - Main DataFrame rows: {df.count():,}")
        # print(f"   - Hierarchy DataFrame rows: {hierarchy_df.count():,}")

        print("🔧 Creating hierarchy schema...")
        hierarchy_schema = schemas.make_hierarchy_schema(children, competencies, l2_children)
        print(f"✅ Hierarchy schema created: {hierarchy_schema}")

        print(f"🔗 Joining DataFrames on {id_col} = identifier...")
        joined_df = df.join(
            hierarchy_df,
            df[id_col] == hierarchy_df["identifier"],
            how="left"
        )
        # join_count = joined_df.count()
        # print(f"✅ Join completed - Result rows: {join_count:,}")

        print("🔧 Processing hierarchy column...")
        result_df = joined_df \
            .fillna("{}", subset=["hierarchy"]) \
            .withColumn(as_col, from_json(col("hierarchy"), hierarchy_schema)) \
            .drop("hierarchy")

        print("✅ Hierarchy column processing completed")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(result_df, f"DataFrame with Hierarchy Column ({as_col})")

        return result_df

    except Exception as e:
        print(f"❌ Error in add_hierarchy_column: {str(e)}")
        raise


def assessment_children_dataframe(assess_with_hierarchy_df: DataFrame) -> DataFrame:
    """
    Extracts and flattens children data from the given assessment DataFrame with comprehensive logging.
    """
    print(f"\n🚀 Starting assessment_children_dataframe function")
    start_time = time.time()

    try:
        # input_count = assess_with_hierarchy_df.count()
        # print(f"📊 Input DataFrame rows: {input_count:,}")

        print("🔄 Exploding children array...")
        exploded_df = assess_with_hierarchy_df.select(col("assessID"), explode_outer(col("children")).alias("ch"))

        child_df = exploded_df.filter(col("ch").isNotNull()).select(
            col("assessID"),
            col("ch.identifier").alias("assessChildID"),
            col("ch.name").alias("assessChildName"),
            col("ch.duration").cast(FloatType()).alias("assessChildDuration"),
            col("ch.primaryCategory").alias("assessChildPrimaryCategory"),
            col("ch.contentType").alias("assessChildContentType"),
            col("ch.objectType").alias("assessChildObjectType"),
            col("ch.showTimer").alias("assessChildShowTimer"),
            col("ch.allowSkip").alias("assessChildAllowSkip"))

        self_df = assess_with_hierarchy_df.select(
            col("assessID"),
            col("assessID").alias("assessChildID"),
            col("assessName").alias("assessChildName"),
            col("assessDuration").alias("assessChildDuration"),
            col("assessCategory").alias("assessChildPrimaryCategory"),
            col("assessContentType").alias("assessChildContentType"),
            col("assessObjectType").alias("assessChildObjectType"),
            lit(None).cast("string").alias("assessChildShowTimer"),
            lit(None).cast("string").alias("assessChildAllowSkip"))

        df = child_df.unionByName(self_df).dropDuplicates(["assessID", "assessChildID"])
        # exploded_count = exploded_df.count()
        # print(f"✅ After exploding children - Rows: {exploded_count:,}")

        print("🔧 Selecting and casting child columns...")
        df = exploded_df.select(
            col("assessID"),  # courseID
            col("ch.identifier").alias("assessChildID"),  # assessmentID
            col("ch.name").alias("assessChildName"),
            col("ch.duration").cast(FloatType()).alias("assessChildDuration"),
            col("ch.primaryCategory").alias("assessChildPrimaryCategory"),
            col("ch.contentType").alias("assessChildContentType"),
            col("ch.objectType").alias("assessChildObjectType"),
            col("ch.showTimer").alias("assessChildShowTimer"),
            col("ch.allowSkip").alias("assessChildAllowSkip")
        )

        # final_count = df.count()
        # print(f"✅ Final DataFrame rows: {final_count:,}")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Assessment Children DataFrame")

        return df

    except Exception as e:
        print(f"❌ Error in assessment_children_dataframe: {str(e)}")
        raise

def standalone_assessment_enrolments_dataframe(spark: SparkSession) -> DataFrame:
    """
    Standalone Assessments have no real 'enrolment' event (no batch, no
    ENROLMENT_PARQUET_FILE record) - users just attempt the assessment
    directly. This synthesizes ONE enrolment-style row per (userID, parent
    assessID), so a Standalone Assessment with multiple child leaf assessments
    (e.g. 'Baseline Survey' + 'Final Assessment') still produces a single row
    per user, matching user_enrolments' one-row-per-(user_id, content_id) shape.

    Logic (per user, per parent assessID, across ALL of that assessment's
    children combined):
      - If the user has ANY SUBMITTED attempt on any child: pick the SINGLE
        highest-scored submitted attempt across all children (same ranking
        logic as CourseBasedAssessmentModel Stage 5: assessOverallResult, or
        assessTotalSectionMarks for sectional assessments) - this is "the"
        row, matching what assessment_detail would show as their best result.
          -> user_consumption_status = 'completed'
          -> enrolled_on = that row's assessEndTime (submission/attempt time)
          -> certificate_generated = 'Yes'/'No' from that row's assessPass
          -> content_progress_percentage = 100.0
      - Else (only non-submitted attempts exist): pick their most recent
        attempt.
          -> user_consumption_status = 'in-progress'
          -> enrolled_on = that row's assessStartTime
          -> certificate_generated = 'No'
          -> content_progress_percentage = 0.0

    .img phantom parent IDs (see CourseBasedAssessmentModel fix) are excluded
    up front so they can't collide with the real assessID here either.
    """
    from pyspark.sql.functions import col, when, lit, row_number
    from pyspark.sql.window import Window

    # --- Standalone Assessment content only, .img phantoms excluded ---
    assessmentDF = spark.read.parquet(ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE) \
        .filter(col("assessCategory") == "Standalone Assessment") \
        .filter(~col("assessID").endswith(".img"))

    hierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_FLATTENED_ENHANCED_PARQUET_FILE) \
        .select("identifier", "root_content_id", "parent_id", "name", "primaryCategory", "duration")

    assessChildrenDF = assessment_children_from_flat_hierarchy(assessmentDF, hierarchyDF)  # assessID (parent), assessChildID

    # --- Raw attempts, ALL statuses (need non-submitted ones for 'in-progress') ---
    rawUserAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) \
        .withColumn("assessStartTime", col("assessStartTimestamp").cast("long")) \
        .withColumn("assessEndTime", col("assessEndTimestamp").cast("long"))

    # Attach parent assessID to every attempt (inner join naturally drops
    # attempts against non-Standalone-Assessment content).
    userWithParentDF = rawUserAssessmentDF.join(
        assessChildrenDF.select("assessID", "assessChildID"), on="assessChildID", how="inner"
    )

    # --- COMPLETED branch: single highest-scored SUBMITTED attempt per
    #     (assessID, userID), ranked ACROSS all children of that parent ---
    submittedDF = userWithParentDF.filter(col("assessUserStatus") == "SUBMITTED")

    windowBasicAll = Window.partitionBy("assessID", "userID").orderBy(col("assessOverallResult").desc())
    windowSectionalAll = Window.partitionBy("assessID", "userID").orderBy(col("assessTotalSectionMarks").desc())

    completedDF = submittedDF \
        .withColumn("rn",
                    when(col("assessTotalSectionMarks").isNull(), row_number().over(windowBasicAll))
                    .otherwise(row_number().over(windowSectionalAll))) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .select(
        col("assessID").alias("content_id"),
        col("userID"),
        col("assessEndTime").alias("enrolled_on_epoch"),      # submit time of the highest-scored attempt
        col("assessEndTime").alias("last_accessed_epoch"),
        lit("completed").alias("user_consumption_status"),
        when(col("assessPass") == 1, "Yes").otherwise("No").alias("certificate_generated"),
        lit(100.0).alias("content_progress_percentage")
    )

    # --- IN-PROGRESS branch: users with only non-submitted attempts, most
    #     recent one picked (no score exists yet to rank by) ---
    inProgressRaw = userWithParentDF.filter(col("assessUserStatus") != "SUBMITTED")
    windowRecent = Window.partitionBy("assessID", "userID").orderBy(col("assessStartTime").desc())

    inProgressPicked = inProgressRaw \
        .withColumn("rn", row_number().over(windowRecent)) \
        .filter(col("rn") == 1) \
        .drop("rn") \
        .select(
        col("assessID").alias("content_id"),
        col("userID"),
        col("assessStartTime").alias("enrolled_on_epoch"),
        col("assessStartTime").alias("last_accessed_epoch"),
        lit("in-progress").alias("user_consumption_status"),
        lit("No").alias("certificate_generated"),
        lit(0.0).alias("content_progress_percentage")
    )

    # A user who already has a completed row for this assessID should not
    # ALSO show as in-progress for the same assessID.
    inProgressFinal = inProgressPicked.join(
        completedDF.select("content_id", "userID"),
        on=["content_id", "userID"], how="left_anti"
    )

    result = completedDF.unionByName(inProgressFinal) \
        .withColumn("batchID", lit("Not Available")) \
        .withColumn("enrolment_status", lit("enrolled"))

    return result

def user_assessment_children_dataframe(user_assessment_df: DataFrame, assess_children_df: DataFrame) -> DataFrame:
    """
    Joins user assessment data with assessment children data on 'assessChildID' with comprehensive logging.
    """
    print(f"\n🚀 Starting user_assessment_children_dataframe function")
    start_time = time.time()

    try:
        # print("📊 Input DataFrames info:")
        # user_count = user_assessment_df.count()
        # children_count = assess_children_df.count()
        # print(f"   - User Assessment rows: {user_count:,}")
        # print(f"   - Assessment Children rows: {children_count:,}")

        print("🔗 Performing inner join on 'assessChildID'...")
        df = user_assessment_df.join(assess_children_df, on="assessChildID", how="inner")

        result_count = df.count()
        print(f"✅ Join completed - Result rows: {result_count:,}")

        # Calculate join efficiency
        # if user_count > 0 and children_count > 0:
        #     join_efficiency = (result_count / min(user_count, children_count)) * 100
        #     print(f"📊 Join efficiency: {join_efficiency:.2f}%")

        # execution_time = time.time() - start_time
        # print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "User Assessment Children DataFrame", show_sample=True, sample_rows=3)

        return df

    except Exception as e:
        print(f"❌ Error in user_assessment_children_dataframe: {str(e)}")
        raise

def user_assessment_children_details_dataframe(
        user_assess_children_df: DataFrame,
        assess_with_details_df: DataFrame,
        all_course_programdetails_with_rating_df: DataFrame,
        user_org_df: DataFrame
) -> DataFrame:
    """
    Joins user assessment data with assessment details, course details, and user org info with comprehensive logging.
    """
    print(f"\n🚀 Starting user_assessment_children_details_dataframe function")
    start_time = time.time()

    try:
        # print("📊 Input DataFrames info:")
        # user_assess_count = user_assess_children_df.count()
        # assess_details_count = assess_with_details_df.count()
        # course_details_count = all_course_programdetails_with_rating_df.count()
        # user_org_count = user_org_df.count()

        # print(f"   - User Assessment Children rows: {user_assess_count:,}")
        # print(f"   - Assessment Details rows: {assess_details_count:,}")
        # print(f"   - Course Program Details rows: {course_details_count:,}")
        # print(f"   - User Organization rows: {user_org_count:,}")

        print("🗑️ Dropping rating count columns from course data...")
        course_df = all_course_programdetails_with_rating_df.drop(
            "count1Star", "count2Star", "count3Star", "count4Star", "count5Star"
        )
        print(f"✅ Course DataFrame cleaned - Remaining columns: {len(course_df.columns)}")

        print("🔗 Step 1: Joining with assessment details...")
        df1 = user_assess_children_df.join(assess_with_details_df, on="assessID", how="left")
        # step1_count = df1.count()
        # print(f"✅ Step 1 completed - Rows: {step1_count:,}")

        print("🔗 Step 2: Joining with course details...")
        df2 = df1.join(course_df, on="courseID", how="left")
        # step2_count = df2.count()
        # print(f"✅ Step 2 completed - Rows: {step2_count:,}")

        print("🔗 Step 3: Joining with user organization data...")
        df = df2.join(user_org_df, on="userID", how="left")
        # final_count = df.count()
        # print(f"✅ Step 3 completed - Final rows: {final_count:,}")

        # print("📊 Join Summary:")
        # print(f"   - Initial → After assess details: {user_assess_count:,} → {step1_count:,}")
        # print(f"   - After assess details → After course details: {step1_count:,} → {step2_count:,}")
        # print(f"   - After course details → Final: {step2_count:,} → {final_count:,}")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "User Assessment Children Details DataFrame", show_sample=True, sample_rows=2)

        return df

    except Exception as e:
        print(f"❌ Error in user_assessment_children_details_dataframe: {str(e)}")
        raise


def all_course_program_details_with_competencies_json_dataframe(
        all_course_program_es_df: DataFrame,
        hierarchy_df: DataFrame,
        org_df: DataFrame,
        spark: SparkSession
) -> DataFrame:
    """
    Enriches course data with hierarchy, competencies, and organization details with comprehensive logging.
    """
    print(f"\n🚀 Starting all_course_program_details_with_competencies_json_dataframe function")
    start_time = time.time()

    try:
        # print("📊 Input DataFrames info:")
        # course_count = all_course_program_es_df.count()
        # hierarchy_count = hierarchy_df.count()
        # org_count = org_df.count()

        # print(f"   - Course Program ES rows: {course_count:,}")
        # print(f"   - Hierarchy rows: {hierarchy_count:,}")
        # print(f"   - Organization rows: {org_count:,}")

        print("🔧 Step 1: Adding hierarchy column with competencies...")
        df_with_hierarchy = add_hierarchy_column(
            all_course_program_es_df,
            hierarchy_df,
            id_col="courseID",
            as_col="data",
            spark=spark,
            competencies=True
        )

        print("📋 Extracting competencies JSON...")
        df_with_hierarchy = df_with_hierarchy.withColumn("competenciesJson", col("data.competencies_v3"))
        print("✅ Competencies JSON extracted")

        print("🔧 Step 2: Adding course organization details...")
        course_org_details_df = add_course_org_details(df_with_hierarchy, org_df)

        print("🔧 Filling null values...")
        final_df = course_org_details_df \
            .na.fill(0.0, subset=["courseDuration"]) \
            .na.fill(0, subset=["courseResourceCount"]) \
            .drop("data")

        # final_count = final_df.count()
        # print(f"✅ Final DataFrame rows: {final_count:,}")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(final_df, "Course Program Details with Competencies JSON", show_sample=True, sample_rows=2)

        return final_df

    except Exception as e:
        print(f"❌ Error in all_course_program_details_with_competencies_json_dataframe: {str(e)}")
        raise


def all_course_program_details_with_rating_df(
        all_course_program_details_df: DataFrame,
        course_rating_df: DataFrame
) -> DataFrame:
    """
    Joins course program details with rating information with comprehensive logging.
    """
    print(f"\n🚀 Starting all_course_program_details_with_rating_df function")
    start_time = time.time()

    try:
        # print("📊 Input DataFrames info:")
        # course_details_count = all_course_program_details_df.count()
        # rating_count = course_rating_df.count()

        # print(f"   - Course Program Details rows: {course_details_count:,}")
        # print(f"   - Course Rating rows: {rating_count:,}")

        print("🔧 Creating categoryLower column...")
        df_with_category = all_course_program_details_df.withColumn("categoryLower", expr("LOWER(category)"))
        print("✅ categoryLower column created")

        print("🔗 Joining with rating data on courseID and categoryLower...")
        df = df_with_category.join(course_rating_df, ["courseID", "categoryLower"], "left")

        final_count = df.count()
        print(f"✅ Join completed - Final rows: {final_count:,}")

        # # Calculate join statistics
        # if course_details_count > 0:
        #     join_ratio = (final_count / course_details_count) * 100
        #     print(f"📊 Join ratio: {join_ratio:.2f}% (should be ~100% for left join)")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Course Program Details with Rating", show_sample=True, sample_rows=2)

        return df

    except Exception as e:
        print(f"❌ Error in all_course_program_details_with_rating_df: {str(e)}")
        raise


def add_course_org_details(course_df: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Adds organization details to course DataFrame with comprehensive logging.
    """
    print(f"\n🚀 Starting add_course_org_details function")
    start_time = time.time()

    try:
        # print("📊 Input DataFrames info:")
        # course_count = course_df.count()
        # org_count = org_df.count()

        # print(f"   - Course DataFrame rows: {course_count:,}")
        # print(f"   - Organization DataFrame rows: {org_count:,}")

        print("🔧 Preparing organization DataFrame for join...")
        join_org_df = org_df.select(
            col("orgID").alias("courseOrgID"),
            col("orgName").alias("courseOrgName"),
            col("orgStatus").alias("courseOrgStatus")
        )
        print(f"✅ Organization DataFrame prepared - Columns: {join_org_df.columns}")

        print("🔗 Joining course data with organization details...")
        df = course_df.join(join_org_df, on="courseOrgID", how="left")

        # final_count = df.count()
        # print(f"✅ Join completed - Final rows: {final_count:,}")

        # Verify join integrity
        # if course_count == final_count:
        #     print("✅ Join integrity verified - Row count maintained")
        # else:
        #     print(f"⚠️ Warning: Row count changed from {course_count:,} to {final_count:,}")

        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")

        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Course with Organization Details", show_sample=True, sample_rows=2)

        return df

    except Exception as e:
        print(f"❌ Error in add_course_org_details: {str(e)}")
        raise

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType


PARSED_ASSESSMENT_COLUMNS = ("parsedReadResponse", "parsedSubmitRequest", "parsedSubmitResponse")


def with_parsed_assessment_columns(df: DataFrame) -> DataFrame:
    """Used by stage 0: adds the JSON fields stage 1 needs as typed struct columns, next to the raw JSON."""
    return (df
            .withColumn("parsedReadResponse",
                        F.from_json("assessmentreadresponse", schemas.assessment_read_response_schema))
            .withColumn("parsedSubmitRequest",
                        F.from_json("submitassessmentrequest", schemas.submit_assessment_request_schema))
            .withColumn("parsedSubmitResponse",
                        F.from_json("submitassessmentresponse", schemas.submit_assessment_response_schema_with_children)))


def build_user_assessment_df(user_assessment_df: DataFrame, questionset_hierarchy_df: DataFrame) -> DataFrame:
    # 1. use the struct columns stage 0 wrote; parse the JSON only for an older dump that lacks them
    if not all(c in user_assessment_df.columns for c in PARSED_ASSESSMENT_COLUMNS):
        user_assessment_df = with_parsed_assessment_columns(user_assessment_df)
    ua = (user_assessment_df
          .withColumnRenamed("parsedReadResponse", "readResponse")
          .withColumnRenamed("parsedSubmitRequest", "submitRequest")
          .withColumnRenamed("parsedSubmitResponse", "submitResponse"))

    # 2. section stats per row, directly on the children array
    sections = F.col("submitResponse.children")
    n_sections = F.size(sections)
    marks = F.transform(sections, lambda s: s["sectionMarks"].cast(FloatType()).cast(DoubleType()))

    ua = (ua
        # old explode_outer turned a null/empty array into one null row, so count(*) was 1 there
        .withColumn("_secCount", F.when(n_sections > 0, n_sections).otherwise(F.lit(1)).cast(LongType()))
        .withColumn("_secPassed", F.when(sections.isNull(), F.lit(0))
                    .otherwise(F.size(F.filter(sections, lambda s: s["sectionResult"] == "pass")))
                    .cast(LongType()))
        # old sum() over all-null marks returned null, so only aggregate when a mark exists
        .withColumn("_secMarks", F.when(
            F.exists(marks, lambda m: m.isNotNull()),
            F.aggregate(marks, F.lit(0.0).cast(DoubleType()), lambda acc, m: acc + F.coalesce(m, F.lit(0.0)))))
        .withColumn("_passPct", F.col("submitResponse.passPercentage").cast(FloatType()))
        .withColumn("_overall", F.col("submitResponse.overallResult").cast(FloatType()))
        .withColumn("_totalPct", F.col("submitResponse.totalPercentage").cast(FloatType())))

    # 3. small lookup table: one row per questionset, broadcast
    qh = (questionset_hierarchy_df
          .select("questionsetID", "questionsetMinimumPassPercentage", "scoreCutoffType", "questionsetNoOfSection")
          .dropDuplicates(["questionsetID"]))
    df = ua.join(F.broadcast(qh), ua["assessChildID"] == qh["questionsetID"], "left")

    # 4. pass/fail logic (same rules as before; applied only where the questionset is known)
    matched = F.col("questionsetID").isNotNull()
    section_level = F.col("scoreCutoffType") == "SectionLevel"
    effective_pct = (F.when(F.col("_passPct").isNotNull(), F.col("_passPct"))
                     .otherwise(F.col("questionsetMinimumPassPercentage")))
    final_pass = F.when(
        section_level,
        F.when((F.col("_secCount") == F.col("questionsetNoOfSection")) &
               (F.col("_secPassed") == F.col("_secCount")), F.lit(1)).otherwise(F.lit(0))
    ).otherwise(F.when(F.col("_overall") >= effective_pct, F.lit(1)).otherwise(F.lit(0)))
    computed_marks = F.when(section_level, F.col("_secMarks")).otherwise(F.col("_overall"))
    overall_new = F.when(computed_marks.isNotNull(), F.col("_totalPct")).otherwise(F.col("_overall"))

    return df.select(
        F.col("assessChildID"), F.col("assessUserStatus"), F.col("userID"), F.col("assessLanguage"),
        F.col("readResponse.totalQuestions").alias("assessTotalQuestions"),
        F.col("readResponse.maxQuestions").alias("assessMaxQuestions"),
        F.col("readResponse.expectedDuration").alias("assessExpectedDuration"),
        F.col("readResponse.version").alias("assessVersion"),
        F.col("readResponse.maxAssessmentRetakeAttempts").alias("assessMaxRetakeAttempts"),
        F.col("readResponse.status").alias("assessReadStatus"),
        F.col("readResponse.primaryCategory").alias("assessPrimaryCategory"),
        F.col("submitRequest.batchId").alias("assessBatchID"),
        F.col("submitRequest.courseId").alias("courseID"),
        F.col("submitRequest.isAssessment").cast(IntegerType()).alias("assessIsAssessment"),
        F.col("submitRequest.timeLimit").alias("assessTimeLimit"),
        F.col("submitResponse.result").alias("assessResult"),
        F.col("submitResponse.total").alias("assessTotal"),
        F.col("submitResponse.blank").alias("assessBlank"),
        F.col("submitResponse.correct").alias("assessCorrect"),
        F.col("submitResponse.incorrect").alias("assessIncorrect"),
        F.coalesce(F.when(matched, effective_pct), F.col("submitResponse.passPercentage")).alias("assessPassPercentage"),
        F.col("submitResponse.totalSectionMarks").alias("assessTotalSectionMarks"),
        F.coalesce(F.when(matched, overall_new), F.col("submitResponse.overallResult")).alias("assessOverallResult"),
        F.col("submitResponse.totalMarks").alias("assessTotalMarks"),
        F.col("assessStartTime").alias("assessStartTimestamp"),
        F.col("assessEndTime").alias("assessEndTimestamp"),
        F.when(matched, F.col("_totalPct")).alias("assessTotalPercentage"),
        F.when(matched, final_pass)
         .otherwise(F.col("submitResponse.pass").cast(IntegerType())).alias("assessPass"),
    )


def parse_raw_assessment_data(spark: SparkSession, config):
    """Builds userAssessment from the stage-0 dump. Reads only the parsed struct columns, not the raw JSON."""
    output_base_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
    try:
        user_assessment_df = spark.read.parquet(ParquetFileConstants.ASSESSMENT_DATA_RAW_PARQUET_FILE)
        questionset_hierarchy_df = spark.read.parquet(ParquetFileConstants.QUESTIONSET_HIERARCHY_PARQUET_FILE)

        result = build_user_assessment_df(user_assessment_df, questionset_hierarchy_df)

        # No coalesce: keeps all cores busy. Output file count follows the input splits.
        result.write.mode("overwrite").option("compression", "snappy").parquet(f"{output_base_path}/userAssessment")
    except Exception as e:
        print(f"❌ Error in parse_raw_assessment_data: {str(e)}")
        raise



def write_parquet(df: "DataFrame", path: str, partition_cols: list = None, mode: str = "overwrite"):
        """Write DataFrame to Parquet with optimization"""
        writer = df.coalesce(16)

        if partition_cols:
            writer = writer.write.partitionBy(*partition_cols)
        else:
            writer = writer.write

        writer.mode(mode) \
        .option("compression", "snappy") \
        .parquet(path)


def print_execution_summary():
    """
    Print a summary of the execution.
    """
    print(f"\n{'=' * 80}")
    print(f"🎉 EXECUTION SUMMARY")
    print(f"{'=' * 80}")
    print(f"✅ All functions executed successfully!")
    print(f"📊 Check the detailed logs above for performance metrics")
    print(f"🔍 Review DataFrame schemas and row counts for data quality")
    print(f"{'=' * 80}\n")