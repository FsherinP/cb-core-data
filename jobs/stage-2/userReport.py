import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, upper, concat, exists, split,
    current_timestamp, date_format, from_unixtime, concat_ws, from_json, explode, trim, length, first, expr
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import collect_set
from pyspark.sql.types import ArrayType
import os
import time
from datetime import datetime

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from util import schemas
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import get_environment_config

# Initialize Spark
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "25g") \
    .config("spark.driver.memory", "15g") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def processUserReport(config):
    """
    User Report Generation with minimal traceable steps
    """

    try:
        start_time = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

        # Step 1: Load User Master Data
        print("📊 Step 1: Loading User Master Data...")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        print("✅ Step 1 Complete")

        # Step 2: Load Enrolment Data
        print("📚 Step 2: Loading Enrolment Data...")
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)

        user_badges = (spark.read.parquet(ParquetFileConstants.GAMIFICATION_BADGE_USER_ENROLMENT_PARQUET_FILE)
                       .select(col("userID").alias("user_id"), "badge_id")
                       .groupBy("user_id").agg(expr("count(distinct badge_id)").alias("total_badges_earned")))
        print("✅ Step 2 Complete")

        # Step 3: Load Content Duration
        print("📖 Step 3: Loading Content Duration Data...")
        content_duration_df = (
            spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            .filter((col("courseCategory") == "Course"))
            .select(
                col("courseID").alias("content_id"),
                col("courseDuration").cast("double"),
                col("category")
            )
        )
        print("✅ Step 3 Complete")

        # Step 4: Add User Status Classification
        print("🏷️ Step 4: Classifying User Status...")
        print("✅ Step 4 Complete")

        # Step 5: Join User and Content Data
        print("🔗 Step 5: Joining User and Content Data...")
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, content_duration_df
        )
        print("✅ Step 5 Complete")


        # Step 6: Add Event Metrics
        print("📊 Step 6: Adding Event Metrics...")
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        print("✅ Step 6 Complete")

        # Step 7: Create Derived Columns
        print("✨ Step 7: Creating Derived Columns...")
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0))
                        ) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))
        print("✅ Step 7 Complete")

        # Step 8: Build Warehouse Data (the mdo-wise CSV "user-report" now lives entirely in Step 11,
        # merged with custom fields, and split into Govt/Non-Govt only where an org actually has both)
        print("🎯 Step 8: Preparing Warehouse Data...")

        warehouseDF = user_complete_data \
            .withColumn("marked_as_not_my_user",
                        when(col("userProfileStatus") == "NOT-MY-USER", lit(True)).otherwise(lit(False))) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .withColumn("is_verified_karmayogi",
                        when(col("userProfileStatus") == "VERIFIED", lit(True)).otherwise(lit(False))) \
            .select(
            col("userID").alias("user_id"),
            col("userOrgID").alias("mdo_id"),
            col("userStatus").alias("status"),
            coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
            col("fullName").alias("full_name"),
            col("professionalDetails.designation").alias("designation"),
            col("personalDetails.primaryEmail").alias("email"),
            col("personalDetails.mobile").alias("phone_number"),
            col("personalDetails.pincode").alias("pincode"),
            col("professionalDetails.group").alias("groups"),
            col("Tag").alias("tag"),
            col("userProfileStatus").alias("profile_status"),
            date_format(from_unixtime(col("userCreatedTimestamp") / 1000), ParquetFileConstants.DATE_TIME_FORMAT).alias(
                "user_registration_date"),
            date_format(from_unixtime(col("userUpdatedTimestamp") / 1000), ParquetFileConstants.DATE_TIME_FORMAT).alias(
                "profile_last_updated_date"),
            col("role").alias("roles"),
            col("personalDetails.gender").alias("gender"),
            col("personalDetails.category").alias("category"),
            col("marked_as_not_my_user"),
            col("is_verified_karmayogi"),
            col("userCreatedBy").alias("created_by_id"),
            col("additionalProperties.externalSystem").alias("external_system"),
            col("additionalProperties.externalSystemId").alias("external_system_id"),
            col("weekly_claps_day_before_yesterday"),
            coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("total_event_learning_hours"),
            coalesce(col("total_content_duration"), lit(0)).alias("total_content_learning_hours"),
            coalesce(col("Total_Learning_Hours"), lit(0)).alias("total_learning_hours"),
            col("employmentDetails.employeeCode").alias("employee_id"),
            col("cadreName").alias("cadre"),
            col("civilServiceType").alias("civil_service_type"),
            col("civilServiceName").alias("civil_services"),
            col("cadreBatch").alias("cadre_batch"),
            col("isOnCentralDeputation").alias("is_on_central_deputation"),
            col("organised_service").alias("is_from_organised_service_of_govt"),
            col("data_last_generated_on")
        ).join(user_badges, on="user_id", how="left").fillna(0, subset=["total_badges_earned"])
        print("✅ Step 8 Complete")

        # Step 9: Export Warehouse Data
        print("📁 Step 9: Exporting Warehouse Data...")
        warehouseDF.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{config.warehouseReportDir}/{config.dwUserTable}")
        print("✅ Step 9 Complete")


        # Step 10: Process User Extended Profile Data
        print("🔍 Step 10: Processing User Extended Profile Data...")
        # Load user extended profile data
        user_extended_profile_df = (
            spark.read.parquet(ParquetFileConstants.USER_EXTENDED_PROFILE)
            .filter(col("contexttype") == "orgAdditionalProperties")
            .withColumnRenamed("userid", "userID")
            .withColumn("contextDataArray", from_json(col("contextdata"), ArrayType(schemas.context_data_schema)))
            .withColumn("contextData", explode(col("contextDataArray")))
            .select(
                col("userID"),
                col("contexttype").alias("contextType"),
                col("contextData"),
                col("contextData.organisationId").alias("mdo_id")
            )
        )

        # Step 1: Explode customFieldValues and handle based on type
        exploded_df_base = (
            user_extended_profile_df
            .withColumn("customField", explode(col("contextData.customFieldValues")))
            .select(
                col("userID"),
                col("mdo_id"),
                col("customField.type").alias("field_type"),
                col("customField.attributeName").alias("attribute_name"),
                col("customField.value").alias("direct_value"),
                col("customField.values").alias("values_array")
            )
        )

        # Handle direct values (where type is not "masterList" and direct_value is not null)
        direct_values_df = (
            exploded_df_base
            .filter(
                (col("field_type") != "masterList") &
                col("direct_value").isNotNull()
            )
            .select(
                col("userID"),
                col("mdo_id"),
                col("attribute_name"),  # Use main attributeName for text fields
                col("direct_value").alias("attribute_value")
            )
        )

        # Handle masterList values (where type is "masterList" and values_array is not null)
        master_list_values_df = (
            exploded_df_base
            .filter(
                (col("field_type") == "masterList") &
                col("values_array").isNotNull()
            )
            .withColumn("valueItem", explode(col("values_array")))
            .select(
                col("userID"),
                col("mdo_id"),
                col("valueItem.attributeName").alias("attribute_name"),  # Use nested attributeName for masterList
                col("valueItem.value").alias("attribute_value")
            )
        )

        # Combine both direct values and masterList values
        exploded_df = direct_values_df.union(master_list_values_df).filter(
            col("attribute_name").isNotNull() &
            col("attribute_value").isNotNull()
        )

        # Write to warehouse tables
        exploded_df \
            .select(
            col("userID").alias("user_id"),
            col("mdo_id"),
            col("attribute_name"),
            col("attribute_value")
        ) \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(
            f"{config.warehouseReportDir}/userCustomFields"
        )

        # Cache the exploded data for reuse
        exploded_cached = exploded_df.cache()
        print("✅ Step 10 Complete")

        # Step 11: Create the single MDO-wise User Report (with Custom Fields),
        # split into Govt / Non-Govt ONLY for orgs that actually have both kinds of users.
        print("📋 Step 11: Creating MDO-wise User Report (with Custom Fields, Govt/Non-Govt aware)...")

        mdo_wise_slim = (
            user_complete_data
            .filter(col("userStatus").cast("int") == 1)
            .select(
                col("userID"),
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("userOrgName").alias("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                when(col("ministry_name").isNull(), col("userOrgName"))
                .otherwise(col("ministry_name")).alias("Ministry"),
                when(
                    (col("ministry_name").isNotNull()) &
                    (col("ministry_name") != col("userOrgName")) &
                    ((col("dept_name").isNull()) | (col("dept_name") == "")),
                    col("userOrgName")
                ).otherwise(col("dept_name")).alias("Department"),
                when(
                    (col("ministry_name") != col("userOrgName")) &
                    (col("dept_name") != col("userOrgName")),
                    col("userOrgName")
                ).otherwise(lit("")).alias("Organization"),
                from_unixtime(col("userCreatedTimestamp") / 1000, ParquetFileConstants.DATE_FORMAT).alias("User_Registration_Date"),
                col("role").alias("Roles"),
                col("personalDetails.gender").alias("Gender"),
                col("personalDetails.category").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("employmentDetails.employeeCode").alias("Employee_Id"),
                from_unixtime(col("userOrgCreatedDate") / 1000, ParquetFileConstants.DATE_FORMAT).alias("MDO_Created_On"),
                col("userProfileStatus").alias("Profile_Status"),
                col("weekly_claps_day_before_yesterday"),
                coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
                coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
                coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
                coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
                coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
                coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
                coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
                (coalesce(col("total_event_enrolments"), lit(0)) +
                 coalesce(col("total_content_enrolments"), lit(0))).alias("Total_Enrolments"),
                (coalesce(col("total_event_completions"), lit(0)) +
                 coalesce(col("total_content_completions"), lit(0))).alias("Total_Completions"),
                coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
                lit(currentDateTime).alias("Report_Last_Generated_On"),
                col("userOrgID").alias("mdoid")
            )
            .withColumn(
                # Designation or Roles containing "VOLUNTEER" => Non-Govt user, everything else => Govt user
                "is_non_govt_user",
                when(
                    (trim(upper(coalesce(col("Designation"), lit("")))) == "VOLUNTEER") |
                    coalesce(exists(split(coalesce(col("Roles"), lit("")), ","), lambda r: trim(upper(r)) == "VOLUNTEER"), lit(False)),
                    lit(True)
                ).otherwise(lit(False))
            )
        ).repartition(col("mdoid")).cache()

        govt_mdo_wise_slim = mdo_wise_slim.filter(~col("is_non_govt_user")).drop("is_non_govt_user").cache()
        non_govt_mdo_wise_slim = mdo_wise_slim.filter(col("is_non_govt_user")).drop("is_non_govt_user").cache()

        # Everything goes in ONE parent folder/date.
        base_out = f"standalone-reports/user-report/{today}"

        print("📊 Pre-collecting organization metadata...")
        org_metadata = (
            exploded_cached
            .groupBy("mdo_id")
            .agg(collect_set("attribute_name").alias("custom_fields"))
            .collect()
        )

        org_custom_fields = {
            row.mdo_id: sorted([field for field in row.custom_fields if field and field.strip()])
            for row in org_metadata
        }

        fixed_cols = [
            "userID", "Full_Name", "Designation", "Email", "Phone_Number", "MDO_Name", "Group", "Tag",
            "Ministry", "Department", "Organization", "User_Registration_Date", "Roles", "Gender",
            "Category", "External_System", "External_System_Id", "Employee_Id", "MDO_Created_On",
            "Profile_Status", "weekly_claps_day_before_yesterday", "Karma_Points", "Event_Enrolments",
            "Event_Completions", "Event_Learning_Hours", "Course_Enrolments", "Course_Completions",
            "Course_Learning_Hours", "Total_Enrolments", "Total_Completions", "Total_Learning_Hours",
            "Report_Last_Generated_On", "mdoid"
        ]
        fixed_cols_lower = [col_name.lower() for col_name in fixed_cols]

        def process_single_organization(org_id, user_slim_df, base_out_path, report_label, folder_suffix=""):
            """Process a single organization (govt or non-govt subset) and write its mdo-wise CSV
            (+ custom fields, if any) into mdoid=<org_id><folder_suffix>."""
            attribute_names = []
            try:
                print(f"  Processing [{report_label}] organization: {org_id}{folder_suffix}")

                # Get pre-computed custom fields for this org
                attribute_names = org_custom_fields.get(org_id, [])

                # Filter custom-field data for current organization
                org_data = exploded_cached.filter(col("mdo_id") == org_id)

                # Create pivot
                if attribute_names:
                    pivoted = (
                        org_data
                        .groupBy("userID")
                        .pivot("attribute_name")
                        .agg(first("attribute_value"))
                    )
                else:
                    pivoted = org_data.select("userID").distinct()

                # Handle conflicts
                conflicts = []
                attribute_names_lower = [attr_name.lower() for attr_name in attribute_names]

                for i, attr_lower in enumerate(attribute_names_lower):
                    if attr_lower in fixed_cols_lower:
                        conflicts.append(attribute_names[i])

                # Rename conflicting columns
                renamed_pivoted = pivoted
                custom_field_mapping = {}

                for conflict_col in conflicts:
                    if conflict_col in pivoted.columns:
                        new_name = f"Custom_{conflict_col}"
                        renamed_pivoted = renamed_pivoted.withColumnRenamed(conflict_col, new_name)
                        custom_field_mapping[conflict_col] = new_name

                # User data (govt or non-govt subset) drives the output rows so that:
                #  - the govt/non-govt split is respected
                #  - users with no custom fields still appear (left join keeps them)
                #  - orgs with NO custom fields at all still get a report (attribute_names is empty,
                #    pivoted just carries distinct userIDs, so the join adds no extra columns)
                org_user_data = user_slim_df.filter(col("mdoid") == org_id)
                joined = (
                    org_user_data
                    .join(renamed_pivoted, ["userID"], "left")
                    .withColumn("mdoid", lit(org_id))
                )

                # Create final column list
                final_custom_cols = []
                for attr_name in attribute_names:
                    if attr_name in custom_field_mapping:
                        final_custom_cols.append(custom_field_mapping[attr_name])
                    else:
                        final_custom_cols.append(attr_name)

                available_columns = set(joined.columns)
                existing_fixed_cols = [c for c in fixed_cols if c in available_columns]
                existing_custom_cols = [c for c in final_custom_cols if c in available_columns]
                final_columns = existing_fixed_cols + existing_custom_cols

                # Remove duplicates
                final_columns = list(dict.fromkeys(final_columns))

                def safe_column_reference(col_name):
                    """Create safe column reference for selection"""
                    if any(char in col_name for char in ['.', ' ', '(', ')', '-', '/', '`']):
                        return f"`{col_name}`"
                    else:
                        return col_name

                select_expressions = [safe_column_reference(c) for c in final_columns]

                ordered = joined.selectExpr(*select_expressions)

                out_path = f"{config.localReportDir}/{base_out_path}/mdoid={org_id}{folder_suffix}"
                csv_file_path = f"{out_path}/{config.userReport}"

                os.makedirs(out_path, exist_ok=True)

                result = dfexportutil.write_single_csv_duckdb(
                    df=ordered,
                    output_path=csv_file_path,
                    parquet_tmp_path=f"{out_path}/temp_parquet_{org_id}{folder_suffix}",
                    keep_parquets=False
                )

                # write warehouse files - 'warehouseUserCustomReportDir': 'user_custom_report'
                warehouse_user_custom_report_file = (
                    f"{config.warehouseReportDir}/{config.warehouseUserCustomReportDir}/"
                    f"{org_id}{folder_suffix}_custom_report.parquet"
                )

                dfexportutil.write_single_parquet(
                    df=ordered,
                    final_path=warehouse_user_custom_report_file)

                return {
                    'org_id': f"{org_id}{folder_suffix}",
                    'success': result.get('success', True),
                    'rows_written': result.get('rows_written', 0),
                    'custom_fields_count': len(attribute_names),
                    'error': None
                }

            except Exception as e:
                return {
                    'org_id': f"{org_id}{folder_suffix}",
                    'success': False,
                    'rows_written': 0,
                    'custom_fields_count': len(attribute_names),
                    'error': str(e)
                }

        # Determine, per org, whether it has govt users, non-govt users, or both.
        govt_org_ids = set(
            row.mdoid for row in govt_mdo_wise_slim.select("mdoid").distinct().collect() if row.mdoid is not None
        )
        non_govt_org_ids = set(
            row.mdoid for row in non_govt_mdo_wise_slim.select("mdoid").distinct().collect() if row.mdoid is not None
        )
        both_org_ids = govt_org_ids & non_govt_org_ids

        print(f"Orgs with only Govt users: {len(govt_org_ids - both_org_ids)}")
        print(f"Orgs with only Non-Govt users: {len(non_govt_org_ids - both_org_ids)}")
        print(f"Orgs with BOTH Govt and Non-Govt users (2 reports each): {len(both_org_ids)}")

        # ------------------------------------------------------------------ #
        # PERFORMANCE: only orgs that actually have custom fields need the
        # expensive per-org pivot + join + individual write. Every other org
        # (typically the large majority) is written in bulk via
        # write_csv_per_mdo_id_duckdb, exactly like the original fast version.
        # Doing per-org processing for EVERY org (govt + non-govt, with or
        # without custom fields) is what caused the 30-min job to take 3+ hrs.
        # ------------------------------------------------------------------ #
        orgs_with_custom_fields = set(org_custom_fields.keys())

        govt_orgs_with_custom = govt_org_ids & orgs_with_custom_fields
        non_govt_orgs_with_custom = non_govt_org_ids & orgs_with_custom_fields
        govt_orgs_without_custom = govt_org_ids - orgs_with_custom_fields
        non_govt_orgs_without_custom = non_govt_org_ids - orgs_with_custom_fields

        print(f"Govt orgs needing per-org custom-field processing: {len(govt_orgs_with_custom)}")
        print(f"Non-Govt orgs needing per-org custom-field processing: {len(non_govt_orgs_with_custom)}")
        print(f"Govt orgs going through bulk write (no custom fields): {len(govt_orgs_without_custom)}")
        print(f"Non-Govt orgs going through bulk write (no custom fields): {len(non_govt_orgs_without_custom)}")

        # ---- Slow path: per-org pivot, ONLY for orgs with custom fields ---- #
        # Build the task list:
        #  - Govt subset always writes to mdoid=<org_id> (no suffix)
        #  - Non-Govt subset writes to mdoid=<org_id> (no suffix) UNLESS the org also has
        #    Govt users, in which case it writes to mdoid=<org_id>_non_govt to avoid collision.
        tasks = []
        for org_id in govt_orgs_with_custom:
            tasks.append((org_id, govt_mdo_wise_slim, "GOVT", ""))
        for org_id in non_govt_orgs_with_custom:
            suffix = "_non_govt" if org_id in both_org_ids else ""
            tasks.append((org_id, non_govt_mdo_wise_slim, "NON-GOVT", suffix))

        print(f"Processing {len(tasks)} org report(s) with custom fields using up to 8 parallel workers...")

        successful_orgs = 0
        failed_orgs = 0
        total_rows = 0

        max_workers = min(8, len(tasks)) if tasks else 1

        if max_workers > 1 and len(tasks) > 3:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {
                    executor.submit(process_single_organization, org_id, slim_df, base_out, label, suffix): (org_id, suffix)
                    for (org_id, slim_df, label, suffix) in tasks
                }

                for future in as_completed(future_to_task):
                    result = future.result()

                    if result['success']:
                        successful_orgs += 1
                        total_rows += result['rows_written']
                        print(f"  ✅ {result['org_id']}: {result['rows_written']:,} rows, {result['custom_fields_count']} custom fields")
                    else:
                        failed_orgs += 1
                        print(f"  ❌ {result['org_id']}: {result['error']}")
        else:
            for (org_id, slim_df, label, suffix) in tasks:
                result = process_single_organization(org_id, slim_df, base_out, label, suffix)

                if result['success']:
                    successful_orgs += 1
                    total_rows += result['rows_written']
                    print(f"  ✅ {result['org_id']}: {result['rows_written']:,} rows, {result['custom_fields_count']} custom fields")
                else:
                    failed_orgs += 1
                    print(f"  ❌ {result['org_id']}: {result['error']}")

        # ---- Fast path: bulk write for every org with NO custom fields ---- #
        # dfexportutil.write_csv_per_mdo_id_duckdb throws a KeyError on
        # 'successful_conversions' when handed a dataframe with zero orgs to
        # write, so each bulk call is skipped if its subset is empty.
        if govt_orgs_without_custom:
            print(f"📦 Bulk-writing {len(govt_orgs_without_custom)} Govt org(s) without custom fields...")
            govt_bulk_df = govt_mdo_wise_slim.filter(col("mdoid").isin(list(govt_orgs_without_custom)))
            bulk_govt_result = dfexportutil.write_csv_per_mdo_id_duckdb(
                govt_bulk_df,
                f"{config.localReportDir}/{base_out}",
                'mdoid',
                f"{config.localReportDir}/temp/user_report_bulk_govt/{today}",
                csv_filename=config.userReport
            )
            successful_orgs += bulk_govt_result.get('successful_writes', len(govt_orgs_without_custom))
            failed_orgs += bulk_govt_result.get('failed_writes', 0)
            print(f"  ✅ Govt bulk write done: {bulk_govt_result.get('successful_writes', len(govt_orgs_without_custom))} orgs")
        else:
            print("ℹ️  No Govt orgs without custom fields — skipping Govt bulk write.")

        if non_govt_orgs_without_custom:
            print(f"📦 Bulk-writing {len(non_govt_orgs_without_custom)} Non-Govt org(s) without custom fields...")
            # Same collision rule as the per-org path: only suffix mdoid with
            # _non_govt for orgs that also have Govt users; leave it as-is
            # for orgs that only ever had Non-Govt users.
            both_org_ids_list = list(both_org_ids)
            non_govt_bulk_df = (
                non_govt_mdo_wise_slim
                .filter(col("mdoid").isin(list(non_govt_orgs_without_custom)))
                .withColumn(
                    "mdoid",
                    when(col("mdoid").isin(both_org_ids_list), concat(col("mdoid"), lit("_non_govt")))
                    .otherwise(col("mdoid"))
                )
            )
            bulk_non_govt_result = dfexportutil.write_csv_per_mdo_id_duckdb(
                non_govt_bulk_df,
                f"{config.localReportDir}/{base_out}",
                'mdoid',
                f"{config.localReportDir}/temp/user_report_bulk_non_govt/{today}",
                csv_filename=config.userReport
            )
            successful_orgs += bulk_non_govt_result.get('successful_writes', len(non_govt_orgs_without_custom))
            failed_orgs += bulk_non_govt_result.get('failed_writes', 0)
            print(f"  ✅ Non-Govt bulk write done: {bulk_non_govt_result.get('successful_writes', len(non_govt_orgs_without_custom))} orgs")
        else:
            print("ℹ️  No Non-Govt orgs without custom fields — skipping Non-Govt bulk write.")

        print(f"Done: {successful_orgs} successful, {failed_orgs} failed, {total_rows:,} total rows (per-org path)")

        exploded_cached.unpersist()
        mdo_wise_slim.unpersist()
        govt_mdo_wise_slim.unpersist()
        non_govt_mdo_wise_slim.unpersist()

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] UserReport processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    processUserReport(config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserReport completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()
