import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants

import os
import shutil
import time
import duckdb
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    struct, explode, col, from_json, when, expr, concat_ws, array_join, lit, lower, trim, split, array_position, size
)
from pyspark.sql import functions as F


def preComputeACBPData(spark):
    """
    Pre-process ACBP data from raw parquet files.
    Uses DuckDB for memory-efficient joins (v5.0 - DuckDB optimized).
    """
    print("=" * 80)
    print("ACBP Pre-Processing - Version 5.0 (DuckDB Optimized)")
    print("=" * 80)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)

    # CRITICAL FIX: Pre-process contextdata to wrap ALL scalar criteriaValue in arrays
    acbp_df = acbp_df.withColumn("contextdata",
                                 F.regexp_replace(col("contextdata"),
                                                  '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                                                  '"criteriaValue":[$1]'
                                                  )
                                 )

    acbp_select_df = (acbp_df.withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema))
                      .withColumn("userGroup", explode(col("context_data.accessControl.userGroups")))
                      .withColumn("criteria_keys", expr(
        "transform(userGroup.userGroupCriteriaList, x -> lower(x.criteriaKey))"))
                      .withColumn("criteria_values", expr(
        "transform(userGroup.userGroupCriteriaList, x -> concat_ws(', ', x.criteriaValue))"))
                      .withColumn("assignmentType", array_join(col("criteria_keys"), "|"))
                      .withColumn("assignmentTypeInfo", array_join(col("criteria_values"), "|"))
                      .withColumn(
        "userOrgID",
        expr("""
            CASE
              WHEN array_contains(criteria_keys, 'rootorgid') THEN
                filter(
                  criteria_values,
                  (value, idx) -> criteria_keys[idx] = 'rootorgid'
                )[0]
              ELSE NULL
            END
        """)
    )
                      .select(
        col("planid").alias("acbpID"),
        col("userOrgID").alias("orgID"),
        col("draftdata"),
        col("status").alias("acbpStatus"),
        col("createdby").alias("acbpCreatedBy"),
        col("isapar"),
        col("name").alias("cbPlanName"),
        col("enddate").cast("string").alias("completionDueDate"),
        col("publishedat").cast("string").alias("allocatedOn"),
        col("contentlist").alias("acbpCourseIDList"),
        col("assignmentType"),
        col("assignmentTypeInfo")
    )
                      .na.fill({"cbPlanName": ""})
                      )

    acbp_select_count = acbp_select_df.count()
    print(f"  ACBP select data: {acbp_select_count:,} rows")

    draft_cbp_data = (acbp_select_df
                      .filter((col("acbpStatus") == "draft") & col("draftdata").isNotNull())
                      .select("acbpID", "orgID", "draftdata", "acbpStatus", "acbpCreatedBy", "isapar")
                      .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema))
                      .withColumn("cbPlanName", col("draftData.name"))
                      .withColumn("assignmentType", col("draftData.assignmentType"))
                      .withColumn("assignmentTypeInfo",
                                  array_join(col("draftData.assignmentTypeInfo"), ","))
                      .withColumn("completionDueDate", col("draftData.endDate").cast("string"))
                      .withColumn("allocatedOn", lit(None).cast("string"))  # Use None instead of "not published"
                      .withColumn("acbpCourseIDList", col("draftData.contentList"))
                      .drop("draftData"))

    draft_count = draft_cbp_data.count()
    print(f"  Draft CBP data: {draft_count:,} rows")

    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "draft")
    non_draft_count = non_draft_cbp_data.count()
    print(f"  Non-draft CBP data: {non_draft_count:,} rows")

    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))

    final_df = non_draft_cbp_data.unionByName(draft_cbp_data)
    total_plans = final_df.count()
    print(f"  Total plans (final_df): {total_plans:,} rows")

    exportDFToParquet(final_df, ParquetFileConstants.ACBP_SELECT_FILE)

    # Filter to Live plans only for explosion
    live_acbp_df = final_df.filter(col("acbpStatus") == "Live")
    live_count = live_acbp_df.count()
    print(f"  Live plans for processing: {live_count:,} rows")

    # Call DuckDB-optimized explode function
    explodeAcbpData(spark, live_acbp_df)


def explodeAcbpData(spark, acbp_df: DataFrame) -> DataFrame:
    """
    DuckDB-optimized ACBP explosion - Version 5.2 (Full Criteria Matching with OR Logic)

    Strategy:
    1. Use Spark to parse and explode criteria into individual rows
    2. Write expanded criteria data to temp parquet
    3. Use DuckDB for memory-efficient joins with full criteria matching
    4. Support OR logic: Same acbpID with different criteria combinations
    5. Support global plans (without orgID)
    6. Write final output to parquet

    Supported criteria types:
    - rootorgid: User's org must match
    - alluser: All users in the plan's org
    - user/customuser: Specific user IDs
    - designation: User's designation must match
    - cadre: User's cadre name must match
    - group: User's group must match
    - batch: User's cadre batch must match
    - service: User's civil service name must match
    """
    print("\n" + "=" * 80)
    print("ACBP User Allocation - Version 5.2 (DuckDB + Full Criteria + OR Logic)")
    print("=" * 80)

    start_time = time.time()

    # Step 1: Create temp directory
    project_dir = str(Path(__file__).resolve().parents[3])
    temp_dir = f"{project_dir}/temp_acbp_duckdb"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    print(f"\n[1/7] Parsing ACBP assignment types...")
    print(f"  Temp directory: {temp_dir}")

    # Parse criteria into arrays and explode to get one row per criteria
    acbp_expanded = acbp_df \
        .withColumn("criteria_types_arr", split(lower(col("assignmentType")), "\\|")) \
        .withColumn("criteria_values_arr", split(col("assignmentTypeInfo"), "\\|")) \
        .withColumn("criteria_idx", F.expr("sequence(0, size(criteria_types_arr) - 1)")) \
        .withColumn("criteria_exploded", F.explode(col("criteria_idx"))) \
        .withColumn("criteria_type", F.expr("criteria_types_arr[criteria_exploded]")) \
        .withColumn("criteria_value", F.expr("criteria_values_arr[criteria_exploded]")) \
        .withColumn("criteria_value_lower", lower(trim(col("criteria_value")))) \
        .select(
        "acbpID", "orgID", "acbpStatus", "acbpCreatedBy", "isapar", "cbPlanName",
        "completionDueDate", "allocatedOn", "acbpCourseIDList",
        "assignmentType", "assignmentTypeInfo",
        "criteria_type", "criteria_value", "criteria_value_lower"
    )

    # Separate plans by whether they have orgID
    acbp_with_org = acbp_expanded.filter(col("orgID").isNotNull() & (col("orgID") != ""))
    acbp_without_org = acbp_expanded.filter(col("orgID").isNull() | (col("orgID") == ""))

    # Get counts
    plans_with_org_count = acbp_with_org.select("acbpID").distinct().count()
    plans_without_org_count = acbp_without_org.select("acbpID").distinct().count()
    print(f"  Plans WITH orgID: {plans_with_org_count:,}")
    print(f"  Plans WITHOUT orgID: {plans_without_org_count:,}")

    # Write expanded ACBP data
    acbp_with_org_path = f"{temp_dir}/acbp_with_org.parquet"
    acbp_without_org_path = f"{temp_dir}/acbp_without_org.parquet"

    print("  Writing expanded ACBP criteria...")
    acbp_with_org.write.mode("overwrite").parquet(acbp_with_org_path)
    if plans_without_org_count > 0:
        acbp_without_org.write.mode("overwrite").parquet(acbp_without_org_path)

    # User data path and output path
    user_data_path = ParquetFileConstants.USER_ORG_COMPUTED_FILE
    output_path = ParquetFileConstants.ACBP_COMPUTED_FILE

    print(f"\n[2/7] Initializing DuckDB...")
    print(f"  User data path: {user_data_path}")
    print(f"  Output path: {output_path}")

    # Remove existing output
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # Initialize DuckDB
    db_path = f"{temp_dir}/acbp_processing.duckdb"
    con = duckdb.connect(database=db_path)
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # Get user count
    user_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{user_data_path}/*.parquet')
    """).fetchone()[0]
    print(f"  Total users: {user_count:,}")

    print("\n[3/7] Creating user criteria matching view...")

    # Create a view with normalized user data for matching
    con.execute(f"""
        CREATE OR REPLACE VIEW users AS
        SELECT 
            userID,
            fullName,
            userPrimaryEmail,
            userMobile,
            designation,
            "group",
            userOrgID,
            ministry_name,
            dept_name,
            userOrgName,
            cadreName,
            civilServiceType,
            civilServiceName,
            cadreBatch,
            organised_service,
            userStatus,
            LOWER(TRIM(COALESCE(designation, ''))) as designation_lower,
            LOWER(TRIM(COALESCE("group", ''))) as group_lower,
            LOWER(TRIM(COALESCE(cadreName, ''))) as cadre_lower,
            LOWER(TRIM(COALESCE(civilServiceName, ''))) as service_lower,
            LOWER(TRIM(COALESCE(cadreBatch, ''))) as batch_lower
        FROM read_parquet('{user_data_path}/*.parquet')
    """)

    print("\n[4/7] Processing org-based plans with OR logic...")

    # Create view for org-based ACBP data
    con.execute(f"""
        CREATE OR REPLACE VIEW acbp_criteria AS
        SELECT * FROM read_parquet('{acbp_with_org_path}/*.parquet')
    """)

    # Explode criteria values with criteria_group_id for OR logic
    print("  Creating exploded criteria lookup with group tracking...")
    con.execute(f"""
        CREATE OR REPLACE TABLE criteria_exploded AS
        SELECT 
            a.acbpID,
            a.orgID,
            a.assignmentType,
            a.assignmentTypeInfo,
            a.criteria_type,
            LOWER(TRIM(cv.value)) as criteria_value,
            MD5(a.assignmentType || '|' || a.assignmentTypeInfo) as criteria_group_id
        FROM acbp_criteria a,
        LATERAL (SELECT unnest(string_split(a.criteria_value_lower, ',')) as value) cv
        WHERE LENGTH(TRIM(cv.value)) > 0
    """)

    # Get plan info
    print("  Creating plan info table...")
    con.execute(f"""
        CREATE OR REPLACE TABLE plan_info AS
        SELECT DISTINCT
            acbpID, orgID, isapar, assignmentType, assignmentTypeInfo, completionDueDate,
            allocatedOn, acbpCourseIDList, acbpStatus, acbpCreatedBy, cbPlanName
        FROM acbp_criteria
    """)

    # Get criteria count per criteria group (for OR logic)
    print("  Computing criteria counts per criteria group...")
    con.execute(f"""
        CREATE OR REPLACE TABLE criteria_group_count AS
        SELECT 
            acbpID, 
            criteria_group_id,
            assignmentType,
            assignmentTypeInfo,
            COUNT(DISTINCT criteria_type) as total_criteria_types
        FROM criteria_exploded
        GROUP BY acbpID, criteria_group_id, assignmentType, assignmentTypeInfo
    """)

    # Match users to criteria groups
    print("  Matching users to criteria groups...")
    con.execute(f"""
        CREATE OR REPLACE TABLE user_criteria_matches AS
        -- rootorgid
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'rootorgid' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'rootorgid'
            AND u.userOrgID = ce.criteria_value

        UNION ALL

        -- alluser
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'alluser' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'alluser'
            AND u.userOrgID = ce.orgID

        UNION ALL

        -- user/customuser
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            ce.criteria_type as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type IN ('user', 'customuser')
            AND LOWER(u.userID) = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID

        UNION ALL

        -- designation
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'designation' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'designation'
            AND u.designation_lower = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID

        UNION ALL

        -- cadre
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'cadre' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'cadre'
            AND u.cadre_lower = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID

        UNION ALL

        -- group
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'group' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'group'
            AND u.group_lower = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID

        UNION ALL

        -- batch
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'batch' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'batch'
            AND u.batch_lower = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID

        UNION ALL

        -- service
        SELECT DISTINCT 
            u.userID, 
            ce.acbpID, 
            ce.criteria_group_id,
            'service' as matched_type
        FROM users u
        INNER JOIN criteria_exploded ce 
            ON ce.criteria_type = 'service'
            AND u.service_lower = ce.criteria_value
        INNER JOIN plan_info pi ON ce.acbpID = pi.acbpID AND u.userOrgID = pi.orgID
    """)

    # Aggregate by criteria_group_id
    print("  Aggregating matches per criteria group...")
    con.execute(f"""
        CREATE OR REPLACE TABLE complete_matches AS
        SELECT 
            ucm.userID, 
            ucm.acbpID, 
            ucm.criteria_group_id,
            COUNT(DISTINCT ucm.matched_type) as matched_types
        FROM user_criteria_matches ucm
        GROUP BY ucm.userID, ucm.acbpID, ucm.criteria_group_id
    """)

    # Write org-based results
    print("  Creating org-based matched results...")
    con.execute(f"""
        COPY (
            SELECT DISTINCT
                u.userID, u.fullName, u.userPrimaryEmail, u.userMobile,
                u.designation, u."group", u.userOrgID,
                u.ministry_name, u.dept_name, u.userOrgName,
                u.cadreName, u.civilServiceType, u.civilServiceName,
                u.cadreBatch, u.organised_service, u.userStatus,
                p.isapar, p.acbpID, 
                cgc.assignmentType,
                cgc.assignmentTypeInfo,
                p.completionDueDate,
                p.allocatedOn, p.acbpCourseIDList, p.acbpStatus,
                p.acbpCreatedBy, p.cbPlanName
            FROM complete_matches cm
            INNER JOIN criteria_group_count cgc 
                ON cm.acbpID = cgc.acbpID 
                AND cm.criteria_group_id = cgc.criteria_group_id
            INNER JOIN users u ON cm.userID = u.userID
            INNER JOIN plan_info p ON cm.acbpID = p.acbpID
            WHERE cm.matched_types = cgc.total_criteria_types
        ) TO '{temp_dir}/result_org_based.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    print("\n[5/7] Processing global plans (without orgID)...")

    if plans_without_org_count > 0:
        print(f"  Found {plans_without_org_count} global plans")

        # Create view for global plans
        con.execute(f"""
            CREATE OR REPLACE VIEW global_acbp_criteria AS
            SELECT * FROM read_parquet('{acbp_without_org_path}/*.parquet')
        """)

        # Explode global criteria
        con.execute(f"""
            CREATE OR REPLACE TABLE global_criteria_exploded AS
            SELECT 
                a.acbpID,
                a.assignmentType,
                a.assignmentTypeInfo,
                a.criteria_type,
                LOWER(TRIM(cv.value)) as criteria_value,
                MD5(a.assignmentType || '|' || a.assignmentTypeInfo) as criteria_group_id
            FROM global_acbp_criteria a,
            LATERAL (SELECT unnest(string_split(a.criteria_value_lower, ',')) as value) cv
            WHERE LENGTH(TRIM(cv.value)) > 0
        """)

        # Get global criteria group counts
        con.execute(f"""
            CREATE OR REPLACE TABLE global_criteria_group_count AS
            SELECT 
                acbpID, 
                criteria_group_id,
                assignmentType,
                assignmentTypeInfo,
                COUNT(DISTINCT criteria_type) as total_criteria_types
            FROM global_criteria_exploded
            GROUP BY acbpID, criteria_group_id, assignmentType, assignmentTypeInfo
        """)

        # Get global plan info
        con.execute(f"""
            CREATE OR REPLACE TABLE global_plan_info AS
            SELECT DISTINCT
                acbpID, isapar, assignmentType, assignmentTypeInfo, completionDueDate,
                allocatedOn, acbpCourseIDList, acbpStatus, acbpCreatedBy, cbPlanName
            FROM global_acbp_criteria
        """)

        # Match users to global criteria (NO orgID filtering)
        con.execute(f"""
            CREATE OR REPLACE TABLE global_user_matches AS
            -- user/customuser
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                ce.criteria_type as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type IN ('user', 'customuser')
                AND LOWER(u.userID) = ce.criteria_value

            UNION ALL

            -- designation
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                'designation' as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type = 'designation'
                AND u.designation_lower = ce.criteria_value

            UNION ALL

            -- cadre
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                'cadre' as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type = 'cadre'
                AND u.cadre_lower = ce.criteria_value

            UNION ALL

            -- group
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                'group' as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type = 'group'
                AND u.group_lower = ce.criteria_value

            UNION ALL

            -- batch
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                'batch' as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type = 'batch'
                AND u.batch_lower = ce.criteria_value

            UNION ALL

            -- service
            SELECT DISTINCT 
                u.userID, 
                ce.acbpID, 
                ce.criteria_group_id,
                'service' as matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce 
                ON ce.criteria_type = 'service'
                AND u.service_lower = ce.criteria_value
        """)

        # Aggregate global matches
        con.execute(f"""
            CREATE OR REPLACE TABLE global_complete_matches AS
            SELECT 
                gum.userID, 
                gum.acbpID, 
                gum.criteria_group_id,
                COUNT(DISTINCT gum.matched_type) as matched_types
            FROM global_user_matches gum
            GROUP BY gum.userID, gum.acbpID, gum.criteria_group_id
        """)

        # Write global results
        con.execute(f"""
            COPY (
                SELECT DISTINCT
                    u.userID, u.fullName, u.userPrimaryEmail, u.userMobile,
                    u.designation, u."group", u.userOrgID,
                    u.ministry_name, u.dept_name, u.userOrgName,
                    u.cadreName, u.civilServiceType, u.civilServiceName,
                    u.cadreBatch, u.organised_service, u.userStatus,
                    p.isapar, p.acbpID, 
                    gcgc.assignmentType,
                    gcgc.assignmentTypeInfo,
                    p.completionDueDate,
                    p.allocatedOn, p.acbpCourseIDList, p.acbpStatus,
                    p.acbpCreatedBy, p.cbPlanName
                FROM global_complete_matches gcm
                INNER JOIN global_criteria_group_count gcgc 
                    ON gcm.acbpID = gcgc.acbpID 
                    AND gcm.criteria_group_id = gcgc.criteria_group_id
                INNER JOIN users u ON gcm.userID = u.userID
                INNER JOIN global_plan_info p ON gcm.acbpID = p.acbpID
                WHERE gcm.matched_types = gcgc.total_criteria_types
            ) TO '{temp_dir}/result_global.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        print(f"  Global plans processed")
    else:
        print("  No global plans found")

    print("\n[6/7] Combining and writing final output...")

    # Create output directory
    os.makedirs(output_path, exist_ok=True)
    output_file = f"{output_path}/part-00000.parquet"

    # Combine both result files
    if plans_without_org_count > 0:
        con.execute(f"""
            COPY (
                SELECT DISTINCT * FROM (
                    SELECT * FROM read_parquet('{temp_dir}/result_org_based.parquet')
                    UNION ALL
                    SELECT * FROM read_parquet('{temp_dir}/result_global.parquet')
                )
            ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
        """)
    else:
        con.execute(f"""
            COPY (
                SELECT DISTINCT * FROM read_parquet('{temp_dir}/result_org_based.parquet')
            ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
        """)

    # Get final count
    count_result = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_file}')
    """).fetchone()[0]

    # Get unique counts
    unique_users = con.execute(f"""
        SELECT COUNT(DISTINCT userID) FROM read_parquet('{output_file}')
    """).fetchone()[0]

    unique_plans = con.execute(f"""
        SELECT COUNT(DISTINCT acbpID) FROM read_parquet('{output_file}')
    """).fetchone()[0]

    elapsed_time = time.time() - start_time

    print("\n[7/7] Processing complete!")
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"Total time: {elapsed_time:.1f} seconds ({elapsed_time / 60:.1f} minutes)")
    print(f"Total user-plan matches: {count_result:,}")
    print(f"Unique users matched: {unique_users:,}")
    print(f"Unique plans with matches: {unique_plans:,}")
    print(f"Processing rate: {count_result / elapsed_time:,.0f} matches/second")
    print(f"Output location: {output_path}")
    print("=" * 80 + "\n")

    # Cleanup
    print("Cleaning up temporary files...")
    con.close()

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    print("DuckDB-based ACBP explosion complete!")


def cast_ntz_to_string_recursively(schema, prefix=""):
    """
    Recursively builds expressions to cast timestamp_ntz fields to string.
    """
    fields = []
    for field in schema.fields:
        full_name = f"{prefix}.{field.name}" if prefix else field.name

        if isinstance(field.dataType, TimestampNTZType):
            fields.append(col(full_name).cast("string").alias(field.name))
        elif isinstance(field.dataType, StructType):
            nested_cols = cast_ntz_to_string_recursively(field.dataType, prefix=full_name)
            fields.append(struct(*nested_cols).alias(field.name))
        elif isinstance(field.dataType, ArrayType):
            elemType = field.dataType.elementType
            if isinstance(elemType, TimestampNTZType):
                fields.append(expr(f"transform({full_name}, x -> CAST(x AS STRING))").alias(field.name))
            elif isinstance(elemType, StructType):
                # Recursively apply to each struct in the array
                nested_cols = cast_ntz_to_string_recursively(elemType, prefix="x")
                struct_expr = f"struct({', '.join([f'x.{c.name} as {c.name}' for c in elemType.fields])})"
                fields.append(expr(f"transform({full_name}, x -> {struct_expr})").alias(field.name))
            else:
                fields.append(col(full_name).alias(field.name))
        else:
            fields.append(col(full_name).alias(field.name))
    return fields


def drop_all_ntz_fields(df: DataFrame) -> DataFrame:
    df = df.drop("completionDueDate", "allocatedOn")
    return df


# Main function
def cast_ntz_to_string(df):
    new_cols = cast_ntz_to_string_recursively(df.schema)
    return df.select(*new_cols)


def print_nested_schema(df, prefix=""):
    for field in df.schema.fields:
        dt = field.dataType
        name = prefix + field.name
        if isinstance(dt, StructType):
            print_nested_schema(df.select(f"{name}.*"), prefix=name + ".")
        else:
            print(f"{name}: {dt}")