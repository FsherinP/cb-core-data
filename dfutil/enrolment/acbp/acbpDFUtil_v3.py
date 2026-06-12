import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants
import psutil
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


IS_ON_CENTRAL_DEPUTATION_EXPR = """
    CASE
        WHEN isOnCentralDeputation IS NULL
          OR TRIM(CAST(isOnCentralDeputation AS VARCHAR)) = ''
        THEN 'false'
        ELSE LOWER(TRIM(CAST(isOnCentralDeputation AS VARCHAR)))
    END AS is_on_central_deputation_lower
"""

STANDARD_CRITERIA_TYPES = (
    "'rootorgid','alluser','user','customuser',"
    "'designation','cadre','group','batch',"
    "'service','isoncentraldeputation'"
)


def preComputeACBPData(spark):
    print("=" * 80)
    print("ACBP Pre-Processing - Version 5.0 (DuckDB Optimized)")
    print("=" * 80)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)

    acbp_df = acbp_df.withColumn("contextdata",
                                 F.regexp_replace(col("contextdata"),
                                                  '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                                                  '"criteriaValue":[$1]'
                                                  )
                                 )

    acbp_select_df = (acbp_df
                      .withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema))
                      .withColumn("userGroup", explode(col("context_data.accessControl.userGroups")))
                      .withColumn("criteria_keys", expr("transform(userGroup.userGroupCriteriaList, x -> lower(x.criteriaKey))"))
                      # raw_criteria_values: array of first-element strings, used only for userOrgID extraction
                      # (same as original concat_ws approach for the rootorgid lookup — rootorgid always has 1 value)
                      .withColumn("raw_criteria_values", expr("transform(userGroup.userGroupCriteriaList, x -> concat_ws(', ', x.criteriaValue))"))
                      # json_criteria_values: array of JSON-serialized arrays, used for assignmentTypeInfo
                      # preserves multi-value entries like ["deputy director (research, statistics and analysis)","deputy director"]
                      # so DuckDB can unnest them without comma-splitting ambiguity
                      .withColumn("json_criteria_values", expr("transform(userGroup.userGroupCriteriaList, x -> to_json(x.criteriaValue))"))
                      .withColumn("assignmentType", array_join(col("criteria_keys"), "|"))
                      .withColumn("assignmentTypeInfo", array_join(col("json_criteria_values"), "|"))
                      .withColumn("userOrgID", expr("""
                            CASE
                              WHEN array_contains(criteria_keys, 'rootorgid') THEN
                                filter(raw_criteria_values, (value, idx) -> criteria_keys[idx] = 'rootorgid')[0]
                              ELSE NULL
                            END
                        """))
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
                      .withColumn("assignmentTypeInfo", array_join(col("draftData.assignmentTypeInfo"), ","))
                      .withColumn("completionDueDate", col("draftData.endDate").cast("string"))
                      .withColumn("allocatedOn", lit(None).cast("string"))
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

    live_acbp_df = final_df.filter(col("acbpStatus") == "Live")
    live_count = live_acbp_df.count()
    print(f"  Live plans for processing: {live_count:,} rows")
    user_extended_profile_df = spark.read.parquet("/home/analytics/pyspark/data-res/pq_files/cache_pq/userExtendedProfile")
    explodeAcbpData(spark, live_acbp_df, user_extended_profile_df=user_extended_profile_df)

def get_safe_duck_budget():
    free_gb = psutil.virtual_memory().available / (1024 ** 3)
    budget = max(8, int(free_gb * 0.60))
    print(f"  Free RAM: {free_gb:.1f}GB → DuckDB budget: {budget}GB")
    return f"{budget}GB"


def _chunked_user_join(con, pairs_table, pairs_count, users_view,
                       plan_info_table, result_path, temp_dir,
                       chunk_size=2_000_000):
    chunks = (pairs_count // chunk_size) + 1
    print(f"  Joining {pairs_count:,} pairs to users in {chunks} chunks...")

    con.execute(f"""
        CREATE OR REPLACE TABLE {pairs_table}_rn AS
        SELECT *, row_number() OVER () AS rn
        FROM {pairs_table}
    """)
    con.execute(f"DROP TABLE {pairs_table}")

    part_paths = []
    for idx in range(chunks):
        start_rn = idx * chunk_size + 1
        end_rn   = (idx + 1) * chunk_size
        part_path = f"{temp_dir}/{pairs_table}_part_{idx}.parquet"
        print(f"    Write chunk {idx + 1}/{chunks}  "
              f"free RAM: {psutil.virtual_memory().available / (1024**3):.1f}GB")

        con.execute(f"""
            COPY (
                SELECT DISTINCT
                    u.userID, u.fullName, u.userPrimaryEmail, u.userMobile,
                    u.designation, u."group", u.userOrgID,
                    u.ministry_name, u.dept_name, u.userOrgName,
                    u.cadreName, u.civilServiceType, u.civilServiceName,
                    u.cadreBatch, u.organised_service, u.userStatus,
                    p.isapar, p.acbpID,
                    qp.assignmentType,
                    -- Normalize assignmentTypeInfo: each pipe-segment is a JSON array like
                    -- ["val1","val2"] — convert to quoted "val1", "val2" per segment
                    -- keeping quotes prevents comma-splitting ambiguity on re-read
                    array_to_string(
                        list_transform(
                            string_split(qp.assignmentTypeInfo, '|'),
                            seg -> array_to_string(
                                list_transform(
                                    json_extract_string(seg, '$[*]')::VARCHAR[],
                                    v -> '"' || v || '"'
                                ),
                                ', '
                            )
                        ),
                        '|'
                    ) AS assignmentTypeInfo,
                    p.completionDueDate, p.allocatedOn, p.acbpCourseIDList,
                    p.acbpStatus, p.acbpCreatedBy, p.cbPlanName
                FROM {pairs_table}_rn qp
                INNER JOIN {users_view} u ON qp.userID = u.userID
                INNER JOIN {plan_info_table} p ON qp.acbpID = p.acbpID
                WHERE qp.rn BETWEEN {start_rn} AND {end_rn}
            ) TO '{part_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        part_paths.append(part_path)

    con.execute(f"DROP TABLE {pairs_table}_rn")

    print(f"  Merging {len(part_paths)} parts → {result_path}")
    parts_union = " UNION ALL ".join([f"SELECT * FROM read_parquet('{p}')" for p in part_paths])
    con.execute(f"""
        COPY (
            SELECT DISTINCT * FROM ({parts_union})
        ) TO '{result_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    for p in part_paths:
        if os.path.exists(p):
            os.remove(p)


def flattenCustomFields(spark, user_extended_profile_df, temp_dir):
    print("  Flattening user extended profile custom fields...")

    custom_field_schema = ArrayType(StructType([
        StructField("organisationId", StringType(), True),
        StructField("customFieldValues", ArrayType(StructType([
            StructField("customFieldId", StringType(), True),
            StructField("attributeName", StringType(), True),
            StructField("type", StringType(), True),
            StructField("values", ArrayType(StructType([
                StructField("attributeName", StringType(), True),
                StructField("value", StringType(), True),
                StructField("level", StringType(), True)
            ])), True)
        ])), True)
    ]))

    org_props_df = user_extended_profile_df.filter(
        col("contexttype") == "orgAdditionalProperties"
    )

    flattened = (
        org_props_df
        .withColumn("parsed",      from_json(col("contextdata"), custom_field_schema))
        .withColumn("org_entry",   explode(col("parsed")))
        .withColumn("custom_field", explode(col("org_entry.customFieldValues")))
        .withColumn("field_value",  explode(col("custom_field.values")))
        .select(
            col("userid"),
            col("org_entry.organisationId").alias("orgID"),
            lower(trim(
                F.regexp_replace(col("custom_field.attributeName"), "_", " ")
            )).alias("custom_field_name"),
            lower(trim(col("field_value.value"))).alias("custom_field_value")
        )
        .filter(
            col("custom_field_name").isNotNull()  & (F.length(col("custom_field_name"))  > 0) &
            col("custom_field_value").isNotNull() & (F.length(col("custom_field_value")) > 0)
        )
    )

    custom_fields_path = f"{temp_dir}/user_custom_fields.parquet"
    flattened.write.mode("overwrite").parquet(custom_fields_path)

    cf_count = flattened.count()
    print(f"  Custom field rows flattened: {cf_count:,}")
    print(f"  Written to: {custom_fields_path}")

    return custom_fields_path


def explodeAcbpData(spark, acbp_df: DataFrame, user_extended_profile_df: DataFrame = None) -> DataFrame:
    print("\n" + "=" * 80)
    print("ACBP User Allocation - Version 5.8 (Custom Fields Support)")
    print("=" * 80)

    start_time = time.time()

    project_dir = str(Path(__file__).resolve().parents[3])
    temp_dir = f"{project_dir}/temp_acbp_duckdb"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    print(f"\n[1/7] Parsing ACBP assignment types...")
    print(f"  Temp directory: {temp_dir}")

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

    acbp_with_org    = acbp_expanded.filter(col("orgID").isNotNull() & (col("orgID") != ""))
    acbp_without_org = acbp_expanded.filter(col("orgID").isNull() | (col("orgID") == ""))

    plans_with_org_count    = acbp_with_org.select("acbpID").distinct().count()
    plans_without_org_count = acbp_without_org.select("acbpID").distinct().count()
    print(f"  Plans WITH orgID:    {plans_with_org_count:,}")
    print(f"  Plans WITHOUT orgID: {plans_without_org_count:,}")

    acbp_with_org_path    = f"{temp_dir}/acbp_with_org.parquet"
    acbp_without_org_path = f"{temp_dir}/acbp_without_org.parquet"

    print("  Writing expanded ACBP criteria...")
    acbp_with_org.write.mode("overwrite").parquet(acbp_with_org_path)
    if plans_without_org_count > 0:
        acbp_without_org.write.mode("overwrite").parquet(acbp_without_org_path)

    has_custom_fields = user_extended_profile_df is not None
    custom_fields_path = None
    if has_custom_fields:
        custom_fields_path = flattenCustomFields(spark, user_extended_profile_df, temp_dir)
    else:
        print("  No user_extended_profile_df provided — skipping custom field matching")

    user_data_path = ParquetFileConstants.USER_ORG_COMPUTED_FILE
    output_path    = ParquetFileConstants.ACBP_COMPUTED_FILE

    print(f"\n[2/7] Initializing DuckDB...")

    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    db_path = f"{temp_dir}/acbp_processing.duckdb"
    con = duckdb.connect(database=db_path)
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute(f"SET memory_limit='{get_safe_duck_budget()}'")
    con.execute("SET threads=8")
    con.execute("SET preserve_insertion_order=false")

    print("\n[3/7] Creating supporting tables...")

    con.execute(f"""
        CREATE OR REPLACE VIEW acbp_criteria AS
        SELECT * FROM read_parquet('{acbp_with_org_path}/*.parquet')
    """)

    # FIX: criteria_value_lower is now a JSON array string like
    # '["deputy director (research, statistics and analysis)","deputy director"]'
    # Use json_extract to unnest each element safely — no comma-splitting ambiguity.
    con.execute("""
        CREATE OR REPLACE TABLE criteria_exploded AS
        SELECT
            a.acbpID, a.orgID, a.assignmentType, a.assignmentTypeInfo,
            a.criteria_type,
            LOWER(TRIM(cv.value)) AS criteria_value,
            MD5(a.assignmentType || '|' || a.assignmentTypeInfo) AS criteria_group_id
        FROM acbp_criteria a,
        LATERAL (
            SELECT unnest(
                json_extract_string(a.criteria_value_lower, '$[*]')::VARCHAR[]
            ) AS value
        ) cv
        WHERE LENGTH(TRIM(cv.value)) > 0
    """)

    con.execute("""
        CREATE OR REPLACE TABLE plan_info AS
        SELECT DISTINCT
            acbpID, orgID, isapar, assignmentType, assignmentTypeInfo,
            completionDueDate, allocatedOn, acbpCourseIDList,
            acbpStatus, acbpCreatedBy, cbPlanName
        FROM acbp_criteria
    """)

    con.execute("""
        CREATE OR REPLACE TABLE criteria_group_count AS
        SELECT
            acbpID, criteria_group_id, assignmentType, assignmentTypeInfo,
            COUNT(DISTINCT criteria_type) AS total_criteria_types
        FROM criteria_exploded
        GROUP BY acbpID, criteria_group_id, assignmentType, assignmentTypeInfo
    """)

    con.execute("""
        CREATE OR REPLACE TABLE rootorgid_lookup AS
        SELECT DISTINCT acbpID, criteria_group_id, criteria_value AS org_value
        FROM criteria_exploded
        WHERE criteria_type = 'rootorgid'
    """)
    print("  rootorgid_lookup created")

    if has_custom_fields:
        con.execute(f"""
            CREATE OR REPLACE VIEW user_custom_fields AS
            SELECT * FROM read_parquet('{custom_fields_path}/*.parquet')
        """)
        print("  user_custom_fields view created")

    print("\n[4/7] Creating stable user partition...")

    partition_path = f"{temp_dir}/user_partitions.parquet"
    con.execute(f"""
        COPY (
            SELECT
                userID, fullName, userPrimaryEmail, userMobile,
                designation, "group", userOrgID,
                ministry_name, dept_name, userOrgName,
                cadreName, civilServiceType, civilServiceName,
                cadreBatch, organised_service, userStatus,
                LOWER(TRIM(COALESCE(designation, '')))              AS designation_lower,
                LOWER(TRIM(COALESCE("group", '')))                  AS group_lower,
                LOWER(TRIM(COALESCE(cadreName, '')))                AS cadre_lower,
                LOWER(TRIM(COALESCE(civilServiceName, '')))         AS service_lower,
                LOWER(TRIM(COALESCE(cadreBatch, '')))               AS batch_lower,
                {IS_ON_CENTRAL_DEPUTATION_EXPR},
                row_number() OVER () AS rn
            FROM read_parquet('{user_data_path}/*.parquet')
        ) TO '{partition_path}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 500000)
    """)

    user_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{partition_path}')").fetchone()[0]
    print(f"  Total users: {user_count:,}")

    CHUNK_SIZE = 2_000_000
    num_chunks = (user_count // CHUNK_SIZE) + 1
    print(f"  Processing in {num_chunks} chunks of {CHUNK_SIZE:,} users...")

    print("\n[5/7] Matching users to criteria (chunked)...")

    con.execute("""
        CREATE OR REPLACE TABLE user_criteria_matches
        (userID VARCHAR, acbpID VARCHAR, criteria_group_id VARCHAR, matched_type VARCHAR)
    """)

    for chunk_idx in range(num_chunks):
        start_rn = chunk_idx * CHUNK_SIZE + 1
        end_rn   = (chunk_idx + 1) * CHUNK_SIZE
        print(f"\n  [Chunk {chunk_idx + 1}/{num_chunks}] rn {start_rn:,} → {end_rn:,}  "
              f"free RAM: {psutil.virtual_memory().available / (1024**3):.1f}GB")

        con.execute(f"""
            CREATE OR REPLACE VIEW users_chunk AS
            SELECT * FROM read_parquet('{partition_path}')
            WHERE rn BETWEEN {start_rn} AND {end_rn}
        """)

        # rootorgid
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'rootorgid'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'rootorgid'
                                            AND u.userOrgID = ce.criteria_value
                    """)

        # alluser
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'alluser'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'alluser'
                                            AND u.userOrgID = ce.orgID
                    """)

        # user / customuser
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, ce.criteria_type
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type IN ('user', 'customuser')
                                            AND LOWER(u.userID) = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # designation
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'designation'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'designation'
                                            AND u.designation_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # cadre
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'cadre'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'cadre'
                                            AND u.cadre_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # group
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'group'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'group'
                                            AND u.group_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # batch
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'batch'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'batch'
                                            AND u.batch_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # service
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'service'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'service'
                                            AND u.service_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        # isoncentraldeputation
        con.execute("""
                    INSERT INTO user_criteria_matches
                    SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'isoncentraldeputation'
                    FROM users_chunk u
                             INNER JOIN criteria_exploded ce
                                        ON ce.criteria_type = 'isoncentraldeputation'
                                            AND u.is_on_central_deputation_lower = ce.criteria_value
                             LEFT JOIN rootorgid_lookup ro
                                       ON ro.acbpID            = ce.acbpID
                                           AND ro.criteria_group_id = ce.criteria_group_id
                    WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
                    """)

        if has_custom_fields:
            con.execute(f"""
                INSERT INTO user_criteria_matches
                SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, ce.criteria_type
                FROM users_chunk u
                INNER JOIN user_custom_fields ucf
                    ON LOWER(u.userID) = LOWER(ucf.userid)
                    AND u.userOrgID    = ucf.orgID
                INNER JOIN criteria_exploded ce
                    ON ce.criteria_type   = ucf.custom_field_name
                    AND ce.criteria_value = ucf.custom_field_value
                    AND ce.criteria_type NOT IN ({STANDARD_CRITERIA_TYPES})
                LEFT JOIN rootorgid_lookup ro
                    ON ro.acbpID             = ce.acbpID
                    AND ro.criteria_group_id  = ce.criteria_group_id
                WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
            """)

        if chunk_idx < num_chunks - 1:
            checkpoint_path = f"{temp_dir}/matches_checkpoint.parquet"
            con.execute(f"""
                COPY user_criteria_matches
                TO '{checkpoint_path}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
            con.execute("DROP TABLE user_criteria_matches")
            con.execute(f"""
                CREATE TABLE user_criteria_matches AS
                SELECT * FROM read_parquet('{checkpoint_path}')
            """)
            print(f"  Checkpoint written. "
                  f"Matches so far: {con.execute('SELECT COUNT(*) FROM user_criteria_matches').fetchone()[0]:,}")

    total_raw_matches = con.execute("SELECT COUNT(*) FROM user_criteria_matches").fetchone()[0]
    print(f"\n  All chunks complete. Total raw matches: {total_raw_matches:,}")

    print("  Aggregating matches per criteria group...")
    con.execute("""
        CREATE OR REPLACE TABLE complete_matches AS
        SELECT
            userID, acbpID, criteria_group_id,
            COUNT(DISTINCT matched_type) AS matched_types
        FROM user_criteria_matches
        GROUP BY userID, acbpID, criteria_group_id
    """)

    con.execute("DROP TABLE user_criteria_matches")

    con.execute(f"""
        CREATE OR REPLACE VIEW users AS
        SELECT
            userID, fullName, userPrimaryEmail, userMobile,
            designation, "group", userOrgID,
            ministry_name, dept_name, userOrgName,
            cadreName, civilServiceType, civilServiceName,
            cadreBatch, organised_service, userStatus,
            LOWER(TRIM(COALESCE(designation, '')))              AS designation_lower,
            LOWER(TRIM(COALESCE("group", '')))                  AS group_lower,
            LOWER(TRIM(COALESCE(cadreName, '')))                AS cadre_lower,
            LOWER(TRIM(COALESCE(civilServiceName, '')))         AS service_lower,
            LOWER(TRIM(COALESCE(cadreBatch, '')))               AS batch_lower,
            {IS_ON_CENTRAL_DEPUTATION_EXPR}
        FROM read_parquet('{user_data_path}/*.parquet')
    """)

    print("  Applying OR logic filter...")
    con.execute("""
        CREATE OR REPLACE TABLE qualified_pairs AS
        SELECT
            cm.userID, cm.acbpID, cm.criteria_group_id,
            cgc.assignmentType, cgc.assignmentTypeInfo
        FROM complete_matches cm
        INNER JOIN criteria_group_count cgc
            ON cm.acbpID = cgc.acbpID
            AND cm.criteria_group_id = cgc.criteria_group_id
        WHERE cm.matched_types = cgc.total_criteria_types
    """)

    qp_count = con.execute("SELECT COUNT(*) FROM qualified_pairs").fetchone()[0]
    print(f"  Qualified pairs: {qp_count:,}")

    con.execute("DROP TABLE complete_matches")

    print("\n  Writing org-based results (chunked user join)...")
    _chunked_user_join(
        con=con,
        pairs_table="qualified_pairs",
        pairs_count=qp_count,
        users_view="users",
        plan_info_table="plan_info",
        result_path=f"{temp_dir}/result_org_based.parquet",
        temp_dir=temp_dir
    )

    print("\n[6/7] Processing global plans (without orgID)...")

    if plans_without_org_count > 0:
        print(f"  Found {plans_without_org_count} global plans")

        con.execute(f"""
            CREATE OR REPLACE VIEW global_acbp_criteria AS
            SELECT * FROM read_parquet('{acbp_without_org_path}/*.parquet')
        """)

        # FIX: same json_extract unnesting as org-based path above
        con.execute("""
            CREATE OR REPLACE TABLE global_criteria_exploded AS
            SELECT
                a.acbpID, a.assignmentType, a.assignmentTypeInfo, a.criteria_type,
                LOWER(TRIM(cv.value)) AS criteria_value,
                MD5(a.assignmentType || '|' || a.assignmentTypeInfo) AS criteria_group_id
            FROM global_acbp_criteria a,
            LATERAL (
                SELECT unnest(
                    json_extract_string(a.criteria_value_lower, '$[*]')::VARCHAR[]
                ) AS value
            ) cv
            WHERE LENGTH(TRIM(cv.value)) > 0
        """)

        con.execute("""
            CREATE OR REPLACE TABLE global_criteria_group_count AS
            SELECT acbpID, criteria_group_id, assignmentType, assignmentTypeInfo,
                   COUNT(DISTINCT criteria_type) AS total_criteria_types
            FROM global_criteria_exploded
            GROUP BY acbpID, criteria_group_id, assignmentType, assignmentTypeInfo
        """)

        con.execute("""
            CREATE OR REPLACE TABLE global_plan_info AS
            SELECT DISTINCT acbpID, isapar, assignmentType, assignmentTypeInfo,
                            completionDueDate, allocatedOn, acbpCourseIDList,
                            acbpStatus, acbpCreatedBy, cbPlanName
            FROM global_acbp_criteria
        """)

        con.execute("""
            CREATE OR REPLACE TABLE global_rootorgid_lookup AS
            SELECT DISTINCT acbpID, criteria_group_id, criteria_value AS org_value
            FROM global_criteria_exploded
            WHERE criteria_type = 'rootorgid'
        """)
        print("  global_rootorgid_lookup created")

        global_custom_fields_union = ""
        if has_custom_fields:
            global_custom_fields_union = f"""
            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, ce.criteria_type
            FROM users u
            INNER JOIN user_custom_fields ucf
                ON LOWER(u.userID) = LOWER(ucf.userid)
                AND u.userOrgID    = ucf.orgID
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type   = ucf.custom_field_name
                AND ce.criteria_value = ucf.custom_field_value
                AND ce.criteria_type NOT IN ({STANDARD_CRITERIA_TYPES})
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID             = ce.acbpID
                AND ro.criteria_group_id  = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value
            """

        con.execute(f"""
            CREATE OR REPLACE TABLE global_user_matches AS
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, ce.criteria_type AS matched_type
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type IN ('user', 'customuser')
                AND LOWER(u.userID) = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'designation'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'designation'
                AND u.designation_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'cadre'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'cadre'
                AND u.cadre_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'group'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'group'
                AND u.group_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'batch'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'batch'
                AND u.batch_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'service'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'service'
                AND u.service_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            UNION ALL
            SELECT DISTINCT u.userID, ce.acbpID, ce.criteria_group_id, 'isoncentraldeputation'
            FROM users u
            INNER JOIN global_criteria_exploded ce
                ON ce.criteria_type = 'isoncentraldeputation'
                AND u.is_on_central_deputation_lower = ce.criteria_value
            LEFT JOIN global_rootorgid_lookup ro
                ON ro.acbpID            = ce.acbpID
                AND ro.criteria_group_id = ce.criteria_group_id
            WHERE ro.org_value IS NULL OR u.userOrgID = ro.org_value

            {global_custom_fields_union}
        """)

        con.execute("""
            CREATE OR REPLACE TABLE global_complete_matches AS
            SELECT userID, acbpID, criteria_group_id,
                   COUNT(DISTINCT matched_type) AS matched_types
            FROM global_user_matches
            GROUP BY userID, acbpID, criteria_group_id
        """)

        con.execute("DROP TABLE global_user_matches")

        con.execute("""
            CREATE OR REPLACE TABLE global_qualified_pairs AS
            SELECT
                gcm.userID, gcm.acbpID, gcm.criteria_group_id,
                gcgc.assignmentType, gcgc.assignmentTypeInfo
            FROM global_complete_matches gcm
            INNER JOIN global_criteria_group_count gcgc
                ON gcm.acbpID = gcgc.acbpID
                AND gcm.criteria_group_id = gcgc.criteria_group_id
            WHERE gcm.matched_types = gcgc.total_criteria_types
        """)

        g_qp_count = con.execute("SELECT COUNT(*) FROM global_qualified_pairs").fetchone()[0]
        print(f"  Global qualified pairs: {g_qp_count:,}")

        con.execute("DROP TABLE global_complete_matches")

        print("  Writing global results (chunked user join)...")
        _chunked_user_join(
            con=con,
            pairs_table="global_qualified_pairs",
            pairs_count=g_qp_count,
            users_view="users",
            plan_info_table="global_plan_info",
            result_path=f"{temp_dir}/result_global.parquet",
            temp_dir=temp_dir
        )

        print("  Global plans processed")
    else:
        print("  No global plans found")

    print("\n[7/7] Combining and writing final output...")

    os.makedirs(output_path, exist_ok=True)
    output_file = f"{output_path}/part-00000.parquet"

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

    count_result = con.execute(f"SELECT COUNT(*)               FROM read_parquet('{output_file}')").fetchone()[0]
    unique_users = con.execute(f"SELECT COUNT(DISTINCT userID) FROM read_parquet('{output_file}')").fetchone()[0]
    unique_plans = con.execute(f"SELECT COUNT(DISTINCT acbpID) FROM read_parquet('{output_file}')").fetchone()[0]

    elapsed_time = time.time() - start_time

    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"Total time:              {elapsed_time:.1f}s ({elapsed_time / 60:.1f} min)")
    print(f"Total user-plan matches: {count_result:,}")
    print(f"Unique users matched:    {unique_users:,}")
    print(f"Unique plans matched:    {unique_plans:,}")
    print(f"Processing rate:         {count_result / elapsed_time:,.0f} matches/second")
    print(f"Output location:         {output_path}")
    print("=" * 80 + "\n")

    print("Cleaning up temporary files...")
    con.close()

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    print("DuckDB-based ACBP explosion complete!")


def cast_ntz_to_string_recursively(schema, prefix=""):
    fields = []
    for field in schema.schema.fields:
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
                nested_cols = cast_ntz_to_string_recursively(elemType, prefix="x")
                struct_expr = f"struct({', '.join([f'x.{c.name} as {c.name}' for c in elemType.fields])})"
                fields.append(expr(f"transform({full_name}, x -> {struct_expr})").alias(field.name))
            else:
                fields.append(col(full_name).alias(field.name))
        else:
            fields.append(col(full_name).alias(field.name))
    return fields


def drop_all_ntz_fields(df: DataFrame) -> DataFrame:
    return df.drop("completionDueDate", "allocatedOn")


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
