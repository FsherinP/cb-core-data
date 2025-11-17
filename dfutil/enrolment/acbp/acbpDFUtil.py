import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql.types import StructType, TimestampNTZType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode, sum, collect_list, col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,
    coalesce, regexp_replace, array_join
)
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
from functools import reduce

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants


def preComputeACBPData(spark):
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    acbp_select_df = (acbp_df.withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema)) \
                      .withColumn("userGroup", explode(col("context_data.accessControl.userGroups"))) \
                      .withColumn("criteria_keys",
                                  expr("transform(userGroup.userGroupCriteriaList, x -> x.criteriaKey)")) \
                      .withColumn("criteria_values", expr(
        "transform(userGroup.userGroupCriteriaList, x -> concat_ws(', ', x.criteriaValue))")) \
                      .withColumn("assignmentType", array_join(col("criteria_keys"), "|")) \
                      .withColumn("assignmentTypeInfo", array_join(col("criteria_values"), "|")) \
                      .withColumn(
        "userOrgID",
        expr("""
            CASE
              WHEN array_contains(criteria_keys, 'rootOrgId') THEN
                filter(
                  criteria_values,
                  (value, idx) -> criteria_keys[idx] = 'rootOrgId'
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
        # cast to string so it matches draft side
        col("enddate").cast("string").alias("completionDueDate"),
        col("publishedat").cast("string").alias("allocatedOn"),
        col("contentlist").alias("acbpCourseIDList"),
        col("assignmentType"),
        col("assignmentTypeInfo")
    )
                      .na.fill({"cbPlanName": ""})
                      )

    acbp_select_df.show(5, truncate=False)
    print(f"acbp_select_df data: {acbp_select_df.count():,} rows")
    acbp_select_df.filter(col("acbpID") == "e31e1610-84a7-11f0-9e61-91f013f42c26").show(truncate=False)

    draft_cbp_data = (acbp_select_df \
                      .filter((col("acbpStatus") == "draft") & col("draftdata").isNotNull()) \
                      .select("acbpID", "orgID", "draftdata", "acbpStatus", "acbpCreatedBy", "isapar") \
                      .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema)) \
                      .withColumn("cbPlanName", col("draftData.name")) \
                      .withColumn("assignmentType", col("draftData.assignmentType")) \
                      .withColumn("assignmentTypeInfo",
                                  array_join(col("draftData.assignmentTypeInfo"), ",")) \
                      .withColumn("completionDueDate", col("draftData.endDate").cast("string")) \
                      .withColumn("allocatedOn", lit("not published")) \
                      .withColumn("acbpCourseIDList", col("draftData.contentList")) \
                      .drop("draftData"))

    draft_cbp_data.show(5, truncate=False)
    print(f"draft_cbp_data data: {draft_cbp_data.count():,} rows")

    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "draft")

    non_draft_cbp_data.show(5, truncate=False)
    print(f"non_draft_cbp_data data: {non_draft_cbp_data.count():,} rows")

    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))

    draft_cbp_data.show(5, truncate=False)
    print(f"draft_cbp_data data after adding draftdata column: {draft_cbp_data.count():,} rows")

    final_df = non_draft_cbp_data.unionByName(draft_cbp_data)

    final_df.show(5, truncate=False)
    print(f"final_df data: {final_df.count():,} rows")
    exportDFToParquet(final_df, ParquetFileConstants.ACBP_SELECT_FILE)
    explodeAcbpData(spark, final_df)


def explodeAcbpData(spark, acbp_df: DataFrame) -> DataFrame:
    """
    Process ACBP assignments and generate final user list based on assignment types.

    Parameters:
    - spark: SparkSession
    - acbp_df: DataFrame with columns [acbpID, acbpStatus, assignmentType, assignmentTypeInfo,
                                        completionDueDate, allocatedOn, acbpCourseIDList,
                                        acbpCreatedBy, cbPlanName]

    Returns:
    - DataFrame with all matched users and their ACBP details
    """

    print("=== Starting ACBP Allocation ===")

    # Read user data
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)

    # Final output columns
    select_columns = [
        "userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID",
        "userProfileStatus",
        "ministry_name", "dept_name", "userOrgName", "cadreName", "civilServiceType", "civilServiceName", "cadreBatch",
        "organised_service", "userStatus", "isOnCentralDeputation", "isapar", "acbpID", "assignmentType",
        "assignmentTypeInfo",
        "completionDueDate", "allocatedOn", "acbpCourseIDList", "acbpStatus", "acbpCreatedBy", "cbPlanName"]

    # Column mapping: assignmentType key -> userDF column (normalized to lowercase)
    column_mapping = {
        'rootorgid': 'userOrgID',
        'user': 'userID',
        'customuser': 'userID',
        'alluser': 'userOrgID',
        'designation': 'designation',
        'cadre': 'cadreName',
        'group': 'group',
        'batch': 'cadreBatch',
        'service': 'civilServiceName',
        'isprofileverified': 'userProfileStatus',
        'isoncentraldeputation': 'isOnCentralDeputation'
    }

    all_results = []

    # Collect ACBP data for processing
    acbp_data = acbp_df.collect()

    for row in acbp_data:
        acbp_id = row['acbpID']
        assignment_type = row['assignmentType']
        assignment_info = row['assignmentTypeInfo']

        # Skip if assignment info is empty
        if not assignment_info or str(assignment_info).strip() == '':
            continue

        # Parse assignment types (pipe-separated) and normalize to lowercase
        assignment_types = [at.strip().lower() for at in str(assignment_type).split('|')]

        # Case 1: Only rootOrgId
        if len(assignment_types) == 1 and assignment_types[0] == 'rootorgid':
            root_org_id = str(assignment_info).strip()
            matched_users = user_df.filter(F.col('userOrgID') == root_org_id)

        # Case 2: Only user or customuser
        elif len(assignment_types) == 1 and assignment_types[0] in ['user', 'customuser']:
            # Parse comma-separated user IDs
            user_ids = [uid.strip() for uid in str(assignment_info).split(',')]
            matched_users = user_df.filter(F.col('userID').isin(user_ids))

        # Case 3: Only alluser
        elif len(assignment_types) == 1 and assignment_types[0] == 'alluser':
            # Get org_id from the ACBP row if available, otherwise from assignmentTypeInfo
            row_dict = row.asDict()
            org_id = row_dict.get('org_id') or row_dict.get('orgID') or str(assignment_info).strip()
            matched_users = user_df.filter(F.col('userOrgID') == org_id)

        # Case 4: Multiple assignment types (AND condition)
        else:
            # Parse pipe-separated values in assignmentTypeInfo
            info_parts = [part.strip() for part in str(assignment_info).split('|')]

            # Ensure we have matching parts for each assignment type
            if len(info_parts) != len(assignment_types):
                print(f"Warning: Mismatch in assignment types and info for acbpID {acbp_id}")
                continue

            # Start with all users
            matched_users = user_df

            # Apply each filter condition (AND logic)
            for assign_type, assign_values in zip(assignment_types, info_parts):
                # Get the corresponding column in user_df
                user_column = column_mapping.get(assign_type)

                if user_column is None:
                    print(f"Warning: Unknown assignment type '{assign_type}' for acbpID {acbp_id}")
                    continue

                # Parse comma-separated values
                values = [v.strip() for v in assign_values.split(',') if v.strip()]

                if not values:
                    continue

                # Apply filter (AND condition)
                matched_users = matched_users.filter(F.col(user_column).isin(values))

        # Add ACBP details to matched users
        matched_users = matched_users.withColumn('acbpID', F.lit(acbp_id))

        # Add other ACBP columns
        for col_name in ['assignmentType', 'assignmentTypeInfo', 'isapar', 'completionDueDate', 'allocatedOn',
                         'acbpCourseIDList', 'acbpID', 'acbpStatus', 'acbpCreatedBy', 'cbPlanName']:
            if col_name in row.asDict():
                matched_users = matched_users.withColumn(col_name, F.lit(row[col_name]))

        all_results.append(matched_users)

    # Combine all results
    if not all_results:
        print("No matching users found for any ACBP plan")
        return spark.createDataFrame([], schema=acbp_df.schema)

    final_df = all_results[0]
    for df in all_results[1:]:
        final_df = final_df.union(df)

    # Remove duplicates (same user for same acbpID)
    final_df = final_df.dropDuplicates(['acbpID', 'userID'])

    # Add alloted_org_id column
    # Extract rootOrgId value when assignmentType contains rootOrgId, otherwise null
    final_df = final_df.withColumn(
        'alloted_org_id',
        F.when(
            F.lower(F.col('assignmentType')).contains('rootorgid'),
            F.when(
                F.col('assignmentType').contains('|'),
                # If pipe-separated, extract the rootOrgId value
                F.split(F.col('assignmentTypeInfo'), '\\|').getItem(
                    F.expr("array_position(split(lower(assignmentType), '\\\\|'), 'rootorgid') - 1")
                )
            ).otherwise(
                # If only rootOrgId, entire assignmentTypeInfo is the org_id
                F.col('assignmentTypeInfo')
            )
        ).otherwise(F.lit(None))
    )

    # Select only required columns (ensure all exist)
    available_columns = [col for col in select_columns if col in final_df.columns]
    # Add alloted_org_id to selected columns if not already there
    if 'alloted_org_id' not in available_columns:
        available_columns.append('alloted_org_id')
    final_df = final_df.select(available_columns)

    print(f"=== ACBP Allocation Complete. Total records: {final_df.count()} ===")
    exportDFToParquet(final_df, ParquetFileConstants.ACBP_COMPUTED_FILE)
    print("=== ACBP Allocation Completed ===")
    return final_df


def cast_ntz_to_string_recursively(schema, prefix=""):
    """
    Recursively builds expressions to cast timestamp_ntz fields to string.
    """
    fields = []
    for field in schema.fields:
        print(f"{field.name}")
        print(f"{field.dataType}")
        full_name = f"{prefix}.{field.name}" if prefix else field.name

        if isinstance(field.dataType, TimestampNTZType):
            print("----------------------------------->")
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