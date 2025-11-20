import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql.types import StructType, TimestampNTZType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    struct, explode, sum, collect_list, col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,
    coalesce, regexp_replace, array_join
)
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants


def preComputeACBPData(spark):
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    acbp_df.printSchema()

    # CRITICAL FIX: Pre-process contextdata to wrap ALL scalar criteriaValue in arrays
    # Handles: "criteriaValue":true/false/"string"/number → "criteriaValue":[value]
    # Leaves arrays untouched: "criteriaValue":[...] stays as is
    acbp_df = acbp_df.withColumn("contextdata",
                                 regexp_replace(col("contextdata"),
                                                # Match "criteriaValue": followed by non-array value (boolean, string, or number)
                                                # (?!\\[) is negative lookahead to exclude arrays
                                                '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                                                '"criteriaValue":[$1]'
                                                )
                                 )

    acbp_select_df = (acbp_df.withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema)) \
                      .withColumn("userGroup", explode(col("context_data.accessControl.userGroups"))) \
                      .withColumn("criteria_keys",
                                  expr("transform(userGroup.userGroupCriteriaList, x -> x.criteriaKey)")) \
                      .withColumn("criteria_values", expr(
        # Now all criteriaValue are arrays (we pre-processed the JSON)
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
    """
    print("=== Starting ACBP Allocation ===")

    # Read user data and normalize case-sensitive fields
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)

    # Normalize user data for case-insensitive matching
    user_df = user_df.withColumn("designation_normalized", F.lower(F.trim(F.col("designation")))) \
        .withColumn("group_normalized", F.lower(F.trim(F.col("group")))) \
        .withColumn("cadreName_normalized", F.lower(F.trim(F.col("cadreName")))) \
        .withColumn("civilServiceName_normalized", F.lower(F.trim(F.col("civilServiceName")))) \
        .withColumn("cadreBatch_normalized", F.lower(F.trim(F.col("cadreBatch"))))

    # Final output columns
    select_columns = [
        "userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID",
        "ministry_name", "dept_name", "userOrgName", "cadreName", "civilServiceType", "civilServiceName",
        "cadreBatch", "organised_service", "userStatus", "isapar", "acbpID",
        "assignmentType", "assignmentTypeInfo", "completionDueDate", "allocatedOn", "acbpCourseIDList", "acbpStatus",
        "acbpCreatedBy", "cbPlanName"
    ]

    # Column mapping: assignmentType key -> userDF column (normalized to lowercase)
    column_mapping = {
        'rootorgid': 'userOrgID',
        'user': 'userID',
        'customuser': 'userID',
        'alluser': 'userOrgID',
        'designation': 'designation_normalized',
        'cadre': 'cadreName_normalized',
        'group': 'group_normalized',
        'batch': 'cadreBatch_normalized',
        'service': 'civilServiceName_normalized',
        'isprofileverified': 'userProfileStatus',
        'isoncentraldeputation': 'isOnCentralDeputation'
    }

    # Display mapping
    display_mapping = {
        'rootorgid': 'mdo_id',
        'user': 'user',
        'customuser': 'user',
        'alluser': 'user',
        'designation': 'designation',
        'cadre': 'cadre',
        'group': 'groups',
        'batch': 'cadre_batch',
        'service': 'civil_services',
        'isprofileverified': 'is_verified_karmayogi',
        'isoncentraldeputation': 'is_on_central_deputation'
    }

    all_results = []
    plans_without_users = []  # For plans that don't need user explosion

    acbp_data = acbp_df.collect()

    total_acbps = len(acbp_data)
    print(f"Total number of CB Plans: {total_acbps}")
    print(f"Unique CB Plans in input: {acbp_df.select('acbpID').distinct().count()}")

    processed_count = 0
    skipped_count = 0
    skipped_details = []
    no_user_match_count = 0
    empty_assignment_kept = 0

    for row in acbp_data:
        acbp_id = row['acbpID']
        acbp_status = row['acbpStatus']

        # Safe access to row fields
        row_dict = row.asDict()
        acbp_org_id = row_dict.get('orgID')
        assignment_type = row_dict.get('assignmentType', '')
        assignment_info = row_dict.get('assignmentTypeInfo', '')

        # Helper function to safely create plan data dict
        def create_plan_data():
            return {
                'acbpID': acbp_id,
                'acbpStatus': acbp_status,
                'assignmentType': assignment_type if assignment_type else '',
                'assignmentTypeInfo': assignment_info if assignment_info else '',
                'isapar': row_dict.get('isapar'),
                'completionDueDate': row_dict.get('completionDueDate'),
                'allocatedOn': row_dict.get('allocatedOn'),
                'acbpCourseIDList': row_dict.get('acbpCourseIDList'),
                'acbpCreatedBy': row_dict.get('acbpCreatedBy'),
                'cbPlanName': row_dict.get('cbPlanName')
            }

        # IMPORTANT: Handle empty assignmentInfo - keep these plans as-is without user explosion
        if not assignment_info or str(assignment_info).strip() == '':
            print(f"Plan {acbp_id} ({acbp_status}) has empty assignmentInfo - keeping as-is without user explosion")

            plan_df = spark.createDataFrame([create_plan_data()])
            plans_without_users.append(plan_df)
            empty_assignment_kept += 1
            continue

        # Parse assignment types and normalize to lowercase
        assignment_types = [at.strip().lower() for at in str(assignment_type).split('|') if at.strip()]

        if not assignment_types:
            print(f"Warning: Plan {acbp_id} has assignmentInfo but no assignmentType - keeping as-is")
            plan_df = spark.createDataFrame([create_plan_data()])
            plans_without_users.append(plan_df)
            empty_assignment_kept += 1
            continue

        # Format assignmentType for display
        display_assignment_type = '|'.join([display_mapping.get(t, t) for t in assignment_types])

        # Format assignmentTypeInfo
        if len(assignment_types) == 1 and assignment_types[0] == 'alluser':
            formatted_assignment_info = 'AllUser'
        else:
            info_parts = str(assignment_info).split('|')
            formatted_parts = []
            for part in info_parts:
                values = [v.strip() for v in part.split(',') if v.strip()]
                formatted_parts.append(', '.join(values))
            formatted_assignment_info = '|'.join(formatted_parts)

        # Now process based on assignment logic
        try:
            # Case 1: Only rootOrgId - get users from specified orgs
            if len(assignment_types) == 1 and assignment_types[0] == 'rootorgid':
                root_org_ids = [oid.strip() for oid in str(assignment_info).split(',') if oid.strip()]
                if not root_org_ids:
                    print(f"Warning: Plan {acbp_id} ({acbp_status}) has rootOrgId but no org IDs")
                    skipped_count += 1
                    skipped_details.append((acbp_id, acbp_status, "rootOrgId with no IDs"))
                    continue
                matched_users = user_df.filter(F.col('userOrgID').isin(root_org_ids))

            # Case 2: Only user or customuser - specific users
            elif len(assignment_types) == 1 and assignment_types[0] in ['user', 'customuser']:
                user_ids = [uid.strip() for uid in str(assignment_info).split(',') if uid.strip()]
                if not user_ids:
                    print(f"Warning: Plan {acbp_id} ({acbp_status}) has user type but no user IDs")
                    skipped_count += 1
                    skipped_details.append((acbp_id, acbp_status, "user type with no IDs"))
                    continue
                matched_users = user_df.filter(F.col('userID').isin(user_ids))

            # Case 3: Only alluser - all users in the ACBP's org
            elif len(assignment_types) == 1 and assignment_types[0] == 'alluser':
                if acbp_org_id:
                    matched_users = user_df.filter(F.col('userOrgID') == acbp_org_id)
                else:
                    print(f"Warning: Plan {acbp_id} ({acbp_status}) - alluser without orgID")
                    skipped_count += 1
                    skipped_details.append((acbp_id, acbp_status, "alluser without orgID"))
                    continue

            # Case 3.5: Single boolean criteria (isOnCentralDeputation, isProfileVerified)
            elif len(assignment_types) == 1 and assignment_types[0] in ['isoncentraldeputation', 'isprofileverified']:
                assign_type = assignment_types[0]

                if assign_type == 'isoncentraldeputation':
                    # If value is empty, default to True
                    if not assignment_info or str(assignment_info).strip() == '':
                        matched_users = user_df.filter(F.col('isOnCentralDeputation') == True)
                    else:
                        values = [v.strip().lower() for v in str(assignment_info).split(',') if v.strip()]
                        bool_values = [v in ['true', 'yes', '1'] for v in values]
                        if True in bool_values:
                            matched_users = user_df.filter(F.col('isOnCentralDeputation') == True)
                        else:
                            matched_users = user_df.filter(F.col('isOnCentralDeputation') == False)

                elif assign_type == 'isprofileverified':
                    if not assignment_info or str(assignment_info).strip() == '':
                        matched_users = user_df.filter(F.col('userProfileStatus') == True)
                    else:
                        values = [v.strip().lower() for v in str(assignment_info).split(',') if v.strip()]
                        bool_values = [v in ['true', 'yes', '1'] for v in values]
                        if True in bool_values:
                            matched_users = user_df.filter(F.col('userProfileStatus') == True)
                        else:
                            matched_users = user_df.filter(F.col('userProfileStatus') == False)

            # Case 3.6: Single criterion with standard column mapping
            elif len(assignment_types) == 1:
                assign_type = assignment_types[0]
                user_column = column_mapping.get(assign_type)

                if user_column is None:
                    print(
                        f"Warning: Unknown single assignment type '{assign_type}' for plan {acbp_id} ({acbp_status}) - plan will NOT match any users")
                    # Unknown type - skip this plan (will match 0 users)
                    no_user_match_count += 1
                    plan_data = create_plan_data()
                    plan_data['assignmentType'] = display_assignment_type
                    plan_data['assignmentTypeInfo'] = formatted_assignment_info
                    plan_df = spark.createDataFrame([plan_data])
                    plans_without_users.append(plan_df)
                    processed_count += 1
                    continue

                # Parse and apply filter
                values = [v.strip().lower() for v in str(assignment_info).split(',') if v.strip()]
                if values:
                    matched_users = user_df.filter(F.col(user_column).isin(values))
                else:
                    # Empty values - skip this plan
                    #print(f"Warning: Single criterion '{assign_type}' with empty values for plan {acbp_id}")
                    no_user_match_count += 1
                    plan_data = create_plan_data()
                    plan_data['assignmentType'] = display_assignment_type
                    plan_data['assignmentTypeInfo'] = formatted_assignment_info
                    plan_df = spark.createDataFrame([plan_data])
                    plans_without_users.append(plan_df)
                    processed_count += 1
                    continue

            # Case 4: Multiple assignment types (AND condition)
            else:
                # Parse pipe-separated values
                info_parts = [part.strip() for part in str(assignment_info).split('|')]

                # Check for mismatch
                if len(info_parts) != len(assignment_types):
                    #print(f"Warning: Mismatch in assignment types ({len(assignment_types)}) and info ({len(info_parts)}) for plan {acbp_id} ({acbp_status})")
                    #print(f"  Types: {assignment_types}")
                    #print(f"  Info: {info_parts}")
                    skipped_count += 1
                    skipped_details.append(
                        (acbp_id, acbp_status, f"Type/Info mismatch: {len(assignment_types)} vs {len(info_parts)}"))
                    continue

                # Determine starting point based on assignment types
                has_rootorgid = 'rootorgid' in assignment_types
                has_alluser = 'alluser' in assignment_types
                has_user_types = any(t in ['user', 'customuser'] for t in assignment_types)
                criteria_used_for_initial_set = set()

                if has_rootorgid:
                    # If rootOrgId is present, start with those orgs
                    rootorgid_idx = assignment_types.index('rootorgid')
                    root_org_ids = [oid.strip() for oid in info_parts[rootorgid_idx].split(',') if oid.strip()]
                    if not root_org_ids:
                        #print(f"Warning: Plan {acbp_id} ({acbp_status}) - rootOrgId with no org IDs in multi-criteria")
                        skipped_count += 1
                        skipped_details.append((acbp_id, acbp_status, "rootOrgId with no IDs in multi-criteria"))
                        continue
                    matched_users = user_df.filter(F.col('userOrgID').isin(root_org_ids))
                    criteria_used_for_initial_set.add('rootorgid')

                elif has_alluser:
                    # If alluser is present (without rootOrgId), use ACBP's org
                    if acbp_org_id:
                        matched_users = user_df.filter(F.col('userOrgID') == acbp_org_id)
                        criteria_used_for_initial_set.add('alluser')
                    else:
                        #print(f"Warning: Plan {acbp_id} ({acbp_status}) - alluser without orgID in multi-criteria")
                        skipped_count += 1
                        skipped_details.append((acbp_id, acbp_status, "alluser in multi-criteria without orgID"))
                        continue

                elif has_user_types:
                    # If user/customuser is present (without rootOrgId), start with those users
                    user_type_idx = next(i for i, t in enumerate(assignment_types) if t in ['user', 'customuser'])
                    user_type = assignment_types[user_type_idx]
                    user_ids = [uid.strip() for uid in info_parts[user_type_idx].split(',') if uid.strip()]
                    if not user_ids:
                        #print(f"Warning: Plan {acbp_id} ({acbp_status}) - user type with no IDs in multi-criteria")
                        skipped_count += 1
                        skipped_details.append((acbp_id, acbp_status, "user type with no IDs in multi-criteria"))
                        continue
                    matched_users = user_df.filter(F.col('userID').isin(user_ids))
                    criteria_used_for_initial_set.add('user')
                    criteria_used_for_initial_set.add('customuser')
                else:
                    # No org/user filter, start with all users
                    matched_users = user_df

                # Apply remaining filters (AND condition)
                # FIXED: Only skip criteria that were ACTUALLY used to create the initial set
                for assign_type, assign_values in zip(assignment_types, info_parts):
                    # Skip ONLY the criteria that was used to create initial set
                    if assign_type in criteria_used_for_initial_set:
                        continue

                    # Special handling for user/customuser types
                    if assign_type in ['user', 'customuser']:
                        # Apply the user filter as an AND condition
                        user_ids = [uid.strip() for uid in assign_values.split(',') if uid.strip()]
                        if user_ids:
                            matched_users = matched_users.filter(F.col('userID').isin(user_ids))
                        continue

                    # Handle alluser (shouldn't really be in multi-criteria but just in case)
                    if assign_type == 'alluser':
                        # Already handled if it was used for initial set
                        continue

                    # Special handling for isOnCentralDeputation
                    if assign_type == 'isoncentraldeputation':
                        # If value is empty, default to checking if user is on central deputation (True)
                        if not assign_values or assign_values.strip() == '':
                            matched_users = matched_users.filter(F.col('isOnCentralDeputation') == True)
                        else:
                            # Parse values (should be True/False or similar)
                            values = [v.strip().lower() for v in assign_values.split(',') if v.strip()]
                            if values:
                                # Assuming values like 'true', 'false', 'yes', 'no'
                                bool_values = [v in ['true', 'yes', '1'] for v in values]
                                if True in bool_values:
                                    matched_users = matched_users.filter(F.col('isOnCentralDeputation') == True)
                                else:
                                    matched_users = matched_users.filter(F.col('isOnCentralDeputation') == False)
                        continue

                    # Special handling for isProfileVerified (similar pattern)
                    if assign_type == 'isprofileverified':
                        if not assign_values or assign_values.strip() == '':
                            matched_users = matched_users.filter(F.col('userProfileStatus') == True)
                        else:
                            values = [v.strip().lower() for v in assign_values.split(',') if v.strip()]
                            if values:
                                bool_values = [v in ['true', 'yes', '1'] for v in values]
                                if True in bool_values:
                                    matched_users = matched_users.filter(F.col('userProfileStatus') == True)
                                else:
                                    matched_users = matched_users.filter(F.col('userProfileStatus') == False)
                        continue

                    user_column = column_mapping.get(assign_type)

                    if user_column is None:
                        #print(f"Warning: Unknown assignment type '{assign_type}' for plan {acbp_id} ({acbp_status}) - plan will NOT match any users")
                        # Unknown assignment type - this plan should NOT match any users
                        # Create an impossible condition to filter out all users
                        matched_users = matched_users.filter(F.lit(False))
                        break  # No point checking remaining criteria

                    # Parse and normalize values for case-insensitive matching
                    values = [v.strip().lower() for v in assign_values.split(',') if v.strip()]

                    if not values:
                        # Empty value for this criterion - skip it
                        continue

                    # Apply filter
                    matched_users = matched_users.filter(F.col(user_column).isin(values))

            # Check if any users matched
            user_count = matched_users.count()
            if user_count == 0:
                #print(f"Warning: Plan {acbp_id} ({acbp_status}) matched 0 users - keeping plan without users")
                no_user_match_count += 1

                # Create plan data without user fields
                plan_data = create_plan_data()
                plan_data['assignmentType'] = display_assignment_type
                plan_data['assignmentTypeInfo'] = formatted_assignment_info

                plan_df = spark.createDataFrame([plan_data])
                plans_without_users.append(plan_df)
                processed_count += 1
                continue

            # Add ACBP details to matched users
            matched_users = matched_users.withColumn('acbpID', F.lit(acbp_id))
            matched_users = matched_users.withColumn('assignmentType', F.lit(display_assignment_type))
            matched_users = matched_users.withColumn('assignmentTypeInfo', F.lit(formatted_assignment_info))

            # Add other ACBP columns
            for col_name in ['completionDueDate', 'allocatedOn', 'isapar',
                             'acbpCourseIDList', 'acbpStatus', 'acbpCreatedBy', 'cbPlanName']:
                if col_name in row_dict:
                    matched_users = matched_users.withColumn(col_name, F.lit(row_dict[col_name]))

            all_results.append(matched_users)
            processed_count += 1

        except Exception as e:
            print(f"ERROR processing plan {acbp_id} ({acbp_status}): {str(e)}")
            skipped_count += 1
            skipped_details.append((acbp_id, acbp_status, f"Exception: {str(e)}"))
            continue

    # print(f"\n=== Processing Summary ===")
    # print(f"Total plans: {total_acbps}")
    # print(f"Processed with user explosion: {processed_count - no_user_match_count}")
    # print(f"Plans with 0 users (kept as single record): {no_user_match_count}")
    # print(f"Kept without user explosion (empty assignmentInfo): {empty_assignment_kept}")
    # print(f"Skipped due to errors: {skipped_count}")

    if skipped_details:
        #print("\n=== Skipped Plans Details ===")
        live_skipped = [x for x in skipped_details if x[1] == 'Live']
        draft_skipped = [x for x in skipped_details if x[1] == 'draft']

        if live_skipped:
            #print(f"\nLIVE Plans Skipped ({len(live_skipped)}):")
            for acbp_id, status, reason in live_skipped:
                print(f"  {acbp_id}: {reason}")

        if draft_skipped:
            #print(f"\nDraft Plans Skipped ({len(draft_skipped)}):")
            for acbp_id, status, reason in draft_skipped:
                print(f"  {acbp_id}: {reason}")

    # Combine all results
    if not all_results and not plans_without_users:
        print("No matching users found for any ACBP plan")
        return spark.createDataFrame([], schema=acbp_df.schema)

    # Combine user-matched plans
    if all_results:
        final_df = all_results[0]
        for df in all_results[1:]:
            final_df = final_df.union(df)

        # Remove duplicates
        final_df = final_df.dropDuplicates(['acbpID', 'userID', 'assignmentType', 'assignmentTypeInfo'])
    else:
        final_df = None

    # Combine plans without users
    if plans_without_users:
        plans_df = plans_without_users[0]
        for df in plans_without_users[1:]:
            plans_df = plans_df.unionByName(df, allowMissingColumns=True)

        # If we have both types, union them
        if final_df is not None:
            final_df = final_df.unionByName(plans_df, allowMissingColumns=True)
        else:
            final_df = plans_df

    # Add alloted_org_id column
    final_df = final_df.withColumn(
        'alloted_org_id',
        F.when(
            F.lower(F.col('assignmentType')).contains('mdo_id'),
            F.when(
                F.col('assignmentType').contains('|'),
                F.split(F.col('assignmentTypeInfo'), '\\|').getItem(
                    F.expr("array_position(split(lower(assignmentType), '\\\\|'), 'mdo_id') - 1")
                )
            ).otherwise(
                F.col('assignmentTypeInfo')
            )
        ).otherwise(F.lit(None))
    )

    # Select required columns (only those available)
    available_columns = [col for col in select_columns if col in final_df.columns]
    if 'alloted_org_id' not in available_columns:
        available_columns.append('alloted_org_id')
    final_df = final_df.select(available_columns)

    # print(f"\n=== ACBP Allocation Complete ===")
    # print(f"Total records in output: {final_df.count():,}")
    # print(f"Unique acbpIDs in output: {final_df.select('acbpID').distinct().count()}")
    # print(f"Expected unique acbpIDs: {acbp_df.select('acbpID').distinct().count()}")

    # Show breakdown by status
    # print("\n=== Plans by Status ===")
    # final_df.groupBy("acbpStatus").agg(
    #    F.countDistinct("acbpID").alias("unique_plans"),
    #    F.count("*").alias("total_records")
    # ).show()

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