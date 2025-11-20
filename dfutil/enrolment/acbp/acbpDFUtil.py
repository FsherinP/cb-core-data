import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode, col, from_json, when, expr, concat_ws, array_join, lit, lower, trim, split, array_position, size, struct
)
from pyspark.sql import functions as F
from functools import reduce
import time
from collections import defaultdict

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from pyspark.sql.types import StringType, LongType
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql.types import StructType, TimestampNTZType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode, sum, collect_list, col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,
    coalesce, regexp_replace, array_join
)



def preComputeACBPData(spark):
    """
    Pre-process ACBP data from raw parquet files.
    Uses v3 optimized org-partitioned strategy.
    """
    print("="*80)
    print("ACBP Pre-Processing - Version 3.0 (Org-Partitioned)")
    print("="*80)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    #acbp_df = spark.read.parquet("/tmp/acbp_extracted/acbp")
    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    acbp_df.printSchema()

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

    draft_cbp_data = (acbp_select_df
                      .filter((col("acbpStatus") == "draft") & col("draftdata").isNotNull())
                      .select("acbpID", "orgID", "draftdata", "acbpStatus", "acbpCreatedBy", "isapar")
                      .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema))
                      .withColumn("cbPlanName", col("draftData.name"))
                      .withColumn("assignmentType", col("draftData.assignmentType"))
                      .withColumn("assignmentTypeInfo",
                                  array_join(col("draftData.assignmentTypeInfo"), ","))
                      .withColumn("completionDueDate", col("draftData.endDate").cast("string"))
                      .withColumn("allocatedOn", lit("not published"))
                      .withColumn("acbpCourseIDList", col("draftData.contentList"))
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
    live_acbp_df = final_df.filter(col("acbpStatus") == "Live")
    print(f"Total plans: {final_df.count()}, Live Plans: {live_acbp_df.count()}")
    # Call optimized v3 explode function
    explodeAcbpData(spark, live_acbp_df)


def explodeAcbpData(spark, acbp_df: DataFrame) -> DataFrame:
    """
    OPTIMIZED VERSION 3.0: Org-Partitioned Strategy

    Key optimization: Group plans by orgId and process each org separately

    Algorithm:
    1. Separate plans into: with_orgId and without_orgId
    2. Group with_orgId plans by org
    3. For each org:
       - Filter users to just that org
       - Match against org's plans only
    4. Process without_orgId plans against all users
    5. Combine results

    Performance: 3-5 minutes for 14M users, 10K plans
    """

    print("\n" + "="*80)
    print("ACBP User Allocation - Version 3.0 (ORG-PARTITIONED STRATEGY)")
    print("="*80)

    start_time = time.time()
    # Step 1: Load and normalize user data
    print("\n[1/7] Loading user data...")
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
    # Add missing columns if they don't exist (backward compatibility)
    existing_columns = user_df.columns
    if 'isOnCentralDeputation' not in existing_columns:
        print("   Note: 'isOnCentralDeputation' column not found - adding default (False)")
        user_df = user_df.withColumn('isOnCentralDeputation', lit(False))
    if 'userProfileStatus' not in existing_columns:
        print("   Note: 'userProfileStatus' column not found - adding default (False)")
        user_df = user_df.withColumn('userProfileStatus', lit(False))

    user_df = user_df.withColumn("designation_normalized", lower(trim(col("designation")))) \
        .withColumn("group_normalized", lower(trim(col("group")))) \
        .withColumn("cadreName_normalized", lower(trim(col("cadreName")))) \
        .withColumn("civilServiceName_normalized", lower(trim(col("civilServiceName")))) \
        .withColumn("cadreBatch_normalized", lower(trim(col("cadreBatch"))))


    # CRITICAL: Cache user_df as it will be filtered multiple times
    user_df = user_df.persist()

    # Step 2: Collect and categorize plans
    print("\n[2/7] Analyzing ACBP plans...")
    acbp_data = acbp_df.collect()
    total_plans = len(acbp_data)
    print(f"   Total plans: {total_plans:,}")

    # Step 3: Separate plans by orgId presence
    print("\n[3/7] Categorizing plans by orgId...")
    plans_with_orgid, plans_without_orgid, org_to_plans = categorize_plans_by_org(acbp_data)

    print(f"   Plans WITH orgId: {len(plans_with_orgid):,}")
    print(f"   Plans WITHOUT orgId: {len(plans_without_orgid):,}")
    print(f"   Unique orgs in plans: {len(org_to_plans):,}")

    # Step 4: Process plans WITH orgId (org-by-org)
    print("\n[4/7] Processing plans WITH orgId (org-partitioned)...")
    org_results = []

    for org_idx, (org_id, org_plans) in enumerate(sorted(org_to_plans.items()), 1):
        org_start = time.time()

        print(f"\n   Org {org_idx}/{len(org_to_plans)}: {org_id}")
        print(f"      Plans in this org: {len(org_plans):,}")

        # Filter users to this org only
        org_users = user_df.filter(col('userOrgID') == org_id)
        org_user_count = org_users.count()
        print(f"      Users in this org: {org_user_count:,}")

        if org_user_count == 0:
            print(f"      No users found - skipping")
            continue

        # Build plan index for this org's plans only
        org_plan_index = build_plan_index_from_rows(org_plans)

        # Broadcast org's plan index
        org_plan_bc = spark.sparkContext.broadcast(org_plan_index)

        # Match org users against org plans
        org_matches = match_users_to_plans(org_users, org_plan_bc)

        if org_matches:
            match_count = org_matches.count()
            org_elapsed = time.time() - org_start
            print(f"      Matches found: {match_count:,} ({org_elapsed:.1f}s)")
            org_results.append(org_matches)
        else:
            print(f"      No matches")

        org_plan_bc.unpersist()

    # Step 5: Combine org results
    print("\n[5/7] Combining org-specific results...")
    if org_results:
        org_combined = org_results[0]
        for df in org_results[1:]:
            org_combined = org_combined.union(df)
        #org_combined = org_combined.dropDuplicates(['userID', 'acbpID'])
        org_matches_count = org_combined.count()
        print(f"   Total org-specific matches: {org_matches_count:,}")
    else:
        org_combined = None
        org_matches_count = 0
        print(f"   No org-specific matches")

    # Step 6: Process plans WITHOUT orgId (global plans)
    print("\n[6/7] Processing plans WITHOUT orgId (global)...")
    if plans_without_orgid:
        print(f"   Plans to process: {len(plans_without_orgid):,}")

        global_plan_index = build_plan_index_from_rows(plans_without_orgid)
        global_plan_bc = spark.sparkContext.broadcast(global_plan_index)

        global_matches = match_users_to_plans(user_df, global_plan_bc)

        if global_matches:
            #global_matches = global_matches.dropDuplicates(['userID', 'acbpID'])
            global_matches_count = global_matches.count()
            print(f"   Global matches: {global_matches_count:,}")
        else:
            global_matches = None
            global_matches_count = 0
            print(f"   No global matches")

        global_plan_bc.unpersist()
    else:
        global_matches = None
        global_matches_count = 0
        print(f"   No global plans to process")

    # Unpersist user_df
    user_df.unpersist()

    # Step 7: Combine all results
    print("\n[7/7] Finalizing results...")
    final_df = combine_results(spark, org_combined, global_matches)
    if final_df is None:
        print("   No matches found")
        return spark.createDataFrame([], schema=acbp_df.schema)

     # Cache immediately after combine
    final_df.cache()
    final_df.count()  # Force into cache (takes ~20 mins, do it once)

    metrics = final_df.agg(F.count("*").alias("total_matches"), F.countDistinct("userID").alias("unique_users"), F.countDistinct("acbpID").alias("unique_plans")).collect()[0]

    total_matches = metrics['total_matches']
    unique_users = metrics['unique_users']
    unique_plans = metrics['unique_plans']

    elapsed_time = time.time() - start_time
    print("\n" + "="*80)
    print("PROCESSING COMPLETE!")
    print("="*80)
    print(f"Total time: {elapsed_time/60:.1f} minutes")
    print(f"Total user-plan matches: {total_matches:,}")
    print(f"  - From org-specific plans: {org_matches_count:,}")
    print(f"  - From global plans: {global_matches_count:,}")
    print(f"Unique users matched: {unique_users:,}")
    print(f"Unique plans with matches: {unique_plans:,}")
    print(f"Processing rate: {total_matches/elapsed_time:,.0f} matches/second")
    print(f"Output location: {ParquetFileConstants.ACBP_COMPUTED_FILE}")
    print("="*80 + "\n")

    # Export
    exportDFToParquet(final_df, ParquetFileConstants.ACBP_COMPUTED_FILE)

    return final_df


def categorize_plans_by_org(acbp_data):
    """
    Separate plans into:
    1. Plans with orgId (can be processed per-org)
    2. Plans without orgId (must process against all users)
    3. Dictionary mapping orgId -> list of plans

    Returns: (plans_with_orgid, plans_without_orgid, org_to_plans_dict)
    """
    plans_with_orgid = []
    plans_without_orgid = []
    org_to_plans = defaultdict(list)

    for row in acbp_data:
        row_dict = row.asDict()
        assignment_type = row_dict.get('assignmentType', '')
        assignment_info = row_dict.get('assignmentTypeInfo', '')

        # Skip empty assignments
        if not assignment_type or not assignment_info or str(assignment_info).strip() == '':
            continue

        # Check if this plan has rootOrgId
        types = [t.strip().lower() for t in str(assignment_type).split('|') if t.strip()]

        if 'rootorgid' in types:
            # Extract orgId value
            infos = [i.strip() for i in str(assignment_info).split('|') if i.strip()]
            if len(types) == len(infos):
                rootorgid_idx = types.index('rootorgid')
                org_ids_str = infos[rootorgid_idx]
                # Handle multiple org IDs (comma-separated)
                org_ids = [oid.strip() for oid in org_ids_str.split(',') if oid.strip()]

                for org_id in org_ids:
                    org_to_plans[org_id].append(row)

                plans_with_orgid.append(row)
        else:
            plans_without_orgid.append(row)

    return plans_with_orgid, plans_without_orgid, dict(org_to_plans)


def build_plan_index_from_rows(plan_rows):
    """
    Build plan index from a list of plan rows.
    Same structure as v2 but for a subset of plans.
    """
    plan_index = {}

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

    for row in plan_rows:
        row_dict = row.asDict()
        acbp_id = row_dict['acbpID']
        assignment_type = row_dict.get('assignmentType', '')
        assignment_info = row_dict.get('assignmentTypeInfo', '')

        if not assignment_type or not assignment_info or str(assignment_info).strip() == '':
            continue

        types = [t.strip().lower() for t in str(assignment_type).split('|') if t.strip()]
        infos = [i.strip() for i in str(assignment_info).split('|') if i.strip()]

        if len(types) != len(infos):
            continue

        criteria = []
        for ctype, cinfo in zip(types, infos):
            values_list = [v.strip().lower() for v in cinfo.split(',') if v.strip()]
            criteria.append({
                'type': ctype,
                'values': set(values_list),
                'raw_values': values_list
            })

        display_type = '|'.join([display_mapping.get(t, t) for t in types])

        if len(types) == 1 and types[0] == 'alluser':
            display_info = 'AllUser'
        else:
            display_info = '|'.join([', '.join(c['raw_values']) for c in criteria])

        plan_index[acbp_id] = {
            'criteria': criteria,
            'display_type': display_type,
            'display_info': display_info,
            'metadata': {
                'acbpStatus': row_dict.get('acbpStatus', ''),
                'cbPlanName': row_dict.get('cbPlanName', ''),
                'completionDueDate': row_dict.get('completionDueDate', ''),
                'allocatedOn': row_dict.get('allocatedOn', ''),
                'acbpCourseIDList': row_dict.get('acbpCourseIDList', ''),
                'acbpCreatedBy': row_dict.get('acbpCreatedBy', ''),
                'isapar': row_dict.get('isapar', ''),
                'orgID': (row_dict.get('orgID') or '').strip()
            }
        }

    return plan_index


def match_users_to_plans(user_df, plan_index_bc):
    """
    Match a DataFrame of users against a broadcast plan index.
    Returns DataFrame with user-plan matches.
    """

    # Define UDF to match users against plans
    def match_user(userID, userOrgID, designation, cadre, group, batch, service,
                   isOnCentralDeputation, userProfileStatus):
        """Self-contained UDF for matching (same as v2)."""
        matches = []
        plans = plan_index_bc.value

        user_profile = {
            'userID': (userID or '').strip(),
            'userOrgID': (userOrgID or '').strip(),
            'designation': (designation or '').strip(),
            'cadre': (cadre or '').strip(),
            'group': (group or '').strip(),
            'batch': (batch or '').strip(),
            'service': (service or '').strip(),
            'isOnCentralDeputation': bool(isOnCentralDeputation) if isOnCentralDeputation is not None else False,
            'userProfileStatus': bool(userProfileStatus) if userProfileStatus is not None else False
        }

        for acbp_id, plan_data in plans.items():
            criteria_list = plan_data['criteria']
            plan_org_id = (plan_data['metadata'].get('orgID') or '').strip()

            matches_all = True

            for criterion in criteria_list:
                ctype = criterion['type']
                cvalues = criterion['values']

                if ctype == 'rootorgid':
                    if user_profile['userOrgID'] not in cvalues:
                        matches_all = False
                        break
                elif ctype in ['user', 'customuser']:
                    if user_profile['userID'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'alluser':
                    if user_profile['userOrgID'] != plan_org_id:
                        matches_all = False
                        break
                elif ctype == 'designation':
                    if user_profile['designation'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'cadre':
                    if user_profile['cadre'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'group':
                    if user_profile['group'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'batch':
                    if user_profile['batch'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'service':
                    if user_profile['service'] not in cvalues:
                        matches_all = False
                        break
                elif ctype == 'isoncentraldeputation':
                    if not cvalues or 'true' in cvalues or 'yes' in cvalues or '1' in cvalues:
                        if not user_profile['isOnCentralDeputation']:
                            matches_all = False
                            break
                    else:
                        if user_profile['isOnCentralDeputation']:
                            matches_all = False
                            break
                elif ctype == 'isprofileverified':
                    if not cvalues or 'true' in cvalues or 'yes' in cvalues or '1' in cvalues:
                        if not user_profile['userProfileStatus']:
                            matches_all = False
                            break
                    else:
                        if user_profile['userProfileStatus']:
                            matches_all = False
                            break
                else:
                    matches_all = False
                    break

            if matches_all:
                matches.append({
                    'acbpID': acbp_id,
                    'assignmentType': plan_data['display_type'],
                    'assignmentTypeInfo': plan_data['display_info'],
                    'acbpStatus': plan_data['metadata']['acbpStatus'],
                    'cbPlanName': plan_data['metadata']['cbPlanName'],
                    'completionDueDate': plan_data['metadata']['completionDueDate'],
                    'allocatedOn': plan_data['metadata']['allocatedOn'],
                    'acbpCourseIDList': plan_data['metadata']['acbpCourseIDList'],
                    'acbpCreatedBy': plan_data['metadata']['acbpCreatedBy'],
                    'isapar': plan_data['metadata']['isapar']
                })

        return matches

    # Register UDF
    match_schema = ArrayType(StructType([
        StructField("acbpID", StringType(), True),
        StructField("assignmentType", StringType(), True),
        StructField("assignmentTypeInfo", StringType(), True),
        StructField("acbpStatus", StringType(), True),
        StructField("cbPlanName", StringType(), True),
        StructField("completionDueDate", StringType(), True),
        StructField("allocatedOn", StringType(), True),
        StructField("acbpCourseIDList", StringType(), True),
        StructField("acbpCreatedBy", StringType(), True),
        StructField("isapar", StringType(), True)
    ]))

    match_udf = F.udf(match_user, match_schema)

    # Apply UDF
    users_with_matches = user_df.withColumn(
        'plan_matches',
        match_udf(
            col('userID'), col('userOrgID'),
            col('designation_normalized'), col('cadreName_normalized'),
            col('group_normalized'), col('cadreBatch_normalized'),
            col('civilServiceName_normalized'),
            col('isOnCentralDeputation'), col('userProfileStatus')
        )
    )

    # Filter and explode
    users_with_matches = users_with_matches.filter(size(col('plan_matches')) > 0)
    exploded = users_with_matches.withColumn('plan_match', explode(col('plan_matches')))

    # Select final columns
    result = exploded.select(
        col('userID'), col('fullName'), col('userPrimaryEmail'), col('userMobile'),
        col('designation'), col('group'), col('userOrgID'),
        col('ministry_name'), col('dept_name'), col('userOrgName'),
        col('cadreName'), col('civilServiceType'), col('civilServiceName'),
        col('cadreBatch'), col('organised_service'), col('userStatus'),
        col('plan_match.acbpID').alias('acbpID'),
        col('plan_match.assignmentType').alias('assignmentType'),
        col('plan_match.assignmentTypeInfo').alias('assignmentTypeInfo'),
        col('plan_match.acbpStatus').alias('acbpStatus'),
        col('plan_match.cbPlanName').alias('cbPlanName'),
        col('plan_match.completionDueDate').alias('completionDueDate'),
        col('plan_match.allocatedOn').alias('allocatedOn'),
        col('plan_match.acbpCourseIDList').alias('acbpCourseIDList'),
        col('plan_match.acbpCreatedBy').alias('acbpCreatedBy'),
        col('plan_match.isapar').alias('isapar')
    )

    return result


def combine_results(spark, org_results, global_results):
    """Combine org-specific and global results."""
    if org_results is not None and global_results is not None:
        combined = org_results.unionByName(global_results, allowMissingColumns=True)
        return combined.dropDuplicates(['userID', 'acbpID'])
    elif org_results is not None:
        return org_results
    elif global_results is not None:
        return global_results
    else:
        return None



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