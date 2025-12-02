import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants

import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    struct, explode, col, from_json, when, expr, concat_ws, array_join, lit, lower, trim, split, array_position, size
)
from pyspark.sql import functions as F
from functools import reduce
import time
from collections import defaultdict


def preComputeACBPData(spark):
    """
    Pre-process ACBP data from raw parquet files.
    Uses v4 optimized org-partitioned strategy with batched broadcasts.
    """
    print("="*80)
    print("ACBP Pre-Processing - Version 4.0 (Batched Org-Partitioned)")
    print("="*80)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

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

    
    print(f"draft_cbp_data data: {draft_cbp_data.count():,} rows")

    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "draft")
    
    print(f"non_draft_cbp_data data: {non_draft_cbp_data.count():,} rows")

    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))
    
    print(f"draft_cbp_data data after adding draftdata column: {draft_cbp_data.count():,} rows")

    final_df = non_draft_cbp_data.unionByName(draft_cbp_data)
    
    print(f"final_df data: {final_df.count():,} rows")

    exportDFToParquet(final_df, ParquetFileConstants.ACBP_SELECT_FILE)
    
    live_acbp_df = final_df.filter(col("acbpStatus") == "Live")
    live_count = live_acbp_df.count()
    total_count = final_df.count()
    print(f"Total plans: {total_count}, Live Plans: {live_count}")
    
    # Call optimized v4 explode function
    explodeAcbpData(spark, live_acbp_df)


def explodeAcbpData(spark, acbp_df: DataFrame) -> DataFrame:
    """
    OPTIMIZED VERSION 4.0: Batched Org-Partitioned Strategy with OR Logic Built-In

    Key optimization:
    - Group plans by org (org-partitioning)
    - Build ONE index per org with multiple criteria groups per plan (OR logic)
    - One broadcast per org instead of one per plan row
    - Performance: 2-3 minutes for 14M users, 9500 plans

    QUICK WIN OPTIMIZATIONS APPLIED:
    - Increased shuffle partitions to 400 for better parallelism
    - Pre-filtered inactive users before persist
    - Removed ALL count() operations except final aggregation (massive speedup)
    """

    print("\n" + "="*80)
    print("ACBP User Allocation - Version 4.0 (QUICK WIN OPTIMIZED)")
    print("="*80)

    start_time = time.time()

    # QUICK WIN 1: Tune Spark parallelism settings
    print("\n[0/7] Applying Spark optimizations...")
    original_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    original_parallelism = spark.conf.get("spark.default.parallelism", "200")

    spark.conf.set("spark.sql.shuffle.partitions", "400")
    spark.conf.set("spark.default.parallelism", "400")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    print(f"   Shuffle partitions: {original_shuffle_partitions} → 400")
    print(f"   Default parallelism: {original_parallelism} → 400")
    print(f"   Adaptive query execution: enabled")

    # Step 1: Load and normalize user data
    print("\n[1/7] Loading user data...")
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)

    existing_columns = set(user_df.columns)
    for col_name, default_val in [('isOnCentralDeputation', False), ('userProfileStatus', False)]:
        if col_name not in existing_columns:
            print(f"   Note: '{col_name}' column not found - adding default ({default_val})")
            user_df = user_df.withColumn(col_name, lit(default_val))

    # QUICK WIN 2: Pre-filter users before normalization and persist
    print("   Pre-filtering users (active users with valid orgID)...")
    user_df = user_df.filter(
        (col('userOrgID').isNotNull()) &
        (col('userOrgID') != '') &
        (col('userStatus').isNull() | (col('userStatus') == 'active') | (col('userStatus') == ''))
    )

    user_df = user_df \
        .withColumn("designation_normalized", lower(trim(col("designation")))) \
        .withColumn("group_normalized", lower(trim(col("group")))) \
        .withColumn("cadreName_normalized", lower(trim(col("cadreName")))) \
        .withColumn("civilServiceName_normalized", lower(trim(col("civilServiceName")))) \
        .withColumn("cadreBatch_normalized", lower(trim(col("cadreBatch"))))

    user_df = user_df.persist()

    # QUICK WIN 3: Removed count() here - deferred to final aggregation
    print(f"   Users loaded and cached")

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

    # Step 4: Process plans WITH orgId (org-partitioned, batched)
    print("\n[4/7] Processing plans WITH orgId (org-partitioned, BATCHED)...")
    org_results = []

    for org_idx, (org_id, org_plans) in enumerate(sorted(org_to_plans.items()), 1):
        org_start = time.time()

        print(f"\n   Org {org_idx}/{len(org_to_plans)}: {org_id}")
        print(f"      Plans in this org: {len(org_plans):,}")

        org_users = user_df.filter(col('userOrgID') == org_id)

        # QUICK WIN 3: Removed count() - no longer checking if org has users
        # Will rely on UDF to return empty results if no users match

        # ✅ KEY CHANGE: Build ONE index for ALL plans in this org (with OR logic built-in)
        org_plan_index = build_plan_index_from_rows(org_plans)
        org_plan_bc = spark.sparkContext.broadcast(org_plan_index)

        # Single UDF call for all org plans
        org_matches = match_users_to_plans(org_users, org_plan_bc)

        if org_matches:
            org_elapsed = time.time() - org_start
            # QUICK WIN 3: Removed count() call here
            print(f"      Processed in {org_elapsed:.1f}s")
            org_results.append(org_matches)
        else:
            print(f"      No matches")

        org_plan_bc.unpersist()

    # Step 5: Combine org results
    print("\n[5/7] Combining org-specific results...")
    if org_results:
        org_combined = reduce(lambda df1, df2: df1.union(df2), org_results)
        # QUICK WIN 3: Removed count() call here
        print(f"   Org-specific results combined")
    else:
        org_combined = None
        print(f"   No org-specific matches")

    # Step 6: Process plans WITHOUT orgId (global plans, BATCHED)
    print("\n[6/7] Processing plans WITHOUT orgId (global, BATCHED)...")
    global_matches = None

    if plans_without_orgid:
        print(f"   Plans to process: {len(plans_without_orgid):,}")

        # ✅ KEY CHANGE: Build ONE index for ALL global plans (with OR logic built-in)
        global_plan_index = build_plan_index_from_rows(plans_without_orgid)
        global_plan_bc = spark.sparkContext.broadcast(global_plan_index)

        # Single UDF call for all global plans
        global_matches = match_users_to_plans(user_df, global_plan_bc)

        if global_matches:
            # QUICK WIN 3: Removed count() call here
            print(f"   Global plans processed")
        else:
            print(f"   No global matches")

        global_plan_bc.unpersist()
    else:
        print(f"   No global plans to process")

    # Unpersist user_df
    user_df.unpersist()

    # Step 7: Combine all results
    print("\n[7/7] Finalizing results...")
    final_df = combine_results(spark, org_combined, global_matches)
    if final_df is None:
        print("   No matches found")
        return spark.createDataFrame([], schema=acbp_df.schema)

    # Cache + metrics in one pass (ONLY count operation in entire function)
    final_df.cache()
    metrics = final_df.agg(
        F.count("*").alias("total_matches"),
        F.countDistinct("userID").alias("unique_users"),
        F.countDistinct("acbpID").alias("unique_plans")
    ).collect()[0]

    total_matches = metrics['total_matches']
    unique_users = metrics['unique_users']
    unique_plans = metrics['unique_plans']

    elapsed_time = time.time() - start_time
    print("\n" + "="*80)
    print("PROCESSING COMPLETE!")
    print("="*80)
    print(f"Total time: {elapsed_time/60:.1f} minutes")
    print(f"Total user-plan matches: {total_matches:,}")
    print(f"Unique users matched: {unique_users:,}")
    print(f"Unique plans with matches: {unique_plans:,}")
    print(f"Processing rate: {total_matches/elapsed_time:,.0f} matches/second")
    print(f"Output location: {ParquetFileConstants.ACBP_COMPUTED_FILE}")
    print("="*80 + "\n")

    # Export
    exportDFToParquet(final_df, ParquetFileConstants.ACBP_COMPUTED_FILE)

    final_df.unpersist()

    return final_df


def categorize_plans_by_org(acbp_data):
    """
    Separate plans into:
    1. Plans with orgId (can be processed per-org)
    2. Plans without orgId (must process against all users)
    3. Dictionary mapping orgId -> list of plans
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
    
    ✅ KEY CHANGE: Stores multiple criteria groups per acbpID for OR logic
    Structure: {
        acbpID: [
            {criteria, display_type, display_info, metadata},  # Criteria group 1
            {criteria, display_type, display_info, metadata}   # Criteria group 2 (OR)
        ]
    }
    """
    plan_index = defaultdict(list)

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

        # ✅ KEY CHANGE: Append to list (OR logic between rows)
        plan_index[acbp_id].append({
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
        })

    return dict(plan_index)


def match_users_to_plans(user_df, plan_index_bc):
    """
    Match a DataFrame of users against a broadcast plan index.
    
    ✅ KEY CHANGE: Implements OR logic between criteria groups for same acbpID
    """

    def match_user(userID, userOrgID, designation, cadre, group, batch, service,
                   isOnCentralDeputation, userProfileStatus):
        """Self-contained UDF for matching with OR logic."""
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

        for acbp_id, criteria_groups in plans.items():
            # ✅ KEY CHANGE: Loop through all criteria groups for this plan (OR logic)
            matches_any_group = False
            matched_display_type = None
            matched_display_info = None
            matched_metadata = None
            
            for criteria_group in criteria_groups:
                criteria_list = criteria_group['criteria']
                plan_org_id = (criteria_group['metadata'].get('orgID') or '').strip()
                
                # AND logic within this criteria group
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
                    # ✅ This criteria group matched, store and break (OR logic)
                    matches_any_group = True
                    matched_display_type = criteria_group['display_type']
                    matched_display_info = criteria_group['display_info']
                    matched_metadata = criteria_group['metadata']
                    break  # Stop checking other groups for this plan

            # Only add if ANY criteria group matched
            if matches_any_group:
                matches.append({
                    'acbpID': acbp_id,
                    'assignmentType': matched_display_type,
                    'assignmentTypeInfo': matched_display_info,
                    'acbpStatus': matched_metadata['acbpStatus'],
                    'cbPlanName': matched_metadata['cbPlanName'],
                    'completionDueDate': matched_metadata['completionDueDate'],
                    'allocatedOn': matched_metadata['allocatedOn'],
                    'acbpCourseIDList': matched_metadata['acbpCourseIDList'],
                    'acbpCreatedBy': matched_metadata['acbpCreatedBy'],
                    'isapar': matched_metadata['isapar']
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