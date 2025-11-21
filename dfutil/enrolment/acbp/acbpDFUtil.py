import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql.types import StructType, TimestampNTZType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode, sum, collect_list, col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,
    coalesce, regexp_replace
)
from pyspark.sql.types import LongType

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants
from pyspark.sql import functions as F


def preComputeACBPData(spark):
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)

    acbp_select_df = acbp_df \
        .withColumn("orgid", explode(col("orgidlist"))) \
        .withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema)) \
        .withColumn("userGroup", explode(col("context_data.accessControl.userGroups"))) \
        .withColumn("criteria", explode(col("userGroup.userGroupCriteriaList"))) \
        .withColumn("assignmentType",
             when(col("criteria.criteriaKey") == "rootOrgId", "AllUser")
            .when(col("criteria.criteriaKey") == "designation", "Designation")
            .when(col("criteria.criteriaKey") == "user", "CustomUser")
            .otherwise(col("criteria.criteriaKey"))
        ) \
        .select(
            col("planid").alias("acbpID"),
            col("orgid").alias("userOrgID"),
            col("draftdata"),
            col("status").alias("acbpStatus"),
            col("createdby").alias("acbpCreatedBy"),
            col("isapar"),
            col("name").alias("cbPlanName"),
            col("enddate").alias("completionDueDate"),
            col("publishedat").alias("allocatedOn"),
            col("contentlist").alias("acbpCourseIDList"),
            col("assignmentType"),
            col("criteria.criteriaValue").alias("assignmentTypeInfo")
        ) \
        .na.fill({"cbPlanName": ""})

    # Draft data
    draft_cbp_data = acbp_select_df.filter((col("acbpStatus") == "draft") & col("draftdata").isNotNull()) \
        .select("acbpID", "userOrgID", "draftdata", "acbpStatus", "acbpCreatedBy", "isapar") \
        .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema)) \
        .withColumn("cbPlanName", col("draftData.name")) \
        .withColumn("assignmentType", col("draftData.assignmentType")) \
        .withColumn("assignmentTypeInfo", col("draftData.assignmentTypeInfo")) \
        .withColumn("completionDueDate", col("draftData.endDate").cast("string")) \
        .withColumn("allocatedOn", lit("not published")) \
        .withColumn("acbpCourseIDList", col("draftData.contentList")) \
        .drop("draftData")

    # Non-draft data
    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "draft")

    # Add draftdata column with null values to non-draft data
    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))

    # Union the two
    final_df = non_draft_cbp_data.unionByName(draft_cbp_data) 

    exportDFToParquet(final_df, ParquetFileConstants.ACBP_SELECT_FILE)
    explodeAcbpData(spark, final_df)


def explodeAcbpData(spark, acbp_df):
    selectColumns = ["userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID",
                     "ministry_name", "dept_name", "userOrgName", "cadreName", "civilServiceType", "civilServiceName",
                     "cadreBatch", "organised_service", "userStatus", "isapar", "acbpID",
                     "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList", "acbpStatus",
                     "acbpCreatedBy", "cbPlanName"]
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
    
    # CustomUser
    acbp_custom_user_df = acbp_df \
        .filter(col("assignmentType") == "CustomUser") \
        .withColumn("userID", explode(col("assignmentTypeInfo"))) \
        .join(user_df, ["userID", "userOrgID"], "left")

    # Designation
    acbp_designation_df = acbp_df \
        .filter(col("assignmentType") == "Designation") \
        .withColumn("designation", explode(col("assignmentTypeInfo"))) \
        .join(user_df, ["userOrgID", "designation"], "left")
    
    # AllUser
    acbp_all_user_df = acbp_df \
        .filter(col("assignmentType") == "AllUser") \
        .join(user_df, ["userOrgID"], "left")
    
    # Union the three DataFrames
    dfs = [acbp_custom_user_df, acbp_designation_df, acbp_all_user_df]
    selected_dfs = [df.select([col(c) for c in selectColumns]) for df in dfs]

    # Equivalent to Scala's .reduce((a, b) => a.union(b))
    from functools import reduce
    acbp_allotment_df = reduce(lambda a, b: a.union(b), selected_dfs)

    print(f"acbp_allotment_df data: {acbp_allotment_df.count():,} rows")

    exportDFToParquet(acbp_allotment_df, ParquetFileConstants.ACBP_COMPUTED_FILE)


def exportDFToParquet(df, outputFile):
    #    df_cleaned = drop_all_ntz_fields(df)
    df.write.mode("overwrite").option("compression", "snappy").parquet(outputFile)
    df.unpersist(blocking=True)


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
