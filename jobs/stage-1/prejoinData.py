import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import time
from datetime import datetime

# Add root directory to sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Custom module imports
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil_v3
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.assessment import assessmentDFUtil
from jobs.config import get_environment_config
from jobs.default_config import create_config


def initialize_spark():
    """
    Initializes and returns a SparkSession with optimized configuration
    """
    print("Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName("DataProcessing_Pipeline") \
        .config("spark.executor.memory", "42g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    print("Spark Session initialized successfully")
    return spark


def run_stage(name: str, func, spark, config=None):
    """
    Executes a processing stage with minimal logging for performance

    Parameters:
    - name: Name of the processing stage
    - func: The processing function to execute
    - spark: The SparkSession instance
    - config: Optional config parameter for stages that need it
    """

    print(f"Stage: {name} - Starting...")
    start_time = time.time()

    try:
        # Call function with or without config based on whether it's provided
        if config is not None:
            result = func(spark, config)
        else:
            result = func(spark)

        duration = time.time() - start_time

        print(f"Stage: {name} - Complete ({duration:.2f}s)")
        return result

    except Exception as e:
        duration = time.time() - start_time
        print(f"Stage: {name} - Failed after {duration:.2f}s - Error: {str(e)}")
        raise e


def main():
    """
    Main data processing pipeline execution
    """

    print("Starting Data Processing Pipeline...")

    # Initialize Spark session
    spark = initialize_spark()
    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Track overall progress
    total_start_time = time.time()
    completed_stages = 0

    # Define processing stages - tuple format: (name, function, needs_config)
    processing_stages = [
        ("Org Hierarchy Computation", userDFUtil.preComputeOrgWithHierarchy, False),
        ("Content Ratings & Summary", contentDFUtil.preComputeRatingAndSummaryDataFrame, False),
        ("All Course/Program (ES)", contentDFUtil.preComputeAllCourseProgramESDataFrame, False),
        ("Content Master Data", contentDFUtil.preComputeContentDataFrame, False),
        ("Content Hierarchy Computation", contentDFUtil.precomputeContentHierarchyDataFrame, False),
        ("Assessment Master Data", assessmentDFUtil.precomputeAssessmentEsDataframe, False),
        ("External Content", contentDFUtil.preComputeExternalContentDataFrame, False),
        ("User Profile Computation", userDFUtil.preComputeUser, False),
        ("Enrolment Master Data", enrolmentDFUtil.preComputeEnrolment, False),
        ("External Enrolment", enrolmentDFUtil.preComputeExternalEnrolment, False),
        ("Org-User Mapping with Hierarchy", userDFUtil.preComputeOrgHierarchyWithUser, False),
        ("Enrolment Warehouse", enrolmentDFUtil.preComputeUserEnrolmentWarehouseData, False),
        ("User Warehouse", userDFUtil.preComputeUserWarehouseData, False),
        ("Course Warehouse", contentDFUtil.preComputeContentWarehouseData, False),
        ("Warehouse Parquet Files", contentDFUtil.writeWarehouseParquetFiles, True),  # Only this needs config
        ("Old Assessment Data", assessmentDFUtil.precomputeOldAssessmentDataframe, False),
        ("ACBP Enrolment Computation", acbpDFUtil_v3.preComputeACBPData, False),
    ]

    total_stages = len(processing_stages)

    try:
        for stage_name, stage_function, needs_config in processing_stages:
            if needs_config:
                run_stage(stage_name, stage_function, spark, config)
            else:
                run_stage(stage_name, stage_function, spark)

            completed_stages += 1

            # Progress update
            progress = (completed_stages / total_stages) * 100
            print(f"Progress: {completed_stages}/{total_stages} stages completed ({progress:.0f}%)")

        # Pipeline completion summary
        total_duration = time.time() - total_start_time

        print(f"""
Pipeline Execution Summary:
✅ Stages Completed: {completed_stages}/{total_stages}
⏱️  Total Duration: {total_duration / 60:.1f} minutes
🎯 Success Rate: 100%
📊 Status: All stages completed successfully
        """)

    except Exception as e:
        total_duration = time.time() - total_start_time
        print(f"""
Pipeline Execution Failed:
❌ Failed at stage: {completed_stages + 1}/{total_stages}
⏱️  Duration before failure: {total_duration / 60:.1f} minutes
🚨 Error: {str(e)}
        """)
        raise

    finally:
        print("Data Processing Pipeline finished")
        spark.stop()


if __name__ == "__main__":
    main()