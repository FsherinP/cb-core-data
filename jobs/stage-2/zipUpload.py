import findspark

findspark.init()

import time
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import (col, lower, when, lit, expr, concat_ws, explode_outer, from_json, to_date,
                                   current_timestamp, date_format, round, coalesce, broadcast, size, map_keys,
                                   map_values)
from zipfile import ZipFile, ZIP_DEFLATED
import shutil
import subprocess
import sys
import glob
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils.utils import sync_reports
from jobs.config import get_environment_config
from jobs.default_config import create_config


class ZipUploadModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ZipUpload"

    def name(self):
        return "ZipUploadModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    def upload_parquet_files(self, config):
        try:
            print("Starting upload of Parquet files to GCP bucket")

            base_path = config.unifiedParquetLocalPath
            user_details_file = os.path.join(base_path, "unified_user_details.parquet")
            enrolments_file = os.path.join(base_path, "unified_enrolments.parquet")
            org_hierarchy_file = os.path.join(base_path, "org_hierarchy.parquet")

            # Check existence
            user_details_exists = os.path.isfile(user_details_file)
            enrolments_exists = os.path.isfile(enrolments_file)
            org_hierarchy_exists = os.path.isfile(org_hierarchy_file)

            if not user_details_exists:
                print(f"WARNING: File not found: {user_details_file}")
            if not enrolments_exists:
                print(f"WARNING: File not found: {enrolments_file}")
            if not org_hierarchy_exists:
                print(f"WARNING: File not found: {org_hierarchy_file}")

            # Proceed only if all exist
            if user_details_exists and enrolments_exists and org_hierarchy_exists:
                sync_reports(base_path, config.unifiedParquetPath, config)
                print("Completed uploading Parquet files to GCP bucket.")
            else:
                print("Upload skipped: One or more required files are missing.")

        except Exception as e:
            print(f"Error uploading Parquet files: {str(e)}")
            raise

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            today = self.get_date()
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

            # ------------------ Part 1: Merge & Zip MDOID Reports ------------------ #
            base_dir = os.path.join(config.localReportDir, config.prefixDirectoryPath)
            directories_to_select = config.pysparkDirectoriesToSelect
            today_date = datetime.today().strftime('%Y-%m-%d')
            merged_dir = os.path.join(config.localReportDir, config.destinationDirectoryPath)
            kcm_dir = os.path.join(base_dir, "kcm-report", today_date, "ContentCompetencyMapping")

            # Better KCM file detection
            kcm_file = None
            if os.path.exists(kcm_dir):
                kcm_files = glob.glob(os.path.join(kcm_dir, "*.csv"))
                kcm_file = kcm_files[0] if kcm_files else None

            password = config.password

            # Clean and recreate merged directory
            if os.path.exists(merged_dir):
                shutil.rmtree(merged_dir)
            os.makedirs(merged_dir)

            # Track all MDOID values for KCM distribution
            all_mdoids = set()

            # Collect CSVs for each mdoid from all specified directories
            for subfolder in directories_to_select:
                report_dir = os.path.join(base_dir, subfolder, today_date)
                if os.path.exists(report_dir):
                    for item in os.listdir(report_dir):
                        item_path = os.path.join(report_dir, item)

                        if item.startswith("mdoid="):
                            if item.endswith(".csv"):
                                mdoid_value = item.replace(".csv", "")
                            else:
                                mdoid_value = item

                            # Track this MDOID for KCM distribution
                            all_mdoids.add(mdoid_value)

                            dest_dir = os.path.join(merged_dir, mdoid_value)
                            os.makedirs(dest_dir, exist_ok=True)

                            if os.path.isdir(item_path):
                                for f in os.listdir(item_path):
                                    if f.endswith(".csv"):
                                        shutil.copy(os.path.join(item_path, f),
                                                    os.path.join(dest_dir, f))
                            elif item_path.endswith(".csv"):
                                shutil.copy(item_path, os.path.join(dest_dir, f"{subfolder}.csv"))

            # Add KCM file to each MDOID folder
            if kcm_file and os.path.exists(kcm_file):
                print(f"Adding KCM file to {len(all_mdoids)} MDOID folders: {kcm_file}")
                for mdoid_value in all_mdoids:
                    dest_dir = os.path.join(merged_dir, mdoid_value)
                    if os.path.exists(dest_dir):
                        # Copy KCM file with its original name
                        shutil.copy(kcm_file, os.path.join(dest_dir, "ContentCompetencyMapping.csv"))

                # Separate KCM sync (existing functionality)
                print(f"Syncing KCM file separately: {kcm_file} -> {config.kcmSyncPath}")
                try:
                    sync_reports(kcm_file, config.kcmSyncPath, config)
                except Exception as e:
                    print(f"WARNING: Failed to sync KCM file separately: {e}")
            else:
                print(f"KCM file not found at expected path: {kcm_dir}")
            # Password-protected ZIP creation per mdoid folder
            for mdoid_folder in os.listdir(merged_dir):
                mdoid_path = os.path.join(merged_dir, mdoid_folder)
                if os.path.isdir(mdoid_path):
                    csv_files = [f for f in os.listdir(mdoid_path) if f.endswith(".csv")]
                    if csv_files:
                        for csv_file in csv_files:
                            csv_path = os.path.join(mdoid_path, csv_file)
                            zip_name = csv_file.replace(".csv", ".zip")
                            zip_path = os.path.join(mdoid_path, zip_name)
                            command = ["zip", "-P", password, "-j", zip_path, csv_path]
                            subprocess.run(command, check=True)
                    # Delete the individual CSVs after zipping
                    for f in csv_files:
                        os.remove(os.path.join(mdoid_path, f))

            print(f"All MDOID folders zipped with password at: {merged_dir}")
            sync_reports(merged_dir, config.mdoReportSyncPath, config)

            # ------------------ Part 2: Convert Parquet to CSV & Zip for full reports------------------ #
            warehouse_base = config.warehouseReportDir
            warehouse_output_dir = config.warehouseOutputDir
            warehouse_zip_path = os.path.join(warehouse_output_dir, "reports.zip")

            # Clean and recreate output directory
            if os.path.exists(warehouse_output_dir):
                shutil.rmtree(warehouse_output_dir)
            os.makedirs(warehouse_output_dir)

            # Traverse subfolders of warehouse and convert to CSV
            for folder in os.listdir(warehouse_base):
                folder_path = os.path.join(warehouse_base, folder)
                if os.path.isdir(folder_path):
                    parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
                    if parquet_files:
                        try:
                            df = spark.read.parquet(folder_path)
                            csv_output = os.path.join(warehouse_output_dir, f"{folder}.csv")
                            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output + "_tmp")

                            # Move the actual CSV file out of Spark's temp folder
                            for file in os.listdir(csv_output + "_tmp"):
                                if file.endswith(".csv"):
                                    shutil.move(os.path.join(csv_output + "_tmp", file), csv_output)
                            shutil.rmtree(csv_output + "_tmp")
                        except Exception as e:
                            print(f"Error processing {folder_path}: {str(e)}")

            # Password-protected ZIP of all warehouse CSVs
            warehouse_csvs = [os.path.join(warehouse_output_dir, f) for f in os.listdir(warehouse_output_dir) if
                              f.endswith(".csv")]
            if warehouse_csvs:
                zip_cmd = ["zip", "-P", password, "-j", warehouse_zip_path] + warehouse_csvs
                subprocess.run(zip_cmd, check=True)

            print(f"Warehouse report zipped at: {warehouse_zip_path}")
            sync_reports(warehouse_zip_path, config.fullReportSyncPath, config)

            self.upload_parquet_files(config)

            total_time = time.time() - start_time
            print(f"\n✅ Optimized Zip Upload job generation completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")


        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise
def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Zip Upload Model") \
        .config("spark.executor.memory", "42g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.shuffle.io.connectionTimeout", "300s") \
        .config("spark.shuffle.io.maxRetries", "20") \
        .config("spark.shuffle.io.retryWait", "10s") \
        .getOrCreate()
    # Create model instance
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] ZipUpload processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = ZipUploadModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] ZipUpload completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()