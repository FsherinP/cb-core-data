import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import duckdb
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil 



def write_csv_per_mdo_id(df, output_dir, groupByAttr, isIndividualWrite=False, threshold=100000, csv_filename="report.csv"):
    """
    Optimized hybrid write strategy: 
    - Small/medium groups: Direct CSV write via Spark partitionBy
    - Large groups: Filter first, then write to parquet for DuckDB processing
    
    Args:
        df (DataFrame): Source DataFrame
        output_dir (str): Output directory path
        groupByAttr (str): Column to group by
        isIndividualWrite (bool): If True, write as parquet instead of CSV
        threshold (int): Max row count per group to consider for fast write
        csv_filename (str): Name of the CSV file to create inside each partition folder
    """
    
    if isIndividualWrite == False:
        print("📊 Step 1: Analyzing group sizes...")
        df.cache().count()  # Cache the DataFrame to avoid recomputation
        # Step 1: Get group counts
        group_counts = df.groupBy(groupByAttr).count()
        group_counts.cache()  # Cache since we'll use it multiple times
        
        # Step 2: Collect IDs for fast + fallback paths
        small_ids = [row[groupByAttr] for row in group_counts.filter(col("count") <= threshold).collect()]
        large_ids = [row[groupByAttr] for row in group_counts.filter(col("count") > threshold).collect()]
        
        print(f"📈 Small groups (≤{threshold} rows, fast write): {len(small_ids)}")
        print(f"📊 Large groups (>{threshold} rows, DuckDB write): {len(large_ids)}")
        
        # Step 3: Fast write for small/medium groups - FILTER FIRST
        if small_ids:
            print("🚀 Writing small groups directly via Spark...")
            small_df = df.filter(col(groupByAttr).isin(small_ids))
            
            small_df \
                .repartition(groupByAttr) \
                .write \
                .mode("overwrite") \
                .partitionBy(groupByAttr) \
                .option("header", True) \
                .csv(output_dir + "_spark_temp")
            
            # Convert Spark partitioned output to folder structure
            convert_spark_partitions_to_folders(output_dir + "_spark_temp", output_dir, groupByAttr, csv_filename)
            
            print(f"✅ Completed writing {len(small_ids)} small groups")

        # Step 4: DuckDB write for large groups - FILTER FIRST, then write to parquet
        if large_ids:
            print("🦆 Processing large groups via DuckDB...")
            large_df = df.filter(col(groupByAttr).isin(large_ids))
            
            parquet_tmp_path = output_dir + '_tmp_large_groups'
            write_csv_per_mdo_id_duckdb(large_df, output_dir, groupByAttr, parquet_tmp_path, large_ids, csv_filename=csv_filename)
            
            print(f"✅ Completed writing {len(large_ids)} large groups")
        
        # Cleanup
        group_counts.unpersist()
        
    else:
        # Parquet write mode
        print("📦 Writing as partitioned parquet...")
        df.repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .option("compression", "snappy") \
            .parquet(output_dir)

def convert_spark_partitions_to_folders(spark_output_dir: str, final_output_dir: str, partition_column: str, csv_filename: str):
    """
    Convert Spark's partitioned CSV output to the desired folder structure.
    
    Args:
        spark_output_dir (str): Directory with Spark's partitioned CSV files
        final_output_dir (str): Target directory for folder structure
        partition_column (str): Partition column name
        csv_filename (str): Name for the CSV file inside each folder
    """
    import shutil
    
    spark_path = Path(spark_output_dir)
    final_path = Path(final_output_dir)
    final_path.mkdir(parents=True, exist_ok=True)
    
    # Find all partition directories
    partition_dirs = [d for d in spark_path.iterdir() 
                     if d.is_dir() and d.name.startswith(f"{partition_column}=")]
    
    for partition_dir in partition_dirs:
        # Extract partition value
        partition_value = partition_dir.name.split("=", 1)[1]
        safe_partition_value = str(partition_value).replace("/", "_")
        
        # Create target folder structure
        target_folder = final_path / f"{partition_column}={safe_partition_value}"
        target_folder.mkdir(parents=True, exist_ok=True)
        
        # Find CSV files in the partition directory
        csv_files = list(partition_dir.glob("*.csv"))
        
        if csv_files:
            # If multiple CSV files, merge them into one
            if len(csv_files) == 1:
                # Single file, just copy and rename
                shutil.copy2(csv_files[0], target_folder / csv_filename)
            else:
                # Multiple files, merge them
                merge_csv_files(csv_files, target_folder / csv_filename)
    
    # Cleanup temporary Spark output
    try:
        shutil.rmtree(spark_output_dir)
        print(f"✅ Cleaned up temporary Spark output: {spark_output_dir}")
    except Exception as e:
        print(f"⚠️ Could not clean up {spark_output_dir}: {e}")

def merge_csv_files(csv_files: list, output_file: Path):
    """Merge multiple CSV files into one, keeping only one header."""
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        first_file = True
        for csv_file in csv_files:
            with open(csv_file, 'r', encoding='utf-8') as infile:
                lines = infile.readlines()
                if first_file:
                    outfile.writelines(lines)  # Include header
                    first_file = False
                else:
                    outfile.writelines(lines[1:])  # Skip header for subsequent files

def write_csv_per_mdo_id_duckdb(df, output_dir: str, group_by_attr: str, parquet_tmp_path: str = None, 
                               large_ids=None, max_workers: int = 4, keep_parquets: bool = False, csv_filename: str = "report.csv"):
    """
    Writes CSVs per group_by_attr using partitioned parquet files and parallel conversion.
    Creates folder structure: group_by_attr=value/csv_filename

    Args:
        df (DataFrame): Spark DataFrame containing the groups to process
        output_dir (str): Output directory to write CSV folders
        group_by_attr (str): Column to group by (e.g., "mdo_id")
        parquet_tmp_path (str, optional): Temporary Parquet output path
        large_ids (list, optional): List of group IDs (for filtering, legacy compatibility)
        max_workers (int): Number of parallel workers for CSV conversion
        keep_parquets (bool): Whether to keep intermediate parquet files
        csv_filename (str): Name of the CSV file to create inside each partition folder
    """
    # Setup temporary parquet path if not provided
    if parquet_tmp_path is None:
        parquet_tmp_path = output_dir + "_temp_partitioned_parquets"
    
    print(f"📦 Step 1: Writing partitioned parquets...")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Filter by large_ids if provided (for backward compatibility)
    if large_ids is not None and len(large_ids) > 0:
        print(f"    - Filtering to {len(large_ids)} specific groups")
        df = df.filter(col(group_by_attr).isin(large_ids))
    
    # Write partitioned parquet files (Step 1)
    df \
        .repartition(group_by_attr) \
        .write \
        .mode("overwrite") \
        .partitionBy(group_by_attr) \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    
    # Clean up DataFrame from cache
    df.unpersist(blocking=True)
    print("🧹 Freed DataFrame from cache after parquet write")
    
    # Convert partitioned parquets to CSV files (Step 2)
    result = convert_partitioned_parquets_to_csv(
        parquet_input_dir=parquet_tmp_path,
        csv_output_dir=output_dir,
        partition_column=group_by_attr,
        max_workers=max_workers,
        process_subset=large_ids,  # Only process the large_ids if specified
        keep_parquets=keep_parquets,
        csv_filename=csv_filename
    )
    
    print("🎉 Done writing all group CSV files using partitioned parquet approach.")
    
    return {
        'successful_writes': result['successful_conversions'],
        'failed_writes': result['failed_conversions'],
        'total_groups': result['total_partitions'],
        'success_rate': result['success_rate'],
        'detailed_results': result
    }

def write_single_csv_duckdb(df, output_path: str, parquet_tmp_path: str = None, filter_condition: str = None, 
                           keep_parquets: bool = False):
    """
    Creates single CSV from all partitioned parquet files.
    More memory efficient as it processes data in partitioned chunks.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        output_path (str): Full path for the output CSV file (including filename)
        parquet_tmp_path (str, optional): Temporary Parquet output path  
        filter_condition (str, optional): SQL WHERE condition to filter data
        keep_parquets (bool): Whether to keep intermediate parquet files
    
    Returns:
        dict: Summary of the operation with row counts and success status
    """
    from pathlib import Path
    import shutil
    
    # Setup temporary parquet path if not provided
    if parquet_tmp_path is None:
        output_file = Path(output_path)
        parquet_tmp_path = str(output_file.parent / f"{output_file.stem}_temp_parquets")
    
    print(f"📦 Step 1: Writing DataFrame to partitioned parquet...")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Apply filter at Spark level if specified
    if filter_condition:
        print(f"    - Applying Spark filter: {filter_condition}")
        df = df.filter(filter_condition)
    
    # Write to partitioned parquet (using a dummy partition or let Spark decide)
    df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    
    # Clean up DataFrame from cache  
    df.unpersist(blocking=True)
    print("🧹 Freed DataFrame from cache after parquet write")
    
    print("🦆 Step 2: Loading partitioned parquet into DuckDB...")
    con = duckdb.connect()
    
    try:
        # DuckDB optimizations
        con.execute("PRAGMA memory_limit='40GB';")
        con.execute("PRAGMA threads=14;")
        con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
        con.execute("INSTALL parquet; LOAD parquet;")
        
        # Load all parquet files from the partitioned directory
        con.execute(f"CREATE TABLE source_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")
        
        # Get row count for verification
        total_rows = con.execute("SELECT COUNT(*) FROM source_df").fetchone()[0]
        print(f"    - Total rows loaded: {total_rows:,}")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"📤 Step 3: Writing single CSV to {output_path}...")
        
        # Write to CSV (filter_condition already applied at Spark level)
        query = f"""
            COPY (
                SELECT * FROM source_df
            ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
        """
        
        con.execute(query)
        
        # Verify the output file was created
        if output_file.exists():
            file_size = output_file.stat().st_size / (1024 * 1024)  # Size in MB
            print(f"✅ CSV successfully written!")
            print(f"    - File size: {file_size:.2f} MB")
            print(f"    - Rows written: {total_rows:,}")
            success = True
        else:
            print(f"❌ CSV file was not created!")
            success = False
            
    except Exception as e:
        print(f"❌ Error during CSV writing: {e}")
        success = False
        total_rows = 0
        
    finally:
        con.close()
    
    # Cleanup temporary parquet files
    if not keep_parquets:
        print("\n🧹 Cleaning up temporary parquet files...")
        try:
            shutil.rmtree(parquet_tmp_path)
            print(f"✅ Cleaned up: {parquet_tmp_path}")
        except Exception as e:
            print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")
    else:
        print(f"\n💾 Keeping intermediate parquet files at: {parquet_tmp_path}")
    
    # Summary
    print(f"\n📈 Single CSV Writing Summary:")
    print(f"   📁 Output file: {output_path}")
    print(f"   📊 Rows written: {total_rows:,}")
    print(f"   ✅ Success: {success}")
    
    if success:
        print("🎉 Done writing single CSV file using partitioned parquet approach!")
    
    return {
        'success': success,
        'output_path': output_path,
        'rows_written': total_rows,
        'total_rows': total_rows,
        'filter_applied': filter_condition is not None
    }

# Legacy method for writing single parquet file (not used in current workflow but kept for reference)
def write_single_parquet(df, final_path: str):
    """
    Writes a PySpark DataFrame as a single parquet file with a custom filename.

    Args:
        df: PySpark DataFrame
        final_path (str): Full destination path including filename (.parquet)
        spark: SparkSession (optional, only needed for some environments)
    """

    # Create a unique temp directory
    temp_dir = f"{final_path}_tmp_{uuid.uuid4().hex}"

    # Step 1: Write to temp directory as single partition
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .parquet(temp_dir)

    # Step 2: Locate the generated parquet part file
    part_file = None
    for file in os.listdir(temp_dir):
        if file.endswith(".parquet"):
            part_file = os.path.join(temp_dir, file)
            break

    if not part_file:
        raise Exception("No parquet part file found in temp directory")

    # Step 3: Ensure final directory exists
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    # Step 4: Move and rename to final path
    shutil.move(part_file, final_path)

    # Step 5: Clean up temp directory
    shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"✅ Parquet written to: {final_path}")

def write_csv_combined(df, single_csv_path: str, partitioned_output_dir: str, 
                      partition_column: str, parquet_tmp_path: str = None, 
                      filter_condition: str = None, max_workers: int = 4, 
                      keep_parquets: bool = False,csv_filename: str = "report.csv"):
    """
    Combined method using partitioned parquet approach for both outputs.
    More memory efficient and supports parallel processing for partitioned CSVs.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        single_csv_path (str): Full path for the single CSV file (including filename)
        partitioned_output_dir (str): Output directory for partitioned CSV files
        partition_column (str): Column to partition by (e.g., "mdo_id")
        parquet_tmp_path (str, optional): Temporary Parquet output path
        filter_condition (str, optional): SQL WHERE condition to filter data
        max_workers (int): Number of parallel workers for partitioned CSV conversion
        keep_parquets (bool): Whether to keep intermediate parquet files
    
    Returns:
        dict: Summary of both operations
    """
    from pathlib import Path
    import shutil
    
    # Setup temporary parquet path if not provided
    if parquet_tmp_path is None:
        parquet_tmp_path = partitioned_output_dir + "_temp_partitioned_parquets"
    
    print(f"📦 Step 1: Writing DataFrame to partitioned parquet...")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Apply filter at Spark level if specified
    if filter_condition:
        print(f"    - Applying Spark filter: {filter_condition}")
        df = df.filter(filter_condition)
        
        # Get filtered row count for reporting
        filtered_count = df.count()
        print(f"    - Filtered rows: {filtered_count:,}")
    
    # Write partitioned parquet files - ONLY ONCE
    df \
        .repartition(partition_column) \
        .write \
        .mode("overwrite") \
        .partitionBy(partition_column) \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    
    # Free DataFrame memory immediately after parquet write
    df.unpersist(blocking=True)
    print("🧹 Freed DataFrame from cache after parquet write")
    
    # Summary tracking
    summary = {
        'single_csv': {'success': False, 'rows_written': 0, 'path': single_csv_path},
        'partitioned_csv': {'successful_writes': 0, 'failed_writes': 0, 'total_partitions': 0},
        'total_rows': 0,
        'filter_applied': filter_condition is not None
    }
    
    # ===== STEP 2: Write Single CSV from partitioned parquets =====
    print(f"📤 Step 2: Writing single CSV from partitioned parquets...")
    
    con = duckdb.connect()
    try:
        # DuckDB optimizations
        con.execute("PRAGMA memory_limit='40GB';")
        con.execute("PRAGMA threads=14;")
        con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
        con.execute("INSTALL parquet; LOAD parquet;")
        
        # Load all partitioned parquet files - ONLY ONCE
        con.execute(f"CREATE TABLE source_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")
        
        # Get total row count
        total_rows = con.execute("SELECT COUNT(*) FROM source_df").fetchone()[0]
        summary['total_rows'] = total_rows
        print(f"    - Total rows loaded: {total_rows:,}")
        
        # Ensure output directory exists
        single_csv_file = Path(single_csv_path)
        single_csv_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            query = f"""
                COPY (
                    SELECT * FROM source_df
                ) TO '{single_csv_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """
            
            con.execute(query)
            
            # Verify single CSV was created
            if single_csv_file.exists():
                file_size = single_csv_file.stat().st_size / (1024 * 1024)  # Size in MB
                print(f"✅ Single CSV successfully written!")
                print(f"    - File size: {file_size:.2f} MB")
                print(f"    - Rows written: {total_rows:,}")
                summary['single_csv']['success'] = True
                summary['single_csv']['rows_written'] = total_rows
            else:
                print(f"❌ Single CSV file was not created!")
                
        except Exception as e:
            print(f"❌ Error writing single CSV: {e}")
        
    except Exception as e:
        print(f"❌ Error during DuckDB processing for single CSV: {e}")
    finally:
        con.close()
    
    # ===== STEP 3: Write Partitioned CSVs using new method =====
    print(f"📤 Step 3: Writing partitioned CSVs using parallel conversion...")
    
    partition_result = convert_partitioned_parquets_to_csv(
        parquet_input_dir=parquet_tmp_path,
        csv_output_dir=partitioned_output_dir,
        partition_column=partition_column,
        max_workers=max_workers,
        keep_parquets=keep_parquets,
        csv_filename=csv_filename  # This method handles cleanup
    )
    
    # Update summary with partition results
    if partition_result['success']:
        summary['partitioned_csv']['successful_writes'] = partition_result['successful_conversions']
        summary['partitioned_csv']['failed_writes'] = partition_result['failed_conversions']
        summary['partitioned_csv']['total_partitions'] = partition_result['total_partitions']
    
    # Overall summary
    print(f"\n🎉 Combined CSV Writing Complete!")
    print(f"   📁 Single CSV: {single_csv_path}")
    print(f"   📂 Partitioned CSVs: {partitioned_output_dir}")
    print(f"   📊 Total rows processed: {summary['total_rows']:,}")
    print(f"   🎯 Partitioned success rate: {partition_result.get('success_rate', 0):.1f}%")
    
    return {
        **summary,
        'partitioned_details': partition_result
    }


def convert_partitioned_parquets_to_csv(parquet_input_dir: str, csv_output_dir: str, 
                                      partition_column: str, max_workers: int = 4,
                                      process_subset: list = None, keep_parquets: bool = False,csv_filename: str = "report.csv"):
    """
    Convert partitioned parquet files to individual CSV files inside folders.
    Creates structure: csv_output_dir/partition_column=value/csv_filename
    
    Args:
        parquet_input_dir (str): Directory containing partitioned parquet files
        csv_output_dir (str): Output directory for CSV folders
        partition_column (str): The partition column name
        max_workers (int): Maximum number of parallel workers for conversion
        process_subset (list, optional): List of specific partition values to process
        keep_parquets (bool): Whether to keep the input parquet files after conversion
        csv_filename (str): Name of the CSV file to create inside each partition folder
    
    Returns:
        dict: Summary of conversion results
    """
    print(f"🦆 Converting partitioned parquets to CSV folders...")
    print(f"    - Source: {parquet_input_dir}")
    print(f"    - Target: {csv_output_dir}")
    print(f"    - Max workers: {max_workers}")
    print(f"    - CSV filename: {csv_filename}")
    
    # Discover partition directories
    parquet_path = Path(parquet_input_dir)
    if not parquet_path.exists():
        print(f"❌ Parquet directory does not exist: {parquet_input_dir}")
        return {'success': False, 'error': 'Parquet directory not found'}
    
    partition_dirs = [d for d in parquet_path.iterdir() 
                     if d.is_dir() and d.name.startswith(f"{partition_column}=")]
    
    # Filter to process subset if specified
    if process_subset:
        filtered_dirs = []
        subset_set = set(str(v) for v in process_subset)
        
        for d in partition_dirs:
            partition_value = d.name.split("=", 1)[1] if "=" in d.name else "unknown"
            if partition_value in subset_set:
                filtered_dirs.append(d)
        
        partition_dirs = filtered_dirs
        print(f"    - Processing subset: {len(partition_dirs)} partitions")
    
    total_partitions = len(partition_dirs)
    print(f"    - Found {total_partitions} partitions to convert")
    
    if total_partitions == 0:
        print("❌ No partition directories found")
        return {'success': False, 'error': 'No partitions found'}
    
    # Helper function to convert single partition
    def convert_single_partition_to_folder(partition_dir, csv_output_dir, partition_column, csv_filename, partition_value=None):
        try:
            # Extract partition value from directory name if not provided
            if partition_value is None:
                dir_name = Path(partition_dir).name
                if "=" in dir_name:
                    partition_value = dir_name.split("=", 1)[1]
                else:
                    partition_value = "unknown"
            
            # Sanitize partition value for folder name
            safe_partition_value = str(partition_value).replace("/", "_") if partition_value else "null"
            
            # Setup paths - Create folder structure
            partition_folder = Path(csv_output_dir) / f"{partition_column}={safe_partition_value}"
            partition_folder.mkdir(parents=True, exist_ok=True)
            
            csv_output_path = partition_folder / csv_filename
            
            # Connect to DuckDB
            con = duckdb.connect()
            
            try:
                # DuckDB optimizations for single partition processing
                con.execute("PRAGMA memory_limit='20GB';")  # Lower memory for parallel processing
                con.execute("PRAGMA threads=4;")  # Lower threads for parallel processing
                con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
                con.execute("INSTALL parquet; LOAD parquet;")
                
                # Load the specific partition parquet files
                parquet_pattern = f"{partition_dir}/*.parquet"
                con.execute(f"CREATE TABLE partition_df AS SELECT * FROM parquet_scan('{parquet_pattern}');")
                
                # Get row count
                row_count = con.execute("SELECT COUNT(*) FROM partition_df").fetchone()[0]
                
                # Write to CSV
                con.execute(f"""
                    COPY (
                        SELECT * FROM partition_df
                    ) TO '{csv_output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
                """)
                
                # Verify output file
                if csv_output_path.exists():
                    file_size_mb = csv_output_path.stat().st_size / (1024 * 1024)
                    return {
                        'success': True,
                        'partition_value': partition_value,
                        'csv_path': str(csv_output_path),
                        'folder_path': str(partition_folder),
                        'rows_written': row_count,
                        'file_size_mb': round(file_size_mb, 2),
                        'error': None
                    }
                else:
                    return {
                        'success': False,
                        'partition_value': partition_value,
                        'csv_path': str(csv_output_path),
                        'folder_path': str(partition_folder),
                        'rows_written': 0,
                        'file_size_mb': 0,
                        'error': 'CSV file was not created'
                    }
                    
            finally:
                con.close()
                
        except Exception as e:
            return {
                'success': False,
                'partition_value': partition_value if 'partition_value' in locals() else 'unknown',
                'csv_path': str(csv_output_path) if 'csv_output_path' in locals() else 'unknown',
                'folder_path': str(partition_folder) if 'partition_folder' in locals() else 'unknown',
                'rows_written': 0,
                'file_size_mb': 0,
                'error': str(e)
            }
    
    # Conversion tracking
    results = []
    successful_conversions = 0
    failed_conversions = 0
    total_rows = 0
    total_size_mb = 0
    
    # Process partitions (sequential or parallel)
    if max_workers == 1:
        # Sequential processing
        print("🔄 Processing partitions sequentially...")
        for i, partition_dir in enumerate(partition_dirs, 1):
            partition_value = partition_dir.name.split("=", 1)[1] if "=" in partition_dir.name else "unknown"
            
            result = convert_single_partition_to_folder(str(partition_dir), csv_output_dir, partition_column, csv_filename, partition_value)
            results.append(result)
            
            if result['success']:
                successful_conversions += 1
                total_rows += result['rows_written']
                total_size_mb += result['file_size_mb']
                print(f"✅ {i}/{total_partitions}: {partition_value} ({result['rows_written']:,} rows) → {result['folder_path']}")
            else:
                failed_conversions += 1
                print(f"❌ {i}/{total_partitions}: {partition_value} - {result['error']}")
    
    else:
        # Parallel processing
        print(f"🚀 Processing partitions in parallel ({max_workers} workers)...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_partition = {}
            for partition_dir in partition_dirs:
                partition_value = partition_dir.name.split("=", 1)[1] if "=" in partition_dir.name else "unknown"
                
                future = executor.submit(
                    convert_single_partition_to_folder,
                    str(partition_dir), csv_output_dir, partition_column, csv_filename, partition_value
                )
                future_to_partition[future] = partition_value
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_partition):
                partition_value = future_to_partition[future]
                completed += 1
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        successful_conversions += 1
                        total_rows += result['rows_written']
                        total_size_mb += result['file_size_mb']
                        print(f"✅ {completed}/{total_partitions}: {partition_value} ({result['rows_written']:,} rows) → {Path(result['folder_path']).name}/")
                    else:
                        failed_conversions += 1
                        print(f"❌ {completed}/{total_partitions}: {partition_value} - {result['error']}")
                        
                except Exception as e:
                    failed_conversions += 1
                    print(f"❌ {completed}/{total_partitions}: {partition_value} - Exception: {e}")
                    results.append({
                        'success': False,
                        'partition_value': partition_value,
                        'error': str(e)
                    })
    
    # Cleanup parquet files if requested
    if not keep_parquets:
        print(f"\n🧹 Cleaning up partitioned parquet files...")
        try:
            import shutil
            shutil.rmtree(parquet_input_dir)
            print(f"✅ Cleaned up: {parquet_input_dir}")
        except Exception as e:
            print(f"⚠️ Could not clean up {parquet_input_dir}: {e}")
    else:
        print(f"\n💾 Keeping partitioned parquet files at: {parquet_input_dir}")
    
    # Final summary
    success_rate = (successful_conversions / total_partitions * 100) if total_partitions > 0 else 0
    
    print(f"\n📈 Conversion Summary:")
    print(f"   📊 Total partitions: {total_partitions}")
    print(f"   ✅ Successful: {successful_conversions}")
    print(f"   ❌ Failed: {failed_conversions}")
    print(f"   🎯 Success rate: {success_rate:.1f}%")
    print(f"   📄 Total rows: {total_rows:,}")
    print(f"   💾 Total size: {total_size_mb:.2f} MB")
    print(f"   📂 CSV output structure: {csv_output_dir}/partition=value/{csv_filename}")
    
    return {
        'success': successful_conversions > 0,
        'total_partitions': total_partitions,
        'successful_conversions': successful_conversions,
        'failed_conversions': failed_conversions,
        'success_rate': success_rate,
        'total_rows': total_rows,
        'total_size_mb': total_size_mb,
        'csv_output_dir': csv_output_dir,
        'results': results
    }
