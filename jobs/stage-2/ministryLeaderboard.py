import findspark

findspark.init()
import os
import time
import duckdb
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import (col, lit, current_date, date_format, add_months, last_day, date_trunc, date_add)
from datetime import datetime
import sys
from pyspark.sql.functions import to_timestamp

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config


class MinistryLeaderBoardModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.leaderboard.MinistryLeaderBoardModel"

    def name(self):
        return "MinistryLeaderBoardModel"

    def process_data(self, spark, config):
        try:
            start_time = time.time()

            # Create temp directory
            project_dir = str(Path(__file__).resolve().parents[3])
            temp_dir = f"{project_dir}/temp_leaderboard_duckdb"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            print("=" * 80)
            print("MINISTRY LEADERBOARD - DuckDB Optimized")
            print("=" * 80)

            # Calculate month window
            month_start_col = spark.sql(
                "SELECT date_format(date_trunc('MONTH', add_months(current_date(), -1)), 'yyyy-MM-dd HH:mm:ss')").collect()[
                0][0]
            month_end_col = spark.sql(
                "SELECT concat(date_format(last_day(add_months(current_date(), -1)), 'yyyy-MM-dd'), ' 23:59:59')").collect()[
                0][0]
            month_num_col = \
            spark.sql("SELECT date_format(date_add(last_day(add_months(current_date(), -1)), 1), 'M')").collect()[0][0]
            year_num_col = spark.sql("SELECT date_format(add_months(current_date(), -1), 'yyyy')").collect()[0][0]
            app_postgres_url = f"jdbc:postgresql://{config.appPostgresHost}/{config.appPostgresSchema}"
            # Convert to strings/ints
            month_start = str(month_start_col)
            month_end = str(month_end_col)
            month_num = int(month_num_col)
            year_num = int(year_num_col)

            print(f"\n[1/5] Processing for Month: {month_num}, Year: {year_num}")
            print(f"  Date Range: {month_start} to {month_end}")
            # Initialize DuckDB
            db_path = f"{temp_dir}/leaderboard.duckdb"
            con = duckdb.connect(database=db_path)
            con.execute(f"SET temp_directory='{temp_dir}'")
            con.execute("SET memory_limit='8GB'")
            con.execute("SET threads=8")

            print("\n[2/5] Loading data into DuckDB...")

            # Load parquet files directly into DuckDB
            user_org_path = ParquetFileConstants.USER_ORG_COMPUTED_FILE
            org_hierarchy_path = ParquetFileConstants.ORG_COMPLETE_HIERARCHY_PARQUET_FILE
            karma_points_path = ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE

            # Create views in DuckDB
            con.execute(f"""
                CREATE OR REPLACE VIEW user_org AS
                SELECT * FROM read_parquet('{user_org_path}/**.parquet')
            """)

            con.execute(f"""
                CREATE OR REPLACE VIEW org_hierarchy AS
                SELECT * FROM read_parquet('{org_hierarchy_path}')
            """)

            con.execute(f"""
                CREATE OR REPLACE VIEW karma_points AS
                SELECT 
                    userid,
                    points,
                    credit_date
                FROM read_parquet('{karma_points_path}')
                WHERE credit_date >= '{month_start}' 
                  AND credit_date <= '{month_end}'
            """)

            user_org_count = con.execute("SELECT COUNT(*) FROM user_org").fetchone()[0]
            org_hierarchy_count = con.execute("SELECT COUNT(*) FROM org_hierarchy").fetchone()[0]
            karma_count = con.execute("SELECT COUNT(*) FROM karma_points").fetchone()[0]

            print(f"  User-Org records: {user_org_count:,}")
            print(f"  Org Hierarchy records: {org_hierarchy_count:,}")
            print(f"  Karma Points (filtered): {karma_count:,}")

            print("\n[3/5] Aggregating karma points...")

            # Aggregate karma points
            con.execute(f"""
                CREATE OR REPLACE TABLE karma_aggregated AS
                SELECT 
                    userid,
                    SUM(points) as total_points,
                    MAX(credit_date) as last_credit_date
                FROM karma_points
                GROUP BY userid
            """)

            agg_count = con.execute("SELECT COUNT(*) FROM karma_aggregated").fetchone()[0]
            print(f"  Users with karma points: {agg_count:,}")

            print("\n[4/5] Building organization hierarchies and joining...")

            # Get distinct MDO IDs
            con.execute(f"""
                CREATE OR REPLACE TABLE distinct_mdos AS
                SELECT DISTINCT userOrgID
                FROM user_org
            """)

            # Join org hierarchy with distinct MDOs
            con.execute(f"""
                CREATE OR REPLACE TABLE joined_orgs AS
                SELECT oh.*
                FROM org_hierarchy oh
                INNER JOIN distinct_mdos dm ON oh.sborgid = dm.userOrgID
            """)

            # Process L3 organizations (MDO level)
            con.execute(f"""
                CREATE OR REPLACE TABLE orgs_l3 AS
                SELECT DISTINCT
                    uo.userID,
                    uo.userOrgID as userParentID,
                    uo.professionalDetails.designation as designation,
                    uo.userProfileImgUrl,
                    uo.fullName,
                    uo.userOrgName
                FROM user_org uo
                INNER JOIN (
                    SELECT DISTINCT sborgid as organisationID
                    FROM joined_orgs
                    WHERE sborgtype = 'mdo' 
                      AND sborgsubtype != 'department'
                ) orgs ON uo.userOrgID = orgs.organisationID
            """)

            # Process L2 departments
            con.execute(f"""
                CREATE OR REPLACE TABLE orgs_l2 AS
                SELECT DISTINCT
                    uo.userID,
                    dept.departmentID as userParentID,
                    uo.professionalDetails.designation as designation,
                    uo.userProfileImgUrl,
                    uo.fullName,
                    uo.userOrgName
                FROM user_org uo
                INNER JOIN (
                    SELECT DISTINCT
                        jo1.sborgid as departmentID,
                        jo2.sborgid as organisationID
                    FROM joined_orgs jo1
                    LEFT JOIN joined_orgs jo2 ON jo1.mapid = jo2.l2mapid
                    WHERE jo1.sborgtype = 'department'
                ) dept ON uo.userOrgID = dept.departmentID 
                       OR uo.userOrgID = dept.organisationID
            """)

            # Process L1 ministries
            con.execute(f"""
                CREATE OR REPLACE TABLE orgs_l1 AS
                SELECT DISTINCT
                    uo.userID,
                    min_data.ministryID as userParentID,
                    uo.professionalDetails.designation as designation,
                    uo.userProfileImgUrl,
                    uo.fullName,
                    uo.userOrgName
                FROM user_org uo
                INNER JOIN (
                    SELECT DISTINCT
                        jo1.sborgid as ministryID,
                        jo2.sborgid as departmentID,
                        jo3.sborgid as organisationID
                    FROM joined_orgs jo1
                    LEFT JOIN joined_orgs jo2 ON jo1.mapid = jo2.l1mapid
                    LEFT JOIN joined_orgs jo3 ON jo2.mapid = jo3.l2mapid
                    WHERE jo1.sborgtype IN ('ministry', 'state')
                ) min_data ON uo.userOrgID = min_data.ministryID 
                           OR uo.userOrgID = min_data.departmentID
                           OR uo.userOrgID = min_data.organisationID
            """)

            # Union all org levels
            con.execute(f"""
                CREATE OR REPLACE TABLE user_org_combined AS
                SELECT * FROM orgs_l3
                UNION ALL
                SELECT * FROM orgs_l2
                UNION ALL
                SELECT * FROM orgs_l1
            """)

            combined_count = con.execute("SELECT COUNT(*) FROM user_org_combined").fetchone()[0]
            print(f"  Combined user-org mappings: {combined_count:,}")

            print("\n[5/5] Calculating leaderboard rankings...")

            # Join with karma and calculate rankings
            con.execute(f"""
                CREATE OR REPLACE TABLE leaderboard_final AS
                SELECT 
                    uoc.userID as userid,
                    uoc.userParentID as org_id,
                    uoc.fullName as fullname,
                    uoc.userProfileImgUrl as profile_image,
                    uoc.userOrgName as org_name,
                    uoc.designation,
                    ka.total_points,
                    ka.last_credit_date,
                    {month_num} as month,
                    {year_num} as year,
                    DENSE_RANK() OVER (
                        PARTITION BY uoc.userParentID 
                        ORDER BY ka.total_points DESC
                    ) as rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY uoc.userParentID 
                        ORDER BY ka.total_points DESC, ka.last_credit_date DESC
                    ) as row_num
                FROM user_org_combined uoc
                INNER JOIN karma_aggregated ka ON uoc.userID = ka.userid
                WHERE uoc.userParentID IS NOT NULL 
                  AND uoc.userParentID != ''
            """)

            # Get final count
            final_count = con.execute("SELECT COUNT(*) FROM leaderboard_final").fetchone()[0]
            unique_orgs = con.execute("SELECT COUNT(DISTINCT org_id) FROM leaderboard_final").fetchone()[0]
            unique_users = con.execute("SELECT COUNT(DISTINCT userid) FROM leaderboard_final").fetchone()[0]

            print(f"\n  Total leaderboard entries: {final_count:,}")
            print(f"  Unique organizations: {unique_orgs:,}")
            print(f"  Unique users: {unique_users:,}")

            # Export to parquet
            output_file = f"{temp_dir}/leaderboard_final.parquet"
            con.execute(f"""
                COPY (SELECT * FROM leaderboard_final) 
                TO '{output_file}' 
                (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
            """)

            con.close()

            elapsed_time = time.time() - start_time
            print(
                f"\n[INFO] DuckDB processing completed in {elapsed_time:.1f} seconds ({elapsed_time / 60:.1f} minutes)")

            # Read back to Spark for Cassandra write
            print("\n[6/6] Writing to Cassandra...")
            leaderboard_df = spark.read.parquet(output_file)
            final_df = leaderboard_df.select(
                col("org_id"),
                col("row_num").cast("integer"),
                col("designation"),
                col("fullname"),
                col("org_name"),
                col("profile_image"),
                col("total_points").cast("integer").alias("total_learning_hours"),  # Same value as total_points
                col("total_points").cast("integer"),
                col("userid"))

            # leaderboard_df = leaderboard_df.withColumn("last_credit_date", to_timestamp(col("last_credit_date")))
            # Repartition for efficient Cassandra write
            final_df = final_df.orderBy("org_id", "row_num")

            # Use coalesce instead of repartition to avoid shuffling
            final_df = final_df.coalesce(10)
            self.write_postgres_table(final_df, app_postgres_url,
                                      "slw_mdo_top_learners",
                                      config.appPostgresUsername,
                                      config.appPostgresCredential)
            # Cleanup
            print("\nCleaning up temporary files...")
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

            total_time = time.time() - start_time
            print("\n" + "=" * 80)
            print(f"[SUCCESS] MinistryLearnerLeaderboardModel completed in {total_time:.1f} seconds")
            print("=" * 80)

        except Exception as e:
            print(f"❌ Error occurred during MinistryLearnerLeaderboard processing: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def write_postgres_table(self, df, url: str, table: str, username: str, password: str, mode: str = "overwrite"):
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("truncate", "true") \
            .option("createTableOptions", "") \
            .mode("overwrite") \
            .save()


def create_spark_session_with_packages(config):
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("Ministry leaderboard Model - DuckDB") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '500') \
        .config("spark.cassandra.output.batch.grouping.key", "partition") \
        .config("spark.cassandra.output.consistency.level", "LOCAL_ONE") \
        .config("spark.cassandra.connection.keepAliveMS", "180000") \
        .config("spark.cassandra.connection.timeoutMS", '180000') \
        .config("spark.cassandra.read.timeoutMS", '180000') \
        .config("spark.cassandra.output.concurrent.writes", "3") \
        .getOrCreate()
    return spark


def main():
    start_time = datetime.now()
    print(f"[START] Ministry leaderboard processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    spark = create_spark_session_with_packages(config)
    model = MinistryLeaderBoardModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Ministry leaderboard completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()