import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import MapType, StringType, StructType, StructField, ArrayType
from datetime import datetime
import sys
import time

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config


class KCMModel:
    """
    Python implementation of KCM (Knowledge and Competency Management) Model
    """
    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.kcm.KCMModel"
        
    def name(self):
        return "KCMModel"
    
    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def process_data(self, spark,config):
        """
        Process KCM data
        
        Args:
            spark: SparkSession
        """
        try:
            today = self.get_date()
            report_path_content_competency_mapping = f"{config.localReportDir}/{config.kcmReportPath}/{today}/ContentCompetencyMapping"
            file_name = config.kcmReport

            # Content - Competency Mapping data
            categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program"]
            initial_df = spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)\
                .filter(F.col("category").isin(categories))\
                .where("courseStatus IN ('Live', 'Retired')")\
                .select("courseID", "competencyAreaRefId", "competencyThemeRefId", "competencySubThemeRefId", "courseName")
            
            schema = initial_df.schema
            competency_area_type = None
            competency_theme_type = None
            competency_sub_theme_type = None
            
            for field in schema.fields:
                if field.name == "competencyAreaRefId":
                    competency_area_type = field.dataType
                elif field.name == "competencyThemeRefId":
                    competency_theme_type = field.dataType
                elif field.name == "competencySubThemeRefId":
                    competency_sub_theme_type = field.dataType
            
            # If columns are already arrays, use them directly. If strings, parse them.
            if isinstance(competency_area_type, ArrayType):
                print("Columns are already arrays - using them directly")
                cbp_details = initial_df.select(
                    F.col("courseID"),
                    F.when(F.size(F.col("competencyAreaRefId")) == 0, F.array()).otherwise(F.col("competencyAreaRefId")).alias("competencyAreaRefId"),
                    F.when(F.size(F.col("competencyThemeRefId")) == 0, F.array()).otherwise(F.col("competencyThemeRefId")).alias("competencyThemeRefId"),
                    F.when(F.size(F.col("competencySubThemeRefId")) == 0, F.array()).otherwise(F.col("competencySubThemeRefId")).alias("competencySubThemeRefId"),
                    F.col("courseName")
                )
            else:
                print("Columns are strings - parsing them into arrays")
                def parse_array_string(col_name):
                    """Convert string like '[item1, item2]' back to array"""
                    return F.when(
                        (F.col(col_name).isNotNull()) & 
                        (F.col(col_name) != "") & 
                        (F.col(col_name) != "null") & 
                        (F.col(col_name) != "[]"),
                        F.split(
                            F.regexp_replace(
                                F.regexp_replace(F.col(col_name), r"^\[|\]$", ""),  # Remove [ ]
                                r"\s*,\s*", ","  # Clean up spaces around commas
                            ), 
                            ","
                        )
                    ).otherwise(F.array())
                
                # Convert the string fields back to arrays
                cbp_details = initial_df.select(
                    F.col("courseID"),
                    parse_array_string("competencyAreaRefId").alias("competencyAreaRefId"),
                    parse_array_string("competencyThemeRefId").alias("competencyThemeRefId"), 
                    parse_array_string("competencySubThemeRefId").alias("competencySubThemeRefId"),
                    F.col("courseName")
                )

            #explode area, theme and sub theme separately
            area_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencyAreaRefId) as (pos, competency_area_id)")
            ).repartition(F.col("courseID"))
            
            theme_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencyThemeRefId) as (pos, competency_theme_id)")
            ).repartition(F.col("courseID"))
            
            sub_theme_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencySubThemeRefId) as (pos, competency_sub_theme_id)")
            ).repartition(F.col("courseID"))
            
            # Joining area, theme and subtheme based on position
            competency_joined_df = area_exploded.join(theme_exploded, ["courseID", "pos"]) \
                .join(sub_theme_exploded, ["courseID", "pos"])
            
            # joining with cbp_details for getting courses with no competencies mapped to it
            competency_content_mapping_df = cbp_details \
                .join(competency_joined_df, ["courseID"], "left") \
                .dropDuplicates(["courseID", "competency_area_id", "competency_theme_id", "competency_sub_theme_id"])
            
            content_mapping_df = competency_content_mapping_df \
                .withColumn("data_last_generated_on", F.lit(self.current_date_time())) \
                .select(
                    F.col("courseID").alias("course_id"), 
                    F.col("competency_area_id"), 
                    F.col("competency_theme_id"), 
                    F.col("competency_sub_theme_id"), 
                    F.col("data_last_generated_on")
                )

            content_mapping_df.distinct().coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwKcmContentTable}/")

            # Load KCM v6 data
            kcmv6 = spark.read.parquet(ParquetFileConstants.KCMV6_PARQUET_FILE)

            # Define the schema
            hierarchy_schema = """
            STRUCT<
                categories: ARRAY<
                    STRUCT<
                        terms: ARRAY<
                            STRUCT<
                                refId: STRING,
                                name: STRING,
                                description: STRING,
                                associations: ARRAY<
                                    STRUCT<
                                        refId: STRING,
                                        name: STRING,
                                        description: STRING
                                    >
                                >
                            >
                        >
                    >
                >
            >
            """

            # Parse the JSON column (ONLY ONCE)
            kcmv6 = kcmv6.withColumn(
                "hierarchy_parsed", 
                F.from_json(F.col("hierarchy"), hierarchy_schema)
            )

            # Process area data (using hierarchy_parsed)
            kcm_area = kcmv6.withColumn(
                "competencyAreaData", 
                F.col("hierarchy_parsed.categories")[0]
            ).withColumn(
                "termsExploded", 
                F.explode(F.col("competencyAreaData.terms"))
            ).withColumn(
                "associatedTheme",
                F.explode_outer(F.col("termsExploded.associations"))
            ).select(
                F.col("termsExploded.refId").alias("areaID"),
                F.col("termsExploded.name").alias("areaName"),
                F.col("termsExploded.description").alias("areaDescription"),
                F.col("associatedTheme.refId").alias("themeID"),
                F.col("associatedTheme.name").alias("themeName")
            )

            # Process theme data (using hierarchy_parsed)
            kcm_theme = kcmv6.withColumn(
                "competencyThemeData", 
                F.col("hierarchy_parsed.categories")[1]
            ).withColumn(
                "termsExploded", 
                F.explode(F.col("competencyThemeData.terms"))
            ).withColumn(
                "associatedSubTheme",
                F.explode_outer(F.col("termsExploded.associations"))
            ).select(
                F.col("termsExploded.refId").alias("themeID"),
                F.col("termsExploded.name").alias("themeName"),
                F.col("termsExploded.description").alias("themeDescription"),
                F.col("associatedSubTheme.refId").alias("subThemeID"),
                F.col("associatedSubTheme.name").alias("subThemeName"),
                F.col("associatedSubTheme.description").alias("subThemeDescription")
            )

            # Process All subthemes
            kcm_subtheme_all = kcmv6.withColumn(
                "competencySubThemeData",
                F.col("hierarchy_parsed.categories")[2]
            ).withColumn(
                "termsExploded",
                F.explode(F.col("competencySubThemeData.terms"))
            ).select(
                F.col("termsExploded.refId").alias("subThemeID"),
                F.col("termsExploded.name").alias("subThemeName"),
                F.col("termsExploded.description").alias("subThemeDescription")
            ).distinct()

            area_theme_subtheme = kcm_area.join(
                kcm_theme,
                ["themeID", "themeName"],
                "outer"                          # outer keeps: themes with no area, areas with no theme
            )

            kcm_subtheme_renamed = kcm_subtheme_all.select(
                F.col("subThemeID").alias("master_subThemeID"),
                F.col("subThemeName").alias("master_subThemeName"),
                F.col("subThemeDescription").alias("master_subThemeDescription")
            )

            # Join and finalize
            competency_details_df = kcm_subtheme_renamed.join(
                area_theme_subtheme,
                kcm_subtheme_renamed["master_subThemeID"] == area_theme_subtheme["subThemeID"],
                "left"                           # left keeps every subtheme from the master list
            ).select(
                F.col("areaID").alias("competency_area_id"),
                F.col("areaName").alias("competency_area"),
                F.col("areaDescription").alias("competency_area_description"),
                F.col("themeID").alias("competency_theme_id"),
                F.col("themeName").alias("competency_theme"),
                F.col("themeDescription").alias("competency_theme_description"),
                F.col("master_subThemeID").alias("competency_sub_theme_id"),
                F.col("master_subThemeName").alias("competency_sub_theme"),
                F.col("master_subThemeDescription").alias("competency_sub_theme_description"),

                F.lit("Null").alias("competency_theme_type"),
                F.lit(self.current_date_time()).alias("data_last_generated_on")
            ).distinct()

            competency_details_df.distinct().write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")

            # Competency reporting
            competency_reporting = competency_content_mapping_df \
                .join(competency_details_df, ["competency_area_id", "competency_theme_id", "competency_sub_theme_id"]) \
                .withColumn("competency_theme_type", F.lit("Null")) \
                .select(
                    F.col("courseID").alias("content_id"),
                    F.col("courseName").alias("content_name"),
                    F.col("competency_area"),
                    F.col("competency_area_description"),
                    F.col("competency_theme"),
                    F.col("competency_theme_description"),
                    F.col("competency_theme_type"),
                    F.col("competency_sub_theme"),
                    F.col("competency_sub_theme_description")
                ) \
                .orderBy("content_id") \
                .distinct()
            
            temp_dir = f"{report_path_content_competency_mapping}/{file_name}_temp"
            competency_reporting.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(temp_dir)
            
            # Move the part file to the desired filename
            import os
            import glob
            import shutil
            
            # Find the part file
            part_files = glob.glob(f"{temp_dir}/part-*.csv")
            if part_files:
                part_file = part_files[0]
                final_path = f"{report_path_content_competency_mapping}/{file_name}"
                
                # Create the directory if it doesn't exist
                os.makedirs(report_path_content_competency_mapping, exist_ok=True)
                
                # Copy the part file to the final location with desired name
                shutil.copy2(part_file, final_path)
                
                # Clean up the temporary directory
                shutil.rmtree(temp_dir)
                print(f"CSV file created successfully: {final_path}")
            else:
                print("Warning: No part files found in the temporary directory")
        except Exception as e:
            print(f"Error occurred during KCMModel processing: {str(e)}")
            sys.exit(1)
    
    @staticmethod
    def get_kcm_schema():
        """Returns the schema for KCM data"""
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType
        
        return StructType([
            StructField("categories", ArrayType(
                StructType([
                    StructField("code", StringType(), False),
                    StructField("terms", ArrayType(
                        StructType([
                            StructField("name", StringType(), False),
                            StructField("description", StringType(), False),
                            StructField("refId", StringType(), False),
                            StructField("category", StringType(), False),
                            StructField("associations", ArrayType(
                                StructType([
                                    StructField("name", StringType(), False),
                                    StructField("refType", StringType(), False),
                                    StructField("description", StringType(), False),
                                    StructField("refId", StringType(), False),
                                    StructField("category", StringType(), False)
                                ]), 
                                False
                            ))
                        ]), 
                        False
                    ))
                ]),
                False
            ))
        ])
    
def main():
    spark = SparkSession.builder.appName("KCM Model").getOrCreate()
    
    # Create model instance
    start_time = datetime.now()
    print(f"[START] KCMModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = KCMModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] KCMModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()