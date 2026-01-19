import os
import sys
import glob
import shutil
import re

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.config import LANDING_DIR, STAGING_DIR
from utils.logger import get_logger
from utils.spark import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, substring

logger = get_logger("raw_to_bronze_prep")

def main():
    input_pattern = os.path.join(LANDING_DIR, "*.csv.gz")
    output_dir = STAGING_DIR
    
    logger.info(f"Search pattern for raw files: {input_pattern}")
    logger.info(f"Output directory: {output_dir}")
    
    input_files = sorted(glob.glob(input_pattern))
    if not input_files:
        logger.error(f"No raw *.csv.gz files found at {input_pattern}")
        sys.exit(1)
        
    logger.info(f"Found {len(input_files)} raw files to process:")
    for f in input_files:
        logger.info(f"    - {os.path.basename(f)}")

    # Cleanup output directory to start fresh
    if os.path.exists(output_dir):
        logger.info(f"Clearing existing staging directory at {output_dir}...")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Initialize Spark Session via shared utility
    logger.info("Initializing Spark Session...")
    spark = get_spark_session("Split_Raw_Clickstream_Spark", "2g")

    # Define exact schema upfront
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True)
    ])

    try:
        # Loop through each raw file and process
        for idx, file_path in enumerate(input_files):
            file_name = os.path.basename(file_path)
            logger.info(f"Processing file {idx+1}/{len(input_files)}: {file_name}...")
            
            # Extract year from filename dynamically (e.g., '2020-Jan.csv.gz' -> '2020')
            match = re.search(r"(\d{4})", file_name)
            file_year = match.group(1) if match else "2020"
            logger.info(f"Detected target year for filtering: {file_year}")

            # Read raw CSV
            raw_df = spark.read.csv(file_path, header=True, schema=schema)

            # Extract year, month, day columns from event_time (e.g. '2020-01-01 00:00:00 UTC')
            partitioned_df = raw_df \
                .withColumn("year", substring(col("event_time"), 1, 4)) \
                .withColumn("month", substring(col("event_time"), 6, 2)) \
                .withColumn("day", substring(col("event_time"), 9, 2)) \
                .filter(col("year") == file_year)

            # Write partitioned Parquet to raw_staging/
            logger.info(f"Writing partitions for {file_name} to raw_staging...")
            partitioned_df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(output_dir)
                
            logger.info(f"Completed processing: {file_name}")
            
        logger.info("PySpark Clickstream splitting for all months completed successfully!")

    except Exception as e:
        logger.error(f"Spark processing failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
