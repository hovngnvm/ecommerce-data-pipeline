import sys
import glob
import shutil
import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, substring

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config.settings import settings
from scripts.utils.logger import get_logger
from scripts.utils.spark import get_spark_session

logger = get_logger(__name__)

def run_raw_prep(input_pattern: str | None = None, output_dir: str | None = None, spark: SparkSession | None = None) -> None:
    """
    Reads raw compressed CSV clickstream files, partitions them by year/month/day,
    and writes optimized partitioned Parquet files to the staging directory.
    """
    pattern = input_pattern or str(Path(settings.landing_dir) / "*.csv.gz")
    out_dir = output_dir or settings.staging_dir

    logger.info(f"Search pattern for raw files: {pattern}")
    logger.info(f"Output directory: {out_dir}")

    input_files = sorted(glob.glob(pattern))
    if not input_files:
        logger.error(f"No raw *.csv.gz files found at {pattern}")
        raise FileNotFoundError(f"No raw files found at {pattern}")

    logger.info(f"Found {len(input_files)} raw files to process:")
    for f in input_files:
        logger.info(f"    - {Path(f).name}")

    if Path(out_dir).exists():
        logger.info(f"Clearing existing staging directory at {out_dir}...")
        shutil.rmtree(out_dir)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    should_stop_spark = False
    if spark is None:
        logger.info("Initializing Spark Session...")
        spark = get_spark_session("Split_Raw_Clickstream_Spark", "2g")
        should_stop_spark = True

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
        for idx, file_path in enumerate(input_files):
            file_name = Path(file_path).name
            logger.info(f"Processing file {idx+1}/{len(input_files)}: {file_name}...")

            match = re.search(r"(\d{4})", file_name)
            file_year = match.group(1) if match else "2020"
            logger.info(f"Detected target year for filtering: {file_year}")

            raw_df = spark.read.csv(file_path, header=True, schema=schema)

            partitioned_df = raw_df \
                .withColumn("year", substring(col("event_time"), 1, 4)) \
                .withColumn("month", substring(col("event_time"), 6, 2)) \
                .withColumn("day", substring(col("event_time"), 9, 2)) \
                .filter(col("year") == file_year)

            logger.info(f"Writing partitions for {file_name} to staging...")
            partitioned_df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(out_dir)

            logger.info(f"Completed processing: {file_name}")

        logger.info("PySpark Clickstream splitting completed successfully.")

    finally:
        if should_stop_spark and spark is not None:
            spark.stop()
            logger.info("Spark Session stopped.")

def main() -> None:
    try:
        run_raw_prep()
    except Exception as e:
        logger.error(f"Spark processing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
