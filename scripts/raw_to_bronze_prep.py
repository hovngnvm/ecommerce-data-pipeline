import sys
import shutil
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, substring

from scripts.config.settings import settings
from scripts.utils.logger import get_logger
from scripts.utils.spark import get_spark_session

logger = get_logger(__name__)


def run_raw_prep(input_dir: str | None = None, output_dir: str | None = None) -> None:
    """
    Reads raw compressed CSV clickstream files, partitions them by year/month/day,
    and writes optimized partitioned Parquet files to the staging directory.
    """
    src_dir = Path(input_dir or settings.landing_dir)
    out_dir = output_dir or settings.staging_dir

    logger.info(f"Source directory for raw files: {src_dir}")
    logger.info(f"Output directory: {out_dir}")

    input_files = sorted(src_dir.glob("*.csv.gz"))
    if not input_files:
        logger.error(f"No raw *.csv.gz files found at {src_dir}")
        raise FileNotFoundError(f"No raw files found at {src_dir}")

    logger.info(f"Found {len(input_files)} raw files to process:")
    for f in input_files:
        logger.info(f"    - {f.name}")

    if Path(out_dir).exists():
        logger.info(f"Clearing existing staging directory at {out_dir}...")
        shutil.rmtree(out_dir)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    logger.info("Initializing Spark Session...")
    spark = get_spark_session("Split_Raw_Clickstream_Spark", "2g")

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
        logger.info(f"Reading {len(input_files)} raw files into Spark DataFrame...")
        raw_df = spark.read.csv([str(p) for p in input_files], header=True, schema=schema)

        partitioned_df = raw_df \
            .withColumn("year", substring(col("event_time"), 1, 4)) \
            .withColumn("month", substring(col("event_time"), 6, 2)) \
            .withColumn("day", substring(col("event_time"), 9, 2))

        logger.info(f"Writing partitioned Parquet files to {out_dir}...")
        partitioned_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(out_dir)

        logger.info("PySpark Clickstream splitting completed successfully.")

    finally:
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

