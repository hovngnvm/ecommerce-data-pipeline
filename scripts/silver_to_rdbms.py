import os
import sys
from pyspark.sql.functions import col

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.logger import get_logger
from utils.db import get_db_connection, get_jdbc_config
from utils.spark import get_spark_session

logger = get_logger("silver_to_rdbms")

def main():
    if len(sys.argv) < 2:
        logger.error("Missing run date argument YYYY-MM-DD")
        sys.exit(1)
        
    run_date = sys.argv[1] # e.g. "2020-01-01"

    # 1. Create target schema/table and ensure idempotency (Delete-Insert pattern)
    logger.info(f"Ensuring schema and table exist, and deleting existing rows for {run_date} on Neon...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create schema and table
                cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS silver.ecommerce_events (
                        user_id INT,
                        event_type VARCHAR(20),
                        product_id INT,
                        category VARCHAR(100),
                        sub_category VARCHAR(100),
                        brand VARCHAR(100),
                        price DOUBLE PRECISION,
                        user_session VARCHAR(50),
                        event_time TIMESTAMP,
                        loyalty_tier VARCHAR(20),
                        acquisition_channel VARCHAR(50)
                    );
                """)
                conn.commit()

                # Delete existing data for that day to prevent duplicates on rerun
                cursor.execute("DELETE FROM silver.ecommerce_events WHERE event_time::date = %s", (run_date,))
                deleted_count = cursor.rowcount
                conn.commit()
                logger.info(f"Schema checked. Deleted {deleted_count} pre-existing records for date {run_date}.")
                
    except Exception as db_err:
        logger.error(f"Database prep failed: {db_err}")
        sys.exit(1)

    # Initialize Spark Session via shared utility
    logger.info(f"Initializing Spark Session for run date: {run_date}...")
    spark = get_spark_session(f"ECommerce_Silver_To_RDBMS_{run_date}", "1G")

    SILVER_DELTA_PATH = "s3a://ecommerce-silver/ecommerce_events"

    try:
        # Read enriched clickstream data from Delta Lake Silver
        logger.info(f"Reading Delta Lake data from {SILVER_DELTA_PATH}...")
        silver_df = spark.read.format("delta").load(SILVER_DELTA_PATH)

        # Apply Filter & Incremental check
        # - Filter by target run_date
        # - Filter OUT 'view' events (reduces storage footprint by ~90% for Neon compatibility)
        logger.info(f"Filtering data for date: {run_date} (excluding 'view' events)...")
        filtered_df = silver_df \
            .filter((col("event_date") == run_date) & (col("event_type") != "view")) \
            .select(
                "user_id",
                "event_type",
                "product_id",
                "category",
                "sub_category",
                "brand",
                "price",
                "user_session",
                "event_time",
                "loyalty_tier",
                "acquisition_channel"
            )

        row_count = filtered_df.count()
        logger.info(f"Found {row_count:,} events (cart/purchase) to write to Neon.")

        # Write to Neon Postgres using Spark JDBC
        if row_count > 0:
            logger.info("Writing records to Neon Postgres 'silver.ecommerce_events' via JDBC...")
            db_url, db_properties = get_jdbc_config()

            filtered_df.write.jdbc(
                url=db_url,
                table="silver.ecommerce_events",
                mode="append",
                properties=db_properties
            )
            logger.info("Write completed successfully.")
        else:
            logger.info("No records to write for this date.")

        logger.info("PySpark Silver to RDBMS pipeline ran successfully!")

    except Exception as e:
        logger.error(f"PySpark load failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
