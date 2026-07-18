import os
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date, split

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.logger import get_logger
from utils.spark import get_spark_session
from utils.db import get_jdbc_config

logger = get_logger("bronze_to_silver")

def main():
    if len(sys.argv) < 2:
        logger.error("Missing run date argument YYYY-MM-DD")
        sys.exit(1)
        
    run_date = sys.argv[1] # e.g. "2020-01-01"
    year = run_date[:4]
    month = run_date[5:7]
    day = run_date[8:10]

    # Initialize Spark Session via shared utility
    logger.info(f"Initializing Spark Session for run date: {run_date}...")
    spark = get_spark_session(f"ECommerce_Bronze_To_Silver_{run_date}", "1536M")

    # Import Delta Table after Spark Session is created
    from delta.tables import DeltaTable

    # Input/Output paths
    INPUT_PATH = f"s3a://ecommerce-bronze/year={year}/month={month}/day={day}/*.parquet"
    SILVER_DELTA_PATH = "s3a://ecommerce-silver/ecommerce_events"
    QUARANTINE_PATH = "s3a://ecommerce-silver/quarantine"

    # Define exact schema for Bronze Clickstream
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
        # Read raw Parquet data from MinIO Bronze
        logger.info(f"Reading clickstream data from {INPUT_PATH}...")
        raw_df = spark.read.schema(schema).parquet(INPUT_PATH)

        # Parse event timestamps and date
        parsed_df = raw_df \
            .withColumn("event_time_parsed", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")) \
            .withColumn("event_date", to_date(col("event_time_parsed")))

        # 4. Quarantine: rows where critical fields are NULL
        na_df = parsed_df.filter(
            col("event_time_parsed").isNull() | 
            col("event_type").isNull() | 
            col("user_id").isNull() | 
            col("product_id").isNull() | 
            col("price").isNull()
        ).drop("event_time_parsed", "event_date")

        # Clean: rows where critical fields are valid
        clean_df = parsed_df \
            .filter(
                col("event_time_parsed").isNotNull() & 
                col("event_type").isNotNull() & 
                col("user_id").isNotNull() & 
                col("product_id").isNotNull() & 
                col("price").isNotNull()
            ) \
            .withColumn("event_time", col("event_time_parsed")) \
            .drop("event_time_parsed") \
            .withColumn("category", split(col("category_code"), r"\.").getItem(0)) \
            .withColumn("sub_category", split(col("category_code"), r"\.").getItem(1)) \
            .fillna({"brand": "unknown", "category": "unknown", "sub_category": "unknown"}) \
            .drop("category_code")

        # Connect and fetch user loyalty data from Neon Postgres CRM via JDBC
        logger.info("Fetching CRM user profiles from Neon Postgres via JDBC...")
        db_url, db_properties = get_jdbc_config()
        
        crm_df = spark.read.jdbc(
            url=db_url,
            table="crm.user_loyalty",
            properties=db_properties
        ).select("user_id", "loyalty_tier", "acquisition_channel")

        # Join Clickstream with CRM data
        logger.info("Performing LEFT JOIN with CRM User Loyalty data...")
        enriched_df = clean_df.join(crm_df, on="user_id", how="left")

        # Fill missing loyalty info with defaults
        enriched_df = enriched_df.fillna({
            "loyalty_tier": "Regular",
            "acquisition_channel": "Organic"
        })

        # Write Quarantine rows to MinIO Silver
        na_count = na_df.count()
        if na_count > 0:
            logger.warning(f"Writing {na_count} quarantined rows to {QUARANTINE_PATH}...")
            na_df.write.mode("append").parquet(QUARANTINE_PATH)

        # Deduplicate to prevent MultipleSourceRowMatchingTargetRow exception on MERGE
        enriched_df_clean = enriched_df.dropDuplicates(["user_session", "event_time", "product_id", "event_type"])

        # Write/Merge clean enriched data to MinIO Silver using Delta Lake
        logger.info(f"Writing Silver data to Delta Table at {SILVER_DELTA_PATH}...")
        
        if DeltaTable.isDeltaTable(spark, SILVER_DELTA_PATH):
            logger.info("    Delta table exists. Performing MERGE (Upsert)...")
            deltaTable = DeltaTable.forPath(spark, SILVER_DELTA_PATH)
            deltaTable.alias("target").merge(
                source = enriched_df_clean.alias("source"),
                condition = "target.user_session = source.user_session AND target.event_time = source.event_time AND target.product_id = source.product_id AND target.event_type = source.event_type"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            logger.info("    Delta table not found. Writing new partitioned Delta Table...")
            enriched_df_clean.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("event_date") \
                .save(SILVER_DELTA_PATH)

        logger.info("PySpark Bronze to Silver pipeline ran successfully!")

    except Exception as e:
        logger.error(f"PySpark pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session stopped.")

if __name__ == "__main__":
    main()
