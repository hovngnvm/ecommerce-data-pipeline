import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date, split
from delta.tables import DeltaTable

from scripts.config.settings import settings
from scripts.utils.logger import get_logger
from scripts.utils.spark import get_spark_session

logger = get_logger(__name__)


def run_bronze_to_silver(run_date: str) -> None:
    """
    Cleans Bronze clickstream Parquet, quarantines invalid rows,
    enriches with Neon Postgres CRM loyalty data, and upserts into Silver Delta Table.
    """
    if not run_date:
        raise ValueError("Missing run date argument YYYY-MM-DD")

    year = run_date[:4]
    month = run_date[5:7]
    day = run_date[8:10]

    input_path = f"s3a://{settings.minio_bronze_bucket}/year={year}/month={month}/day={day}/*.parquet"

    logger.info(f"Initializing Spark Session for run date: {run_date}...")
    spark = get_spark_session(f"ECommerce_Bronze_To_Silver_{run_date}", "1536M")

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
        logger.info(f"Reading clickstream data from {input_path}...")
        raw_df = spark.read.schema(schema).parquet(input_path)

        parsed_df = raw_df \
            .withColumn("event_time_parsed", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss z")) \
            .withColumn("event_date", to_date(col("event_time_parsed")))

        na_df = parsed_df.filter(
            col("event_time_parsed").isNull() |
            col("event_type").isNull() |
            col("user_id").isNull() |
            col("product_id").isNull() |
            col("price").isNull()
        ).drop("event_time_parsed", "event_date")

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

        logger.info("Fetching CRM user profiles from Neon Postgres via JDBC...")
        db_url = f"jdbc:postgresql://{settings.neon_db_host}:{settings.neon_db_port}/{settings.neon_db_name}"
        db_properties = {
            "user": settings.neon_db_user,
            "password": settings.neon_db_password,
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": "require"
        }

        crm_df = spark.read.jdbc(
            url=db_url,
            table="crm.user_loyalty",
            properties=db_properties
        ).select("user_id", "loyalty_tier", "acquisition_channel")

        logger.info("Performing LEFT JOIN with CRM User Loyalty data...")
        enriched_df = clean_df.join(crm_df, on="user_id", how="left")

        enriched_df = enriched_df.fillna({
            "loyalty_tier": "Member",
            "acquisition_channel": "Organic"
        })

        na_count = na_df.count()
        if na_count > 0:
            logger.warning(f"Writing {na_count} quarantined rows to {settings.quarantine_path}...")
            na_df.write.mode("append").parquet(settings.quarantine_path)

        # Deduplicate to prevent MultipleSourceRowMatchingTargetRow exception on MERGE
        enriched_df_clean = enriched_df.dropDuplicates(["user_session", "event_time", "product_id", "event_type"])

        logger.info(f"Writing Silver data to Delta Table at {settings.silver_delta_path}...")

        if DeltaTable.isDeltaTable(spark, settings.silver_delta_path):
            logger.info("Delta table exists. Performing MERGE (Upsert)...")
            delta_table = DeltaTable.forPath(spark, settings.silver_delta_path)
            delta_table.alias("target").merge(
                source=enriched_df_clean.alias("source"),
                condition="target.user_session = source.user_session AND target.event_time = source.event_time AND target.product_id = source.product_id AND target.event_type = source.event_type"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            logger.info("Delta table not found. Writing new partitioned Delta Table...")
            enriched_df_clean.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("event_date") \
                .save(settings.silver_delta_path)

        logger.info("PySpark Bronze to Silver pipeline ran successfully!")

    finally:
        spark.stop()
        logger.info("Spark Session stopped.")


def main() -> None:
    if len(sys.argv) < 2:
        logger.error("Usage: python bronze_to_silver.py YYYY-MM-DD")
        sys.exit(1)

    run_date = sys.argv[1]
    try:
        run_bronze_to_silver(run_date)
    except Exception as e:
        logger.error(f"PySpark pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

