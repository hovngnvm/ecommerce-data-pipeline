import sys
import duckdb
import pandas as pd

from scripts.config.settings import settings
from scripts.utils.logger import get_logger
from scripts.utils.db import get_db_connection

logger = get_logger(__name__)

def run_silver_to_olap(run_date: str | None = None, target_duckdb_path: str | None = None) -> int:
    """
    Ingests cleaned Silver event data and CRM loyalty profiles into DuckDB OLAP Data Warehouse.
    Supports both full-sync and daily partition incremental loading.
    """
    db_path = target_duckdb_path or settings.duckdb_path
    logger.info(f"Connecting to DuckDB Data Warehouse at: {db_path}")
    con = duckdb.connect(db_path)

    try:
        logger.info("Fetching CRM users from Neon Postgres...")
        with get_db_connection() as conn:
            crm_df = pd.read_sql("SELECT user_id, loyalty_tier, signup_date, acquisition_channel FROM crm.user_loyalty;", conn)
            logger.info(f"Fetched {len(crm_df):,} CRM user records from Neon.")

        con.register("crm_users_df", crm_df)

        logger.info("Configuring DuckDB S3 credentials for Silver MinIO Lake access...")
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("INSTALL delta; LOAD delta;")
            endpoint_clean = settings.minio_endpoint.replace('http://', '').replace('https://', '')
            con.execute(f"SET s3_endpoint='{endpoint_clean}';")
            con.execute(f"SET s3_access_key_id='{settings.minio_access_key}';")
            con.execute(f"SET s3_secret_access_key='{settings.minio_secret_key}';")
            con.execute("SET s3_use_ssl=false;")
            con.execute("SET s3_url_style='path';")
            delta_source = settings.silver_delta_path.replace("s3a://", "s3://")
            logger.info(f"Using S3 Silver Delta Lake as source: {delta_source}")
        except Exception as s3_err:
            logger.error(f"Could not configure DuckDB S3/Delta extension: {s3_err}")
            raise RuntimeError(f"Cannot access Silver Lake via MinIO S3: {s3_err}") from s3_err

        con.execute("BEGIN TRANSACTION;")
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS crm;")
            con.execute("CREATE OR REPLACE TABLE crm.user_loyalty AS SELECT user_id, loyalty_tier, signup_date, acquisition_channel FROM crm_users_df;")


            con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
            con.execute("""
                CREATE TABLE IF NOT EXISTS silver.ecommerce_events (
                    user_id INTEGER,
                    event_type VARCHAR,
                    product_id INTEGER,
                    category VARCHAR,
                    sub_category VARCHAR,
                    brand VARCHAR,
                    price DOUBLE,
                    user_session VARCHAR,
                    event_time TIMESTAMP,
                    loyalty_tier VARCHAR,
                    acquisition_channel VARCHAR
                );
            """)

            if run_date:
                logger.info(f"Deleting pre-existing records for date {run_date} in DuckDB...")
                con.execute("DELETE FROM silver.ecommerce_events WHERE CAST(event_time AS DATE) = CAST(? AS DATE);", [run_date])
                date_filter_sql = f"AND CAST(p.event_time AS DATE) = CAST('{run_date}' AS DATE)"
            else:
                logger.info("Clearing silver.ecommerce_events for full sync...")
                con.execute("DELETE FROM silver.ecommerce_events;")
                date_filter_sql = ""

            logger.info(f"Reading Silver Event logs from {delta_source} and joining with CRM data...")
            query = f"""
                INSERT INTO silver.ecommerce_events
                SELECT
                    TRY_CAST(p.user_id AS INTEGER) as user_id,
                    p.event_type,
                    TRY_CAST(p.product_id AS INTEGER) as product_id,
                    COALESCE(p.category, 'unknown') as category,
                    COALESCE(p.sub_category, 'unknown') as sub_category,
                    COALESCE(p.brand, 'unknown') as brand,
                    TRY_CAST(p.price AS DOUBLE) as price,
                    p.user_session,
                    TRY_CAST(p.event_time AS TIMESTAMP) as event_time,
                    COALESCE(l.loyalty_tier, 'Member') as loyalty_tier,
                    COALESCE(l.acquisition_channel, 'Organic') as acquisition_channel
                FROM delta_scan('{delta_source}') p
                LEFT JOIN crm_users_df l ON TRY_CAST(p.user_id AS INTEGER) = l.user_id
                WHERE p.event_type != 'view'
                  AND p.event_time IS NOT NULL
                  AND p.user_id IS NOT NULL
                  {date_filter_sql};
            """
            con.execute(query)
            con.execute("COMMIT;")
        except Exception as tx_err:
            con.execute("ROLLBACK;")
            logger.error(f"Transaction failed and was rolled back: {tx_err}")
            raise RuntimeError(f"Silver to DuckDB sync failed: {tx_err}") from tx_err

        con.unregister("crm_users_df")

        total_records = con.execute("SELECT COUNT(*) FROM silver.ecommerce_events;").fetchone()[0]
        logger.info(f"Successfully synced DuckDB 'silver.ecommerce_events'. Total records: {total_records:,}")
        return total_records

    finally:
        con.close()

def main() -> None:
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        run_silver_to_olap(run_date)
        logger.info("Silver to OLAP sync completed successfully.")
    except Exception as err:
        logger.error(f"Silver to DuckDB sync failed: {err}")
        sys.exit(1)

if __name__ == "__main__":
    main()
