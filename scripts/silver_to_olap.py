import sys
from pathlib import Path
import duckdb
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from utils.config import (
    DUCKDB_PATH,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    STAGING_DIR
)
from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger(__name__)

def run_silver_to_olap(run_date: str | None = None, target_duckdb_path: str | None = None) -> int:
    """
    Ingests cleaned Silver event data and CRM loyalty profiles into DuckDB OLAP Data Warehouse.
    Supports both full-sync and daily partition incremental loading.
    """
    db_path = target_duckdb_path or DUCKDB_PATH
    logger.info(f"Connecting to DuckDB Data Warehouse at: {db_path}")
    con = duckdb.connect(db_path)

    try:
        logger.info("Fetching CRM users from Neon Postgres...")
        crm_df = None
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, loyalty_tier, signup_date, acquisition_channel FROM crm.user_loyalty;")
                crm_rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                crm_df = pd.DataFrame(crm_rows, columns=cols)
                logger.info(f"Fetched {len(crm_df):,} CRM user records from Neon.")

        con.register("crm_users_df", crm_df)
        con.execute("CREATE SCHEMA IF NOT EXISTS crm;")
        con.execute("DROP TABLE IF EXISTS crm.user_loyalty;")
        con.execute("CREATE TABLE crm.user_loyalty (user_id INTEGER PRIMARY KEY, loyalty_tier VARCHAR, signup_date DATE, acquisition_channel VARCHAR);")
        con.execute("INSERT INTO crm.user_loyalty SELECT * FROM crm_users_df;")

        logger.info("Ensuring schema 'silver' and table 'silver.ecommerce_events' exist in DuckDB...")
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
            logger.info("Truncating silver.ecommerce_events for full sync...")
            con.execute("TRUNCATE TABLE silver.ecommerce_events;")
            date_filter_sql = ""

        logger.info("Configuring DuckDB S3 credentials for Silver MinIO Lake access...")
        try:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            endpoint_clean = MINIO_ENDPOINT.replace('http://', '').replace('https://', '')
            con.execute(f"SET s3_endpoint='{endpoint_clean}';")
            con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
            con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
            con.execute("SET s3_use_ssl=false;")
            con.execute("SET s3_url_style='path';")
            parquet_source = "s3://ecommerce-silver/**/*.parquet"
            logger.info("Using S3 Silver Delta/Parquet lake as source: s3://ecommerce-silver/")
        except Exception as s3_err:
            logger.error(f"Could not configure DuckDB S3 extension: {s3_err}")
            raise RuntimeError(f"Cannot access Silver Lake via MinIO S3: {s3_err}") from s3_err

        logger.info(f"Reading Silver Event logs from {parquet_source} and joining with CRM data...")
        query = f"""
            INSERT INTO silver.ecommerce_events
            SELECT
                TRY_CAST(p.user_id AS INTEGER) as user_id,
                p.event_type,
                TRY_CAST(p.product_id AS INTEGER) as product_id,
                COALESCE(split_part(p.category_code, '.', 1), 'unknown') as category,
                COALESCE(split_part(p.category_code, '.', 2), 'unknown') as sub_category,
                COALESCE(p.brand, 'unknown') as brand,
                TRY_CAST(p.price AS DOUBLE) as price,
                p.user_session,
                TRY_CAST(p.event_time AS TIMESTAMP) as event_time,
                COALESCE(l.loyalty_tier, 'Regular') as loyalty_tier,
                COALESCE(l.acquisition_channel, 'Organic') as acquisition_channel
            FROM read_parquet('{parquet_source}') p
            LEFT JOIN crm_users_df l ON TRY_CAST(p.user_id AS INTEGER) = l.user_id
            WHERE p.event_type != 'view'
              AND p.event_time IS NOT NULL
              AND p.user_id IS NOT NULL
              {date_filter_sql};
        """
        try:
            con.execute(query)
        except Exception as query_err:
            logger.error(f"Failed to read from Silver Lake ({parquet_source}): {query_err}")
            raise RuntimeError(f"Cannot access Silver Lake ({parquet_source}): {query_err}") from query_err

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
