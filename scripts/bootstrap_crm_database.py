"""
Bootstrap CRM Database in Neon PostgreSQL with Synthetic Customer Profiles.
Scans staging partition directories for unique user IDs, generates synthetic attributes,
and performs atomic staging swap into Neon PostgreSQL.
"""

import random
from datetime import datetime, timedelta
from pathlib import Path
import duckdb

from scripts.config.settings import settings
from scripts.utils.db import get_db_connection
from scripts.utils.logger import get_logger

logger = get_logger(__name__)

LOYALTY_TIERS = ["Member", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

CHANNELS = ["Direct", "Organic Search", "Paid Search", "Facebook Ads", "Instagram Ads", "Email Referral", "Affiliate"]
CHANNEL_WEIGHTS = [0.25, 0.25, 0.15, 0.15, 0.10, 0.05, 0.05]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 6, 1)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def extract_unique_users_from_parquet() -> set[int]:
    """Scans all staging parquet partitions and extracts distinct user IDs via DuckDB."""
    staging_path = Path(settings.staging_dir)
    logger.info(f"Scanning staging clickstream directories ({staging_path}) for unique user_ids...")

    if not next(iter(staging_path.rglob("*.parquet")), None):
        logger.error(f"No parquet files found in {staging_path}. Aborting bootstrap.")
        raise FileNotFoundError(f"No clickstream parquet files found in {staging_path}")


    try:
        glob_path = (staging_path / "**" / "*.parquet").as_posix()
        rows = duckdb.query(f"SELECT DISTINCT user_id FROM read_parquet('{glob_path}') WHERE user_id IS NOT NULL").fetchall()
        user_ids = {int(r[0]) for r in rows}
    except Exception as e:
        logger.error(f"Failed to read parquet files: {e}")
        raise RuntimeError(f"Corrupt or unreadable parquet file: {e}") from e


    logger.info(f"Extracted {len(user_ids):,} unique user IDs from clickstream files.")
    return user_ids



def bootstrap_crm_table(batch_size: int = 10000) -> None:
    """Streams and upserts user loyalty profiles into PostgreSQL."""
    user_ids = sorted(extract_unique_users_from_parquet())


    if not user_ids:
        logger.error("No user IDs extracted. Aborting bootstrap.")
        raise ValueError("No user IDs extracted from clickstream files.")

    total_users = len(user_ids)
    logger.info(f"Starting CRM bootstrap for {total_users:,} users into PostgreSQL...")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1. Create schema and target table if not exist
            cur.execute("CREATE SCHEMA IF NOT EXISTS crm;")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crm.user_loyalty (
                    user_id INT PRIMARY KEY,
                    loyalty_tier VARCHAR(20) NOT NULL,
                    signup_date DATE NOT NULL,
                    acquisition_channel VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

            # 2. Upsert batches into target table safely
            upsert_sql = """
                INSERT INTO crm.user_loyalty (user_id, loyalty_tier, signup_date, acquisition_channel)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    loyalty_tier = EXCLUDED.loyalty_tier,
                    signup_date = EXCLUDED.signup_date,
                    acquisition_channel = EXCLUDED.acquisition_channel;
            """

            random.seed(42)
            for i in range(0, total_users, batch_size):
                batch_user_ids = user_ids[i:i + batch_size]
                batch_records = []
                for uid in batch_user_ids:
                    tier = random.choices(LOYALTY_TIERS, weights=LOYALTY_WEIGHTS)[0]
                    random_days = random.randint(0, DATE_RANGE_DAYS)
                    signup_date = (START_DATE + timedelta(days=random_days)).date()
                    channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
                    batch_records.append((uid, tier, signup_date, channel))

                cur.executemany(upsert_sql, batch_records)
                conn.commit()
                logger.info(f"Upserted batch {i + len(batch_records):,}/{total_users:,} into crm.user_loyalty.")

    logger.info("CRM user loyalty profiles bootstrapped and upserted successfully!")


if __name__ == "__main__":
    bootstrap_crm_table()
