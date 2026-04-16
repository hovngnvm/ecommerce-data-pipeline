"""
Bootstrap CRM Database in Neon PostgreSQL with Synthetic Customer Profiles.
Scans staging partition directories for unique user IDs, generates synthetic attributes,
and performs atomic staging swap into Neon PostgreSQL.
"""

import glob
import os
import random
from datetime import datetime, timedelta
import pandas as pd
from utils.config import (
    RAW_CLICKSTREAM_DIR,
    NEON_DB_HOST,
    NEON_DB_USER,
    NEON_DB_PASSWORD,
    NEON_DB_NAME,
    NEON_DB_PORT
)
from utils.db import get_db_connection
from utils.logger import get_logger

logger = get_logger("bootstrap_crm")

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

CHANNELS = ["Direct", "Organic Search", "Paid Search", "Facebook Ads", "Instagram Ads", "Email Referral", "Affiliate"]
CHANNEL_WEIGHTS = [0.25, 0.25, 0.15, 0.15, 0.10, 0.05, 0.05]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 6, 1)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def extract_unique_users_from_parquet() -> set[int]:
    """Scans all staging parquet partitions and extracts distinct user IDs."""
    logger.info("Scanning staging clickstream directories for unique user_ids...")
    all_files = glob.glob(os.path.join(RAW_CLICKSTREAM_DIR, "*", "*.parquet"))

    if not all_files:
        logger.warning(f"No parquet files found in {RAW_CLICKSTREAM_DIR}. Falling back to default ID range.")
        return set(range(1000, 2000))

    user_ids: set[int] = set()
    logger.info(f"Found {len(all_files)} parquet files. Scanning all files for complete user coverage...")

    for file_path in all_files:
        try:
            df = pd.read_parquet(file_path, columns=["user_id"])
            user_ids.update(df["user_id"].dropna().astype(int).tolist())
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")

    logger.info(f"Extracted {len(user_ids):,} unique user IDs from clickstream files.")
    return user_ids


def bootstrap_crm_table(batch_size: int = 10000) -> None:
    """Streams and inserts user loyalty profiles into stage table and performs atomic swap."""
    user_ids = sorted(list(extract_unique_users_from_parquet()))

    if not user_ids:
        logger.error("No user IDs extracted. Aborting bootstrap.")
        return

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

            # 2. Create stage table for zero-downtime atomic swap
            cur.execute("DROP TABLE IF EXISTS crm.user_loyalty_stage;")
            cur.execute("""
                CREATE TABLE crm.user_loyalty_stage (
                    user_id INT PRIMARY KEY,
                    loyalty_tier VARCHAR(20) NOT NULL,
                    signup_date DATE NOT NULL,
                    acquisition_channel VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

            # 3. Stream batches into stage table
            insert_sql = """
                INSERT INTO crm.user_loyalty_stage (user_id, loyalty_tier, signup_date, acquisition_channel)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING;
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

                cur.executemany(insert_sql, batch_records)
                conn.commit()
                logger.info(f"Inserted batch {i + len(batch_records):,}/{total_users:,} into stage table.")

            # 4. Atomic swap into production table
            logger.info("Performing atomic swap from stage table to production crm.user_loyalty...")
            cur.execute("TRUNCATE TABLE crm.user_loyalty;")
            cur.execute("INSERT INTO crm.user_loyalty SELECT * FROM crm.user_loyalty_stage;")
            cur.execute("DROP TABLE crm.user_loyalty_stage;")
            conn.commit()

    logger.info("CRM user loyalty profiles bootstrapped and swapped successfully!")


if __name__ == "__main__":
    bootstrap_crm_table()
