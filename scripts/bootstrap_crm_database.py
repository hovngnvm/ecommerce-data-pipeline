import os
import sys
import glob
import random
import datetime
import pandas as pd
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.config import STAGING_DIR
from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger("bootstrap_crm_database")

def seed_crm_database(staging_dir: str | None = None, batch_size: int = 10000) -> int:
    """
    Extracts unique user IDs from staging Parquet files and generates synthetic
    CRM loyalty profiles to seed Neon Postgres.
    """
    stg_dir = staging_dir or STAGING_DIR

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            logger.info("Creating schema 'crm' and table 'user_loyalty' on Neon Postgres...")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS crm;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crm.user_loyalty (
                    user_id INT PRIMARY KEY,
                    loyalty_tier VARCHAR(20) NOT NULL,
                    signup_date DATE NOT NULL,
                    acquisition_channel VARCHAR(50) NOT NULL
                );
            """)
            conn.commit()
            logger.info("Schema and table verified.")

            logger.info("Scanning user IDs dynamically across staging directories...")
            month_dirs = sorted(glob.glob(os.path.join(stg_dir, "year=*", "month=*")))
            parquet_files = []
            years_months = []
            for m_dir in month_dirs:
                parts = m_dir.replace("\\", "/").split("/")
                year_part = [p for p in parts if p.startswith("year=")]
                month_part = [p for p in parts if p.startswith("month=")]
                if year_part and month_part:
                    y = int(year_part[0].split("=")[1])
                    m = int(month_part[0].split("=")[1])
                    years_months.append((y, m))
                
                m_files = sorted(glob.glob(os.path.join(m_dir, "**", "*.parquet"), recursive=True))[:5]
                parquet_files.extend(m_files)

            if not parquet_files:
                logger.error("No staging Parquet files found. Run raw_to_bronze_prep.py first.")
                raise FileNotFoundError("No staging Parquet files found.")

            logger.info(f"Reading {len(parquet_files)} files for user extraction...")
            unique_users = set()
            for f in parquet_files:
                table = pq.read_table(f, columns=["user_id"])
                user_ids = table.column("user_id").to_numpy()
                user_ids_clean = user_ids[~pd.isnull(user_ids)]
                unique_users.update(user_ids_clean.astype(int))

            total_users = len(unique_users)
            logger.info(f"Found {total_users:,} unique users across sample files.")

            logger.info("Generating CRM Loyalty data and seeding Neon Postgres...")
            cursor.execute("TRUNCATE TABLE crm.user_loyalty;")
            conn.commit()

            tiers = ['VIP', 'Gold', 'Silver', 'Regular']
            tier_weights = [0.05, 0.15, 0.30, 0.50]
            channels = ['Google', 'Facebook', 'Organic', 'TikTok', 'Instagram', 'Referral']
            
            start_date = datetime.date(2018, 1, 1)
            if years_months:
                max_y, max_m = max(years_months)
                if max_m == 12:
                    end_date = datetime.date(max_y + 1, 1, 1) - datetime.timedelta(days=1)
                else:
                    end_date = datetime.date(max_y, max_m + 1, 1) - datetime.timedelta(days=1)
            else:
                end_date = datetime.date(2020, 2, 29)

            logger.info(f"Simulating CRM user signup dates between {start_date} and {end_date}...")
            time_between = end_date - start_date
            days_between = time_between.days

            records = []
            for uid in unique_users:
                tier = random.choices(tiers, weights=tier_weights)[0]
                channel = random.choice(channels)
                random_days = random.randrange(days_between)
                signup_date = start_date + datetime.timedelta(days=random_days)
                records.append((int(uid), tier, signup_date, channel))

            logger.info(f"Loading records in batches of {batch_size:,}...")
            insert_query = """
                INSERT INTO crm.user_loyalty (user_id, loyalty_tier, signup_date, acquisition_channel)
                VALUES %s
                ON CONFLICT (user_id) DO NOTHING;
            """
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                execute_values(cursor, insert_query, batch)
                conn.commit()
                if (i // batch_size) % 10 == 0 or i + batch_size >= len(records):
                    logger.info(f"    Inserted {min(i + batch_size, len(records)):,} / {len(records):,} records...")

            logger.info("Seeding completed successfully.")
            return total_users

def main() -> None:
    try:
        seed_crm_database()
    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
