import os
import sys
import glob
import random
import datetime
import pandas as pd
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.config import STAGING_DIR
from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger("bootstrap_crm_database")

def main():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. Create schema and table
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

                # 2. Extract unique User IDs dynamically across all available year/month folders
                logger.info("Scanning user IDs dynamically across staging directories...")
                staging_dir = STAGING_DIR
                
                # Find all year=*/month=* directories
                month_dirs = sorted(glob.glob(os.path.join(staging_dir, "year=*", "month=*")))
                parquet_files = []
                years_months = []
                for m_dir in month_dirs:
                    # Extract year and month
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
                    sys.exit(1)

                logger.info(f"Reading {len(parquet_files)} files for user extraction...")
                unique_users = set()
                for f in parquet_files:
                    table = pq.read_table(f, columns=["user_id"])
                    user_ids = table.column("user_id").to_numpy()
                    user_ids_clean = user_ids[~pd.isnull(user_ids)]
                    unique_users.update(user_ids_clean.astype(int))

                total_users = len(unique_users)
                logger.info(f"Found {total_users:,} unique users across sample files.")
                
                # Query logical database size via SQL
                cursor.execute("SELECT pg_database_size(current_database());")
                db_size_bytes = cursor.fetchone()[0]
                logger.info(f"Current logical database size: {db_size_bytes / (1024*1024):.2f} MB")
                
                # Check storage quota (Default to Neon Free Plan: 512MB)
                quota_bytes = 512 * 1024 * 1024
                
                # Neon API check (if configured)
                api_key = os.getenv("NEON_API_KEY")
                project_id = os.getenv("NEON_PROJECT_ID")
                if api_key and project_id:
                    try:
                        import requests
                        headers = {
                            "Authorization": f"Bearer {api_key}",
                            "Accept": "application/json"
                        }
                        url = f"https://console.neon.tech/api/v2/projects/{project_id}"
                        r = requests.get(url, headers=headers, timeout=10)
                        if r.status_code == 200:
                            data = r.json()
                            quota_bytes = data.get("project", {}).get("quota", {}).get("storage_bytes", quota_bytes)
                            logger.info(f"Neon API: Retrieved project storage quota limit: {quota_bytes / (1024*1024):.2f} MB")
                    except Exception as e:
                        logger.warning(f"Failed to query Neon API: {e}. Falling back to default size estimation.")

                # Calculate usable space (with a 50MB safety buffer)
                safety_buffer = 50 * 1024 * 1024
                available_bytes = quota_bytes - db_size_bytes
                usable_bytes = available_bytes - safety_buffer
                
                # Estimate average row size for crm.user_loyalty (including B-Tree primary key index)
                bytes_per_row = 120
                
                if usable_bytes <= 0:
                    logger.warning("Neon Postgres database is close to or exceeding the storage quota limit!")
                    logger.info("Setting minimum seeding size to 1,000 records to prevent failure.")
                    max_rows = 1000
                else:
                    max_rows = int(usable_bytes // bytes_per_row)
                    
                logger.info(f"Calculated maximum safe CRM rows to seed: {max_rows:,}")
                
                # Perform dynamic downsampling based on capacity
                if total_users > max_rows:
                    logger.info(f"Dynamically downsampling unique users from {total_users:,} to {max_rows:,} to fit Neon storage capacity.")
                    unique_users = set(random.sample(list(unique_users), max_rows))
                    total_users = len(unique_users)
                else:
                    logger.info(f"Neon has ample space. Seeding the entire extracted user base ({total_users:,} users).")

                # Generate and batch insert CRM records
                logger.info("Generating CRM Loyalty data and seeding Neon Postgres...")
                
                cursor.execute("TRUNCATE TABLE crm.user_loyalty;")
                conn.commit()

                tiers = ['VIP', 'Gold', 'Silver', 'Regular']
                tier_weights = [0.05, 0.15, 0.30, 0.50]
                channels = ['Google', 'Facebook', 'Organic', 'TikTok', 'Instagram', 'Referral']
                
                start_date = datetime.date(2018, 1, 1)
                # Dynamically set end_date to the last day of the maximum year/month found
                if years_months:
                    max_y, max_m = max(years_months)
                    if max_m == 12:
                        end_date = datetime.date(max_y + 1, 1, 1) - datetime.timedelta(days=1)
                    else:
                        end_date = datetime.date(max_y, max_m + 1, 1) - datetime.timedelta(days=1)
                else:
                    end_date = datetime.date(2020, 2, 29) # Fallback to Feb 2020

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

                # Batch insert using execute_values
                logger.info("Loading records in batches of 10,000...")
                insert_query = """
                    INSERT INTO crm.user_loyalty (user_id, loyalty_tier, signup_date, acquisition_channel)
                    VALUES %s
                    ON CONFLICT (user_id) DO NOTHING;
                """
                
                batch_size = 10000
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    execute_values(cursor, insert_query, batch)
                    conn.commit()
                    if (i // batch_size) % 10 == 0 or i + batch_size >= len(records):
                        logger.info(f"    Inserted {min(i + batch_size, len(records)):,} / {len(records):,} records...")

                logger.info("Seeding completed successfully.")

    except Exception as e:
        logger.error(f"Database seeding failed: {e}")
        sys.exit(1)
    finally:
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
