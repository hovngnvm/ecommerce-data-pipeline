import os
import sys
import glob
import boto3
from botocore.client import Config

# Ensure scripts directory is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from utils.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, STAGING_DIR, BRONZE_BUCKET
from utils.logger import get_logger

logger = get_logger("upload_to_bronze")

def run_upload(run_date: str) -> None:
    """
    Uploads daily partitioned Parquet files from local staging directory
    to the Bronze S3/MinIO bucket.
    """
    if not run_date:
        raise ValueError("Missing run date argument YYYY-MM-DD")

    year = run_date[:4]
    month = run_date[5:7]
    day = run_date[8:10]

    s3 = boto3.resource(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    bucket = s3.Bucket(BRONZE_BUCKET)

    src_dir = os.path.join(STAGING_DIR, f"year={year}", f"month={month}", f"day={day}")
    search_pattern = os.path.join(src_dir, "*.parquet")
    files = glob.glob(search_pattern)

    if not files:
        logger.error(f"No staging files found at {src_dir} for date {run_date}")
        raise FileNotFoundError(f"No staging files found at {src_dir} for date {run_date}")

    logger.info(f"Ingesting {len(files)} files to {BRONZE_BUCKET} for date {run_date}...")
    for f in files:
        file_name = os.path.basename(f)
        s3_key = f"year={year}/month={month}/day={day}/{file_name}"
        logger.info(f"    - Uploading {file_name} to s3a://{BRONZE_BUCKET}/{s3_key}...")
        bucket.upload_file(f, s3_key)

    logger.info(f"Ingestion to {BRONZE_BUCKET} completed successfully.")

def main() -> None:
    if len(sys.argv) < 2:
        logger.error("Usage: python upload_to_bronze.py YYYY-MM-DD")
        sys.exit(1)

    run_date = sys.argv[1]
    try:
        run_upload(run_date)
    except Exception as e:
        logger.error(f"Upload to Bronze failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
