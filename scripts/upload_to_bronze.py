import sys
from pathlib import Path
import boto3
from botocore.client import Config

from scripts.config.settings import settings
from scripts.utils.logger import get_logger


logger = get_logger(__name__)

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
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    bucket = s3.Bucket(settings.minio_bronze_bucket)

    src_dir = Path(settings.staging_dir) / f"year={year}" / f"month={month}" / f"day={day}"
    files = sorted(src_dir.glob("*.parquet"))

    if not files:
        logger.error(f"No staging files found at {src_dir} for date {run_date}")
        raise FileNotFoundError(f"No staging files found at {src_dir} for date {run_date}")

    logger.info(f"Ingesting {len(files)} files to {settings.minio_bronze_bucket} for date {run_date}...")
    for f in files:
        s3_key = f"year={year}/month={month}/day={day}/{f.name}"
        logger.info(f"    - Uploading {f.name} to s3a://{settings.minio_bronze_bucket}/{s3_key}...")
        bucket.upload_file(str(f), s3_key)


    logger.info(f"Ingestion to {settings.minio_bronze_bucket} completed successfully.")

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
