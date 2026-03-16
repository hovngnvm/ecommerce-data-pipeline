import os
import sys
import socket
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOTENV_PATH = PROJECT_ROOT / ".env"

if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)

REQUIRED_VARS = ["NEON_DB_HOST", "NEON_DB_USER", "NEON_DB_PASSWORD", "NEON_DB_NAME"]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]

if missing_vars:
    print(f"Warning: Missing environment variables: {', '.join(missing_vars)}. Using default fallbacks.", file=sys.stderr)

NEON_DB_HOST = os.getenv("NEON_DB_HOST", "localhost")
NEON_DB_USER = os.getenv("NEON_DB_USER", "postgres")
NEON_DB_PASSWORD = os.getenv("NEON_DB_PASSWORD", "postgres")
NEON_DB_NAME = os.getenv("NEON_DB_NAME", "ecommerce_crm")
NEON_DB_PORT = os.getenv("NEON_DB_PORT", "5432")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
if "minio:" in MINIO_ENDPOINT:
    try:
        socket.gethostbyname("minio")
    except socket.gaierror:
        MINIO_ENDPOINT = MINIO_ENDPOINT.replace("minio:", "127.0.0.1:")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = "ecommerce-bronze"
SILVER_DELTA_PATH = "s3a://ecommerce-silver/ecommerce_events"
QUARANTINE_PATH = "s3a://ecommerce-silver/quarantine"

LANDING_DIR = str(PROJECT_ROOT / "data" / "landing")
STAGING_DIR = str(PROJECT_ROOT / "data" / "staging")
LOGS_DIR = str(PROJECT_ROOT / "logs")
DUCKDB_PATH = str(PROJECT_ROOT / "data" / "gold_warehouse.duckdb")

# Auto-bootstrap runtime directories
for directory in (PROJECT_ROOT / "data" / "landing", PROJECT_ROOT / "data" / "staging", PROJECT_ROOT / "logs"):
    directory.mkdir(parents=True, exist_ok=True)
