import os
import sys
from dotenv import load_dotenv

# Base paths calculation
UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(UTILS_DIR)
PROJECT_ROOT = os.path.dirname(SCRIPTS_DIR)
DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")

if os.path.exists(DOTENV_PATH):
    load_dotenv(DOTENV_PATH)

# Validate essential Neon database credentials
REQUIRED_VARS = ["NEON_DB_HOST", "NEON_DB_USER", "NEON_DB_PASSWORD", "NEON_DB_NAME"]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]

if missing_vars:
    print(f"[!] Error: Missing required environment variables in .env: {', '.join(missing_vars)}", file=sys.stderr)
    sys.exit(1)

# DB Credentials
NEON_DB_HOST = os.getenv("NEON_DB_HOST")
NEON_DB_USER = os.getenv("NEON_DB_USER")
NEON_DB_PASSWORD = os.getenv("NEON_DB_PASSWORD")
NEON_DB_NAME = os.getenv("NEON_DB_NAME")
NEON_DB_PORT = os.getenv("NEON_DB_PORT", "5432")

# MinIO Credentials
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Path variables
LANDING_DIR = os.path.join(PROJECT_ROOT, "data", "landing")
STAGING_DIR = os.path.join(PROJECT_ROOT, "data", "staging")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)
