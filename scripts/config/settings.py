import socket
from functools import lru_cache
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """Centralized System Settings loaded from environment variables."""

    # Database Settings (Neon PostgreSQL)
    neon_db_host: str = Field(default="localhost", alias="NEON_DB_HOST")
    neon_db_user: str = Field(default="postgres", alias="NEON_DB_USER")
    neon_db_password: str = Field(default="", alias="NEON_DB_PASSWORD")
    neon_db_name: str = Field(default="ecommerce_crm", alias="NEON_DB_NAME")
    neon_db_port: str = Field(default="5432", alias="NEON_DB_PORT")

    # Object Storage Settings (MinIO / S3)
    minio_endpoint: str = Field(default="http://minio:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="", alias="MINIO_SECRET_KEY")

    # Buckets & Delta Lake Paths
    minio_bronze_bucket: str = Field(default="ecommerce-bronze", alias="MINIO_BRONZE_BUCKET")
    minio_silver_bucket: str = Field(default="ecommerce-silver", alias="MINIO_SILVER_BUCKET")
    silver_delta_path: str = Field(default="", alias="SILVER_DELTA_PATH")
    quarantine_path: str = Field(default="", alias="QUARANTINE_PATH")

    # Metabase BI Dashboard Settings
    metabase_url: str = Field(default="http://localhost:3000", alias="METABASE_URL")
    metabase_admin_email: str = Field(default="admin@ecommerce.local", alias="METABASE_ADMIN_EMAIL")
    metabase_admin_password: str = Field(default="", alias="METABASE_ADMIN_PASSWORD")

    # Runtime Directories
    landing_dir: str = str(PROJECT_ROOT / "data" / "landing")
    staging_dir: str = str(PROJECT_ROOT / "data" / "staging")
    logs_dir: str = str(PROJECT_ROOT / "logs")
    duckdb_path: str = str(PROJECT_ROOT / "data" / "gold_warehouse.duckdb")

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


@lru_cache()
def get_settings() -> Settings:
    settings_obj = Settings()

    # Dynamic MinIO Endpoint resolution for host vs docker
    if "minio:" in settings_obj.minio_endpoint:
        try:
            socket.gethostbyname("minio")
        except socket.gaierror:
            object.__setattr__(settings_obj, "minio_endpoint", settings_obj.minio_endpoint.replace("minio:", "127.0.0.1:"))

    # Dynamic defaults for derived S3 paths if not overridden
    if not settings_obj.silver_delta_path:
        object.__setattr__(settings_obj, "silver_delta_path", f"s3a://{settings_obj.minio_silver_bucket}/ecommerce_events")
    if not settings_obj.quarantine_path:
        object.__setattr__(settings_obj, "quarantine_path", f"s3a://{settings_obj.minio_silver_bucket}/quarantine")

    return settings_obj


settings = get_settings()

# Backward-compatible module-level aliases
NEON_DB_HOST = settings.neon_db_host
NEON_DB_USER = settings.neon_db_user
NEON_DB_PASSWORD = settings.neon_db_password
NEON_DB_NAME = settings.neon_db_name
NEON_DB_PORT = settings.neon_db_port

MINIO_ENDPOINT = settings.minio_endpoint
MINIO_ACCESS_KEY = settings.minio_access_key
MINIO_SECRET_KEY = settings.minio_secret_key

BRONZE_BUCKET = settings.minio_bronze_bucket
SILVER_BUCKET = settings.minio_silver_bucket
SILVER_DELTA_PATH = settings.silver_delta_path
QUARANTINE_PATH = settings.quarantine_path

LANDING_DIR = settings.landing_dir
STAGING_DIR = settings.staging_dir
RAW_CLICKSTREAM_DIR = settings.staging_dir
LOGS_DIR = settings.logs_dir
DUCKDB_PATH = settings.duckdb_path

# Auto-bootstrap runtime directories
for directory in (PROJECT_ROOT / "data" / "landing", PROJECT_ROOT / "data" / "staging", PROJECT_ROOT / "logs"):
    directory.mkdir(parents=True, exist_ok=True)
