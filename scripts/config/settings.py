from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """Centralized System Settings loaded from environment variables via Pydantic."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Database Settings (Neon PostgreSQL)
    neon_db_host: str = "localhost"
    neon_db_user: str = "postgres"
    neon_db_password: str = ""
    neon_db_name: str = "ecommerce_crm"
    neon_db_port: str = "5432"

    # Object Storage Settings (MinIO / S3)
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""

    # Buckets & Delta Lake Paths
    minio_bronze_bucket: str = "ecommerce-bronze"
    minio_silver_bucket: str = "ecommerce-silver"
    silver_delta_path: str = "s3a://ecommerce-silver/ecommerce_events"
    quarantine_path: str = "s3a://ecommerce-silver/quarantine"

    # Runtime Directories
    landing_dir: str = str(PROJECT_ROOT / "data" / "landing")
    staging_dir: str = str(PROJECT_ROOT / "data" / "staging")
    logs_dir: str = str(PROJECT_ROOT / "logs")
    duckdb_path: str = str(PROJECT_ROOT / "data" / "gold_warehouse.duckdb")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings: Settings = get_settings()


