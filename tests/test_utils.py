import unittest
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from utils.config import (
    NEON_DB_HOST,
    NEON_DB_PORT,
    NEON_DB_NAME,
    DUCKDB_PATH,
    BRONZE_BUCKET,
    SILVER_DELTA_PATH,
    QUARANTINE_PATH
)
from utils.logger import get_logger
from utils.spark import SPARK_PACKAGES

class TestUtils(unittest.TestCase):
    def test_config_variables(self):
        """Test essential configuration constants and centralized paths are loaded"""
        self.assertIsNotNone(NEON_DB_HOST)
        self.assertEqual(str(NEON_DB_PORT), "5432")
        self.assertTrue(len(NEON_DB_NAME) > 0)
        self.assertTrue(DUCKDB_PATH.endswith("gold_warehouse.duckdb"))
        self.assertEqual(BRONZE_BUCKET, "ecommerce-bronze")
        self.assertTrue(SILVER_DELTA_PATH.startswith("s3a://"))
        self.assertTrue(QUARANTINE_PATH.startswith("s3a://"))

    def test_spark_packages_defined(self):
        """Test Spark package dependencies string is valid"""
        self.assertIn("hadoop-aws", SPARK_PACKAGES)
        self.assertIn("delta-spark", SPARK_PACKAGES)
        self.assertIn("postgresql", SPARK_PACKAGES)

    def test_logger_initialization(self):
        """Test standard logger creation with __name__ and custom name"""
        logger = get_logger(__name__)
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, __name__)

        custom_logger = get_logger("test_custom_logger")
        self.assertEqual(custom_logger.name, "test_custom_logger")

    def test_entrypoint_imports_contract(self):
        """Contract Test: Ensures all executable pipeline entrypoints and utilities import cleanly"""
        import importlib
        from unittest.mock import MagicMock

        # Ensure container-only / heavy cloud drivers are safely stubbed if not installed on the local runner
        mock_packages = [
            "boto3",
            "botocore",
            "botocore.client",
            "pyspark",
            "pyspark.sql",
            "pyspark.sql.types",
            "pyspark.sql.functions",
            "delta",
            "delta.tables"
        ]
        original_modules = {}
        for pkg in mock_packages:
            if pkg not in sys.modules:
                try:
                    importlib.import_module(pkg)
                except ImportError:
                    sys.modules[pkg] = MagicMock()
                    original_modules[pkg] = None

        try:
            modules_to_test = [
                "utils.config",
                "utils.db",
                "utils.logger",
                "utils.spark",
                "bootstrap_crm_database",
                "upload_to_bronze",
                "bronze_to_silver",
                "silver_to_olap",
                "raw_to_bronze_prep",
                "setup_metabase",
            ]
            for module_name in modules_to_test:
                with self.subTest(module=module_name):
                    mod = importlib.import_module(module_name)
                    self.assertIsNotNone(mod)
        finally:
            for pkg, orig in original_modules.items():
                if orig is None:
                    sys.modules.pop(pkg, None)

    def test_crm_bootstrap_unique_users_fallback(self):
        """Contract Test: Ensures extract_unique_users_from_parquet provides a safe fallback when no files exist"""
        from bootstrap_crm_database import extract_unique_users_from_parquet
        from unittest.mock import patch

        with patch("pathlib.Path.rglob", return_value=[]):
            fallback_ids = extract_unique_users_from_parquet()
            self.assertEqual(len(fallback_ids), 1000)
            self.assertIn(1000, fallback_ids)
            self.assertIn(1999, fallback_ids)

if __name__ == "__main__":
    unittest.main()
