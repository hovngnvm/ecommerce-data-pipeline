import unittest
import sys
import importlib
from unittest.mock import MagicMock, patch
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from scripts.config.settings import settings, get_settings, Settings
from scripts.utils.logger import get_logger
from scripts.utils.spark import SPARK_PACKAGES
from scripts.bootstrap_crm_database import extract_unique_users_from_parquet

class TestUtils(unittest.TestCase):
    def test_config_variables(self):
        """Test essential configuration constants and centralized paths are loaded"""
        self.assertIsNotNone(settings.neon_db_host)
        self.assertEqual(str(settings.neon_db_port), "5432")
        self.assertTrue(len(settings.neon_db_name) > 0)
        self.assertTrue(settings.duckdb_path.endswith("gold_warehouse.duckdb"))
        self.assertEqual(settings.minio_bronze_bucket, "ecommerce-bronze")
        self.assertTrue(settings.silver_delta_path.startswith("s3a://"))
        self.assertTrue(settings.quarantine_path.startswith("s3a://"))

    def test_settings_singleton_lru_cache(self):
        """Test Settings class, pydantic validation, and @lru_cache singleton identity"""
        s1 = get_settings()
        s2 = settings
        self.assertIsInstance(s1, Settings)
        self.assertIs(s1, s2)
        self.assertEqual(s1.minio_bronze_bucket, "ecommerce-bronze")
        self.assertTrue(len(s1.neon_db_name) > 0)

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
                "scripts.config.settings",
                "scripts.utils.db",
                "scripts.utils.logger",
                "scripts.utils.spark",
                "scripts.bootstrap_crm_database",
                "scripts.upload_to_bronze",
                "scripts.bronze_to_silver",
                "scripts.silver_to_olap",
                "scripts.raw_to_bronze_prep",
                "scripts.setup_metabase",
            ]
            for module_name in modules_to_test:
                with self.subTest(module=module_name):
                    mod = importlib.import_module(module_name)
                    self.assertIsNotNone(mod)
        finally:
            for pkg, orig in original_modules.items():
                if orig is None:
                    sys.modules.pop(pkg, None)

    def test_crm_bootstrap_unique_users_fail_fast_on_empty(self):
        """Contract Test: Ensures extract_unique_users_from_parquet raises FileNotFoundError when no files exist"""
        with patch("pathlib.Path.rglob", return_value=[]):
            with self.assertRaises(FileNotFoundError):
                extract_unique_users_from_parquet()

if __name__ == "__main__":
    unittest.main()
