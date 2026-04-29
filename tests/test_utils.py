import unittest

from scripts.config.settings import settings, get_settings, Settings
from scripts.utils.logger import get_logger
from scripts.utils.spark import SPARK_PACKAGES


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

    def test_settings_singleton_identity(self):
        """Test Settings class, instance identity, and get_settings() contract"""
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


if __name__ == "__main__":
    unittest.main()


