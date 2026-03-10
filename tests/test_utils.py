import unittest
import os
import sys

# Add scripts directory to path
SYS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(SYS_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from utils.config import NEON_DB_HOST, NEON_DB_PORT, NEON_DB_NAME
from utils.logger import get_logger

class TestUtils(unittest.TestCase):
    def test_config_variables(self):
        """Test essential configuration constants are loaded"""
        self.assertIsNotNone(NEON_DB_HOST)
        self.assertEqual(str(NEON_DB_PORT), "5432")
        self.assertTrue(len(NEON_DB_NAME) > 0)

    def test_logger_initialization(self):
        """Test standard logger creation"""
        logger = get_logger("test_logger")
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "test_logger")

if __name__ == "__main__":
    unittest.main()
