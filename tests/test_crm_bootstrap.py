import unittest
from unittest import mock
import tempfile
from pathlib import Path
import pandas as pd
from scripts.bootstrap_crm_database import extract_unique_users_from_parquet

class TestCRMBootstrap(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_extract_unique_users_recursive(self):
        """Test recursive discovery of nested partitions: year=2026/month=01/day=01"""
        nested_dir = self.temp_path / "year=2026" / "month=01" / "day=01"
        nested_dir.mkdir(parents=True, exist_ok=True)

        df1 = pd.DataFrame({"user_id": [101, 102, 103, None]})
        df1.to_parquet(nested_dir / "part1.parquet")

        nested_dir2 = self.temp_path / "year=2026" / "month=01" / "day=02"
        nested_dir2.mkdir(parents=True, exist_ok=True)

        df2 = pd.DataFrame({"user_id": [103, 104, 105]})
        df2.to_parquet(nested_dir2 / "part2.parquet")

        with mock.patch("scripts.bootstrap_crm_database.settings.staging_dir", str(self.temp_path)):
            user_ids = extract_unique_users_from_parquet()
            self.assertEqual(user_ids, {101, 102, 103, 104, 105})

    def test_extract_unique_users_empty_dir_raises(self):
        """Test fail-fast FileNotFoundError when no parquet files exist"""
        with mock.patch("scripts.bootstrap_crm_database.settings.staging_dir", str(self.temp_path)):
            with self.assertRaises(FileNotFoundError):
                extract_unique_users_from_parquet()

    def test_extract_unique_users_corrupt_file_raises(self):
        """Test fail-fast RuntimeError when a parquet file is corrupt"""
        bad_file = self.temp_path / "corrupt.parquet"
        bad_file.write_text("not a valid parquet file content")

        with mock.patch("scripts.bootstrap_crm_database.settings.staging_dir", str(self.temp_path)):
            with self.assertRaises(RuntimeError):
                extract_unique_users_from_parquet()

if __name__ == "__main__":
    unittest.main()
