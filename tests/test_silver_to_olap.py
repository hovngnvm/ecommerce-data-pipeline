import unittest
from unittest import mock
import duckdb
import uuid
import tempfile
from pathlib import Path
from scripts.silver_to_olap import run_silver_to_olap


class TestSilverToOlap(unittest.TestCase):


    @mock.patch("scripts.silver_to_olap.pd.read_sql")
    @mock.patch("scripts.silver_to_olap.get_db_connection")
    def test_failure_atomicity_preserves_tables(self, mock_get_db, mock_read_sql):
        """Atomicity Test: Ensures failed sync rolls back and preserves pre-existing DuckDB tables"""
        tmp_db_path = str(Path(tempfile.gettempdir()) / f"test_atomic_{uuid.uuid4().hex}.duckdb")

        con = duckdb.connect(tmp_db_path)
        try:
            # Seed sentinel pre-existing records in both crm and silver schemas
            con.execute("CREATE SCHEMA IF NOT EXISTS crm;")
            con.execute("CREATE TABLE crm.user_loyalty (user_id INTEGER PRIMARY KEY, loyalty_tier VARCHAR, signup_date DATE, acquisition_channel VARCHAR);")
            con.execute("INSERT INTO crm.user_loyalty VALUES (8888, 'Platinum', '2025-01-01', 'Direct');")

            con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
            con.execute("CREATE TABLE silver.ecommerce_events (user_id INT, event_type VARCHAR, event_time TIMESTAMP);")
            con.execute("INSERT INTO silver.ecommerce_events VALUES (9999, 'cart', '2026-01-01 10:00:00');")
            con.close()

            # Trigger run_silver_to_olap which will fail at delta_scan step
            with self.assertRaises(Exception):
                run_silver_to_olap(target_duckdb_path=tmp_db_path)

            # Re-verify that sentinel data in BOTH tables was preserved and not truncated
            con = duckdb.connect(tmp_db_path)
            silver_rows = con.execute("SELECT user_id, event_type FROM silver.ecommerce_events;").fetchall()
            self.assertEqual(len(silver_rows), 1)
            self.assertEqual(silver_rows[0], (9999, 'cart'))

            crm_rows = con.execute("SELECT user_id, loyalty_tier FROM crm.user_loyalty;").fetchall()
            self.assertEqual(len(crm_rows), 1)
            self.assertEqual(crm_rows[0], (8888, 'Platinum'))
        finally:
            con.close()
            db_file = Path(tmp_db_path)
            if db_file.exists():
                try:
                    db_file.unlink()
                except OSError:
                    pass

if __name__ == "__main__":
    unittest.main()
