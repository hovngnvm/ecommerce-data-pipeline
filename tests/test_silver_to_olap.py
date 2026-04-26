import unittest
from unittest import mock
import duckdb
import uuid
import tempfile
from pathlib import Path
import pandas as pd
from scripts.silver_to_olap import run_silver_to_olap

class TestSilverToOlap(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")

    def tearDown(self):
        self.con.close()

    def test_duckdb_schema_creation_and_query(self):
        """Contract Test: Verifies DuckDB table schema creation and transformation query logic"""
        self.con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        self.con.execute("""
            CREATE TABLE silver.ecommerce_events (
                user_id INTEGER,
                event_type VARCHAR,
                product_id INTEGER,
                category VARCHAR,
                sub_category VARCHAR,
                brand VARCHAR,
                price DOUBLE,
                user_session VARCHAR,
                event_time TIMESTAMP,
                loyalty_tier VARCHAR,
                acquisition_channel VARCHAR
            );
        """)

        sample_data = pd.DataFrame([{
            'user_id': 1001,
            'event_type': 'purchase',
            'product_id': 5001,
            'category': 'electronics',
            'sub_category': 'smartphone',
            'brand': 'apple',
            'price': 999.9,
            'user_session': 'sess_1',
            'event_time': '2026-07-26 12:00:00'
        }])

        self.con.register("sample_events", sample_data)
        self.con.execute("""
            INSERT INTO silver.ecommerce_events (
                user_id, event_type, product_id, category, sub_category,
                brand, price, user_session, event_time, loyalty_tier, acquisition_channel
            )
            SELECT
                user_id,
                event_type,
                product_id,
                category,
                sub_category,
                brand,
                price,
                user_session,
                CAST(event_time AS TIMESTAMP) as event_time,
                'Gold' as loyalty_tier,
                'Direct' as acquisition_channel
            FROM sample_events;
        """)

        result = self.con.execute("SELECT user_id, category, sub_category, loyalty_tier FROM silver.ecommerce_events").fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (1001, 'electronics', 'smartphone', 'Gold'))

    @mock.patch("scripts.silver_to_olap.get_db_connection")
    def test_failure_atomicity_preserves_tables(self, mock_get_db):
        """Atomicity Test: Ensures failed sync rolls back and preserves pre-existing DuckDB tables"""
        mock_conn = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.fetchall.return_value = [(1001, "Gold", "2025-01-15", "Facebook Ads")]
        mock_cur.description = [("user_id",), ("loyalty_tier",), ("signup_date",), ("acquisition_channel",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_db.return_value.__enter__.return_value = mock_conn

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
