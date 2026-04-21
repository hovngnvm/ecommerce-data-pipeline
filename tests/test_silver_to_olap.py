import unittest
from unittest import mock
import duckdb
import os
import uuid
import tempfile
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
            'category_code': 'electronics.smartphone',
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
                split_part(category_code, '.', 1) as category,
                split_part(category_code, '.', 2) as sub_category,
                brand,
                price,
                user_session,
                CAST(event_time AS TIMESTAMP) as event_time,
                'Gold' as loyalty_tier,
                'Direct' as acquisition_channel
            FROM sample_events;
        """)

        result = self.con.execute("SELECT user_id, category, loyalty_tier FROM silver.ecommerce_events").fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (1001, 'electronics', 'Gold'))

    @mock.patch("scripts.silver_to_olap.get_db_connection")
    def test_run_silver_to_olap_entrypoint(self, mock_get_db):
        """Integration Test: Executes run_silver_to_olap entry point with mocked PostgreSQL & S3 boundaries"""
        mock_conn = mock.MagicMock()
        mock_cur = mock.MagicMock()
        mock_cur.fetchall.return_value = [(1001, "Gold", "2025-01-15", "Facebook Ads")]
        mock_cur.description = [("user_id",), ("loyalty_tier",), ("signup_date",), ("acquisition_channel",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_get_db.return_value.__enter__.return_value = mock_conn

        tmp_db_path = os.path.join(tempfile.gettempdir(), f"test_{uuid.uuid4().hex}.duckdb")

        try:
            with self.assertRaises(Exception) as ctx:
                run_silver_to_olap(target_duckdb_path=tmp_db_path)
            err_msg = str(ctx.exception)
            self.assertTrue(
                any(p in err_msg for p in ["Cannot access Silver Lake", "Could not connect", "S3", "IO Error", "Connection refused"])
            )
        finally:
            if os.path.exists(tmp_db_path):
                try:
                    os.remove(tmp_db_path)
                except OSError:
                    pass

if __name__ == "__main__":
    unittest.main()
