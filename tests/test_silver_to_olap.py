import unittest
import duckdb
import os
import pandas as pd

class TestSilverToOlap(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")

    def tearDown(self):
        self.con.close()

    def test_duckdb_schema_creation_and_query(self):
        """Test DuckDB table schema creation and transformation query logic"""
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
        
        self.con.register("raw_sample", sample_data)
        self.con.execute("""
            INSERT INTO silver.ecommerce_events
            SELECT 
                user_id,
                event_type,
                product_id,
                split_part(category_code, '.', 1) as category,
                split_part(category_code, '.', 2) as sub_category,
                brand,
                price,
                user_session,
                TRY_CAST(event_time AS TIMESTAMP),
                'Regular' as loyalty_tier,
                'Organic' as acquisition_channel
            FROM raw_sample;
        """)
        
        res = self.con.execute("SELECT category, sub_category FROM silver.ecommerce_events WHERE user_id = 1001;").fetchone()
        self.assertEqual(res[0], "electronics")
        self.assertEqual(res[1], "smartphone")

if __name__ == "__main__":
    unittest.main()
