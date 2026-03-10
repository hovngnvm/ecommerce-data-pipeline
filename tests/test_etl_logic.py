import unittest
import duckdb
import pandas as pd

class TestETLLogic(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")

    def tearDown(self):
        self.con.close()

    def test_category_code_splitting_and_null_handling(self):
        """Test category splitting and default fallbacks"""
        sample_records = pd.DataFrame([
            {"user_id": 1, "category_code": "electronics.smartphone.phone", "brand": "apple"},
            {"user_id": 2, "category_code": "apparel.shoes", "brand": None},
            {"user_id": 3, "category_code": None, "brand": "samsung"},
        ])
        self.con.register("raw_sample", sample_records)

        query = """
            SELECT 
                user_id,
                COALESCE(NULLIF(split_part(category_code, '.', 1), ''), 'unknown') as category,
                COALESCE(NULLIF(split_part(category_code, '.', 2), ''), 'unknown') as sub_category,
                COALESCE(brand, 'unknown') as brand
            FROM raw_sample
            ORDER BY user_id;
        """
        results = self.con.execute(query).fetchall()

        self.assertEqual(results[0], (1, "electronics", "smartphone", "apple"))
        self.assertEqual(results[1], (2, "apparel", "shoes", "unknown"))
        self.assertEqual(results[2], (3, "unknown", "unknown", "samsung"))

    def test_cart_abandonment_logic(self):
        """Test identification of cart additions without subsequent purchases"""
        events = pd.DataFrame([
            {"user_session": "s1", "product_id": 101, "event_type": "cart"},
            {"user_session": "s1", "product_id": 101, "event_type": "purchase"},
            {"user_session": "s2", "product_id": 202, "event_type": "cart"},
            {"user_session": "s3", "product_id": 303, "event_type": "purchase"},
        ])
        self.con.register("events_tbl", events)

        query = """
            WITH cart_events AS (
                SELECT user_session, product_id FROM events_tbl WHERE event_type = 'cart'
            ),
            purchase_events AS (
                SELECT user_session, product_id FROM events_tbl WHERE event_type = 'purchase'
            )
            SELECT c.user_session, c.product_id
            FROM cart_events c
            LEFT JOIN purchase_events p 
              ON c.user_session = p.user_session AND c.product_id = p.product_id
            WHERE p.user_session IS NULL;
        """
        abandoned = self.con.execute(query).fetchall()
        self.assertEqual(len(abandoned), 1)
        self.assertEqual(abandoned[0], ("s2", 202))

if __name__ == "__main__":
    unittest.main()
