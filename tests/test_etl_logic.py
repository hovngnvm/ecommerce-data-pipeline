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

    def test_dynamic_loyalty_tier_calculation(self):
        """Test Dynamic RFM Loyalty Tier assignment based on accumulated spend and order volume"""
        raw_events = pd.DataFrame([
            # User 1: Browse/Cart only -> Member
            {"user_id": 1, "event_type": "view", "price": 100.0, "user_session": "s1", "event_time": "2026-01-01 10:00:00"},
            # User 2: 2 purchases totaling $250 -> Silver (>= $200 or >= 2 orders)
            {"user_id": 2, "event_type": "purchase", "price": 100.0, "user_session": "s2", "event_time": "2026-01-02 11:00:00"},
            {"user_id": 2, "event_type": "purchase", "price": 150.0, "user_session": "s3", "event_time": "2026-01-03 12:00:00"},
            # User 3: 1 purchase totaling $900 -> Gold (>= $800)
            {"user_id": 3, "event_type": "purchase", "price": 900.0, "user_session": "s4", "event_time": "2026-01-04 13:00:00"},
            # User 4: 10 purchases totaling $2500 -> Platinum (>= $2000 or >= 10 orders)
            {"user_id": 4, "event_type": "purchase", "price": 2500.0, "user_session": "s5", "event_time": "2026-01-05 14:00:00"},
        ])
        self.con.register("stg_events_sample", raw_events)

        query = """
            WITH user_summary AS (
                SELECT
                    user_id,
                    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_orders,
                    COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN price END), 0.0) AS total_spend_usd
                FROM stg_events_sample
                GROUP BY user_id
            )
            SELECT
                user_id,
                CASE
                    WHEN total_spend_usd >= 2000.0 OR total_orders >= 10 THEN 'Platinum'
                    WHEN total_spend_usd >= 800.0  OR total_orders >= 5  THEN 'Gold'
                    WHEN total_spend_usd >= 200.0  OR total_orders >= 2  THEN 'Silver'
                    ELSE 'Member'
                END AS calculated_tier
            FROM user_summary
            ORDER BY user_id;
        """
        results = dict(self.con.execute(query).fetchall())
        self.assertEqual(results[1], "Member")
        self.assertEqual(results[2], "Silver")
        self.assertEqual(results[3], "Gold")
        self.assertEqual(results[4], "Platinum")

    def test_loyalty_tier_progression_velocity(self):
        """Test SCD Type 2 tier progression velocity and transition classification"""
        snapshots_df = pd.DataFrame([
            {"user_id": 100, "loyalty_tier": "Member", "dbt_valid_from": "2026-01-01", "dbt_valid_to": "2026-01-15"},
            {"user_id": 100, "loyalty_tier": "Silver", "dbt_valid_from": "2026-01-15", "dbt_valid_to": "2026-02-15"},
            {"user_id": 100, "loyalty_tier": "Gold", "dbt_valid_from": "2026-02-15", "dbt_valid_to": None},
        ])
        self.con.register("snap_sample", snapshots_df)

        query = """
            WITH ranked AS (
                SELECT
                    user_id,
                    loyalty_tier AS current_tier,
                    LAG(loyalty_tier) OVER (PARTITION BY user_id ORDER BY dbt_valid_from) AS previous_tier,
                    LAG(dbt_valid_from) OVER (PARTITION BY user_id ORDER BY dbt_valid_from) AS previous_valid_from,
                    dbt_valid_from
                FROM snap_sample
            )
            SELECT
                current_tier,
                COALESCE(previous_tier, 'Member') AS prev_tier,
                CASE
                    WHEN previous_tier IS NULL THEN 'INITIAL'
                    WHEN previous_tier = 'Member' AND current_tier IN ('Silver', 'Gold', 'Platinum') THEN 'UPGRADE'
                    WHEN previous_tier = 'Silver' AND current_tier IN ('Gold', 'Platinum') THEN 'UPGRADE'
                    ELSE 'DOWNGRADE'
                END AS transition_type,
                CASE
                    WHEN previous_valid_from IS NOT NULL THEN
                        DATE_DIFF('day', CAST(previous_valid_from AS DATE), CAST(dbt_valid_from AS DATE))
                    ELSE 0
                END AS days_in_previous_tier
            FROM ranked
            ORDER BY dbt_valid_from;
        """
        rows = self.con.execute(query).fetchall()
        # Row 1: Initial Member
        self.assertEqual(rows[0][0], "Member")
        self.assertEqual(rows[0][2], "INITIAL")
        self.assertEqual(rows[0][3], 0)
        # Row 2: Upgrade to Silver after 14 days
        self.assertEqual(rows[1][0], "Silver")
        self.assertEqual(rows[1][1], "Member")
        self.assertEqual(rows[1][2], "UPGRADE")
        self.assertEqual(rows[1][3], 14)
        # Row 3: Upgrade to Gold after 31 days
        self.assertEqual(rows[2][0], "Gold")
        self.assertEqual(rows[2][1], "Silver")
        self.assertEqual(rows[2][2], "UPGRADE")
        self.assertEqual(rows[2][3], 31)

if __name__ == "__main__":
    unittest.main()
