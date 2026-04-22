import os
import sys
import time
from pathlib import Path
import requests

# Ensure scripts directory is in sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from utils.config import (
    NEON_DB_HOST,
    NEON_DB_PORT,
    NEON_DB_USER,
    NEON_DB_PASSWORD,
    NEON_DB_NAME,
)
from utils.logger import get_logger

logger = get_logger(__name__)

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@ecommerce.local")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "AdminPassword123!")


def get_metabase_session(base_url: str = METABASE_URL, retries: int = 6, delay: int = 5) -> str | None:
    """
    Retrieves a valid Metabase admin session token. Completes initial setup wizard
    if first run, or authenticates via existing admin credentials.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info("Fetching session properties from Metabase API...")
            resp = requests.get(f"{base_url}/api/session/properties", timeout=10)
            if resp.status_code == 200:
                properties = resp.json()
                token = properties.get("setup-token")
                break
        except Exception as err:
            logger.warning(f"Connection attempt {attempt}/{retries} failed: {err}")
        time.sleep(delay)
    else:
        logger.error(f"Could not connect to Metabase API after {retries} attempts.")
        return None

    if token:
        logger.info("Setup token detected. Executing initial admin account setup wizard...")
        setup_payload = {
            "token": token,
            "user": {
                "email": ADMIN_EMAIL,
                "first_name": "Admin",
                "last_name": "DE",
                "password": ADMIN_PASSWORD,
            },
            "prefs": {
                "site_name": "E-Commerce Data Platform",
                "allow_tracking": False,
            },
        }
        res = requests.post(f"{base_url}/api/setup", json=setup_payload, timeout=15)
        if res.status_code == 200:
            session_id = res.json().get("id")
            logger.info("Initial setup wizard completed successfully.")
            return session_id
        logger.error(f"Metabase setup wizard failed: {res.text}")
        return None

    logger.info("Setup already completed. Authenticating existing admin credentials...")
    res = requests.post(
        f"{base_url}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        timeout=15,
    )
    if res.status_code == 200:
        session_id = res.json().get("id")
        logger.info("Admin authentication successful.")
        return session_id

    logger.error(f"Admin authentication failed: {res.text}")
    return None


def ensure_crm_database(base_url: str, headers: dict[str, str], db_name: str = "Neon CRM Postgres") -> int | None:
    """
    Idempotently verifies and registers the Neon Cloud Postgres CRM database connection in Metabase.
    """
    logger.info("Checking registered databases in Metabase...")
    try:
        dbs_resp = requests.get(f"{base_url}/api/database", headers=headers, timeout=10).json()
        dbs_list = dbs_resp.get("data", dbs_resp) if isinstance(dbs_resp, dict) else dbs_resp
        existing_db = next((d for d in dbs_list if isinstance(d, dict) and d.get("name") == db_name), None)

        if existing_db:
            db_id = existing_db["id"]
            logger.info(f"Database '{db_name}' already registered (ID: {db_id}).")
            return db_id
    except Exception as err:
        logger.warning(f"Failed to query existing databases: {err}")

    logger.info(f"Registering new database connection '{db_name}'...")
    db_payload = {
        "name": db_name,
        "engine": "postgres",
        "details": {
            "host": NEON_DB_HOST,
            "port": int(NEON_DB_PORT),
            "db": NEON_DB_NAME,
            "user": NEON_DB_USER,
            "password": NEON_DB_PASSWORD,
            "ssl": True,
            "ssl-mode": "require",
        },
    }
    resp = requests.post(f"{base_url}/api/database", headers=headers, json=db_payload, timeout=20)
    if resp.status_code == 200:
        db_id = resp.json().get("id")
        logger.info(f"Database connection '{db_name}' registered successfully (ID: {db_id}).")
        return db_id

    logger.error(f"Failed to register database '{db_name}': {resp.text}")
    return None


def ensure_executive_dashboard(base_url: str, headers: dict[str, str], title: str = "E-Commerce Executive Overview") -> int | None:
    """
    Idempotently creates or retrieves the main Executive BI Dashboard.
    """
    logger.info("Checking existing dashboards...")
    try:
        dashs_resp = requests.get(f"{base_url}/api/dashboard", headers=headers, timeout=10).json()
        dashs_list = dashs_resp.get("data", dashs_resp) if isinstance(dashs_resp, dict) else dashs_resp
        existing_dash = next((d for d in dashs_list if isinstance(d, dict) and title in d.get("name", "")), None)

        if existing_dash:
            dash_id = existing_dash["id"]
            logger.info(f"Dashboard '{title}' already exists (ID: {dash_id}).")
            return dash_id
    except Exception as err:
        logger.warning(f"Failed to query existing dashboards: {err}")

    logger.info(f"Creating new dashboard '{title}'...")
    dash_payload = {
        "name": title,
        "description": "Executive Overview of CRM Users, Loyalty Tiers, and Acquisition Channels.",
    }
    resp = requests.post(f"{base_url}/api/dashboard", headers=headers, json=dash_payload, timeout=15)
    if resp.status_code == 200:
        dash_id = resp.json()["id"]
        logger.info(f"Dashboard '{title}' created successfully (ID: {dash_id}).")
        return dash_id

    logger.error(f"Failed to create dashboard '{title}': {resp.text}")
    return None


def create_and_attach_cards(base_url: str, headers: dict[str, str], db_id: int, dash_id: int) -> bool:
    """
    Creates analytical SQL cards and links them to the target Executive Dashboard.
    """
    cards_config: list[dict[str, Any]] = [
        # Key metric cards
        {
            "name": "Total Registered CRM Users",
            "display": "scalar",
            "query": "SELECT COUNT(*) as total_crm_users FROM crm.user_loyalty;",
            "col": 0, "row": 0, "size_x": 4, "size_y": 3,
        },
        {
            "name": "Active Loyalty Tiers Count",
            "display": "scalar",
            "query": "SELECT COUNT(DISTINCT loyalty_tier) as active_tiers FROM crm.user_loyalty WHERE loyalty_tier IS NOT NULL;",
            "col": 4, "row": 0, "size_x": 4, "size_y": 3,
        },
        {
            "name": "Top Acquisition Channels Count",
            "display": "scalar",
            "query": "SELECT COUNT(DISTINCT acquisition_channel) as active_channels FROM crm.user_loyalty WHERE acquisition_channel IS NOT NULL;",
            "col": 8, "row": 0, "size_x": 4, "size_y": 3,
        },
        # Trend and distribution charts
        {
            "name": "Daily CRM User Signups Trend",
            "display": "line",
            "query": "SELECT signup_date, COUNT(*) as daily_signups FROM crm.user_loyalty WHERE signup_date IS NOT NULL GROUP BY signup_date ORDER BY signup_date ASC;",
            "col": 0, "row": 3, "size_x": 8, "size_y": 6,
        },
        {
            "name": "Acquisition Channel Share",
            "display": "pie",
            "query": "SELECT acquisition_channel, COUNT(*) as channel_users FROM crm.user_loyalty GROUP BY acquisition_channel ORDER BY channel_users DESC;",
            "col": 8, "row": 3, "size_x": 4, "size_y": 6,
        },
        # Breakdown and comparison charts
        {
            "name": "User Distribution by Loyalty Tier",
            "display": "bar",
            "query": "SELECT loyalty_tier, COUNT(*) as total_users FROM crm.user_loyalty GROUP BY loyalty_tier ORDER BY total_users DESC;",
            "col": 0, "row": 9, "size_x": 6, "size_y": 6,
        },
        {
            "name": "Loyalty Tiers Breakdown by Channel",
            "display": "bar",
            "query": "SELECT acquisition_channel, loyalty_tier, COUNT(*) as user_count FROM crm.user_loyalty GROUP BY acquisition_channel, loyalty_tier ORDER BY acquisition_channel, user_count DESC;",
            "col": 6, "row": 9, "size_x": 6, "size_y": 6,
        },
    ]

    # Fetch existing cards to make creation idempotent and prevent duplicate cards
    existing_cards_resp = requests.get(f"{base_url}/api/card", headers=headers, timeout=10).json()
    existing_cards = existing_cards_resp.get("data", existing_cards_resp) if isinstance(existing_cards_resp, dict) else existing_cards_resp
    existing_card_map = {c["name"]: c["id"] for c in existing_cards if isinstance(c, dict) and "name" in c and "id" in c}

    dash_cards: list[dict[str, Any]] = []
    for idx, card_def in enumerate(cards_config, start=1):
        card_name = card_def["name"]
        if card_name in existing_card_map:
            card_id = existing_card_map[card_name]
            logger.info(f"Reusing existing card '{card_name}' (ID: {card_id}).")
        else:
            card_payload = {
                "name": card_name,
                "dataset_query": {
                    "type": "native",
                    "native": {"query": card_def["query"]},
                    "database": db_id,
                },
                "display": card_def["display"],
                "visualization_settings": {},
            }
            resp = requests.post(f"{base_url}/api/card", headers=headers, json=card_payload, timeout=15)
            if resp.status_code == 200:
                card_id = resp.json()["id"]
                logger.info(f"Created card '{card_name}' (ID: {card_id}).")
            else:
                logger.warning(f"Could not create card '{card_name}': {resp.text}")
                continue

        dash_cards.append({
            "id": -idx,
            "card_id": card_id,
            "col": card_def["col"],
            "row": card_def["row"],
            "size_x": card_def["size_x"],
            "size_y": card_def["size_y"],
            "series": [],
            "visualization_settings": {},
            "parameter_mappings": [],
        })

    if dash_cards:
        logger.info("Attaching analytical cards to Executive Dashboard...")
        put_resp = requests.put(
            f"{base_url}/api/dashboard/{dash_id}",
            headers=headers,
            json={"dashcards": dash_cards},
            timeout=15,
        )
        if put_resp.status_code == 200:
            logger.info("Dashboard successfully populated with analytical cards.")
            return True
        logger.error(f"Failed to attach cards to dashboard: {put_resp.text}")

    return False


def main() -> bool:
    """
    Main orchestration routine for Metabase setup and dashboard seeding.
    """
    logger.info("Starting Metabase automated setup and dashboard configuration...")
    session_id = get_metabase_session(METABASE_URL)
    if not session_id:
        logger.error("Metabase session acquisition failed. Aborting setup.")
        return False

    headers = {"X-Metabase-Session": session_id}
    db_id = ensure_crm_database(METABASE_URL, headers)
    if not db_id:
        logger.error("Database registration failed. Aborting setup.")
        return False

    dash_id = ensure_executive_dashboard(METABASE_URL, headers)
    if not dash_id:
        logger.error("Dashboard initialization failed. Aborting setup.")
        return False

    success = create_and_attach_cards(METABASE_URL, headers, db_id, dash_id)
    if success:
        logger.info("Metabase setup and dashboard creation completed successfully.")
        logger.info(f"Access Dashboard at: {METABASE_URL}")
        return True

    logger.warning("Metabase setup finished with warnings.")
    return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
