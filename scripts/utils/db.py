import psycopg2
from contextlib import contextmanager
from utils.config import NEON_DB_HOST, NEON_DB_USER, NEON_DB_PASSWORD, NEON_DB_NAME, NEON_DB_PORT

@contextmanager
def get_db_connection():
    """
    Context manager that yields a psycopg2 database connection
    and ensures it is safely closed on block exit or exception.
    """
    conn = psycopg2.connect(
        host=NEON_DB_HOST,
        user=NEON_DB_USER,
        password=NEON_DB_PASSWORD,
        database=NEON_DB_NAME,
        port=NEON_DB_PORT,
        sslmode="require"
    )
    try:
        yield conn
    finally:
        conn.close()

def get_jdbc_config():
    """
    Returns the JDBC URL and properties dictionary for Spark DB connection.
    """
    db_url = f"jdbc:postgresql://{NEON_DB_HOST}:{NEON_DB_PORT}/{NEON_DB_NAME}"
    db_properties = {
        "user": NEON_DB_USER,
        "password": NEON_DB_PASSWORD,
        "driver": "org.postgresql.Driver",
        "ssl": "true",
        "sslmode": "require"
    }
    return db_url, db_properties
