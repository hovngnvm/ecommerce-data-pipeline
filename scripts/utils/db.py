import psycopg2
import psycopg2.extensions
from contextlib import contextmanager
from collections.abc import Iterator
from scripts.config.settings import settings

@contextmanager
def get_db_connection() -> Iterator[psycopg2.extensions.connection]:
    """
    Context manager that yields a psycopg2 database connection
    and ensures it is safely closed on block exit or exception.
    """
    conn = psycopg2.connect(
        host=settings.neon_db_host,
        user=settings.neon_db_user,
        password=settings.neon_db_password,
        database=settings.neon_db_name,
        port=settings.neon_db_port,
        sslmode="require"
    )
    try:
        yield conn
    finally:
        conn.close()

def get_jdbc_config() -> tuple[str, dict[str, str]]:
    """
    Returns the JDBC URL and properties dictionary for Spark DB connection.
    """
    db_url = f"jdbc:postgresql://{settings.neon_db_host}:{settings.neon_db_port}/{settings.neon_db_name}"
    db_properties = {
        "user": settings.neon_db_user,
        "password": settings.neon_db_password,
        "driver": "org.postgresql.Driver",
        "ssl": "true",
        "sslmode": "require"
    }
    return db_url, db_properties
