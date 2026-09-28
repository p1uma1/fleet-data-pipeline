import os

import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "fleet"),
        user=os.getenv("POSTGRES_USER", "fleet"),
        password=os.getenv("POSTGRES_PASSWORD", "fleet"),
        cursor_factory=RealDictCursor,
    )
