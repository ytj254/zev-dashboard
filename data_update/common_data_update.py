from sqlalchemy import create_engine
import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from your .env file
load_dotenv(dotenv_path=r"D:\Project\Ongoing\DEP MHD-ZEV Performance Monitoring\zev-dashboard\.env")

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL is not set in .env file")

# SQLAlchemy engine (good for pandas, high-level work)
engine = create_engine(DB_URL, pool_pre_ping=True)

def get_conn():
    """Return a raw psycopg2 connection (useful for execute_values)."""
    return psycopg2.connect(DB_URL)
