"""Runtime configuration shared by the ETL pipeline and dashboard."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DATASET_ID = "3gki-wyrb"
SOCRATA_DOMAIN = "data.cambridgema.gov"


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


def csv_path() -> Path:
    raw = env("CSV_PATH", "data/police_log.csv") or "data/police_log.csv"
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def database_url(host_override: str | None = None, port_override: int | None = None) -> URL:
    """Build a properly escaped SQLAlchemy URL from separate credentials."""
    return URL.create(
        drivername="postgresql+psycopg2",
        username=env("DB_USER", env("POSTGRES_USER", "cpd_user")),
        password=env("DB_PASSWORD", env("POSTGRES_PASSWORD", "")),
        host=host_override or env("DB_HOST", "localhost"),
        port=port_override or int(env("DB_PORT", "5433") or "5433"),
        database=env("DB_NAME", env("POSTGRES_DB", "cpd_db")),
    )
