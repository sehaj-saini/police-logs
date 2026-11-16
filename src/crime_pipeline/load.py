"""Load a complete source snapshot into PostgreSQL in one transaction."""

from __future__ import annotations

import pandas as pd
from prefect import task
from sqlalchemy import create_engine, text

from .config import database_url


TABLE_DDL = """
CREATE TABLE IF NOT EXISTS cpd_incidents (
    id TEXT PRIMARY KEY,
    date_time TIMESTAMP NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL DEFAULT '',
    location TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    last_updated TIMESTAMP NULL,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    hour SMALLINT NOT NULL,
    minute SMALLINT NOT NULL,
    second SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    weekday TEXT NOT NULL
)
"""

LOAD_COLUMNS = [
    "id",
    "date_time",
    "type",
    "subtype",
    "location",
    "description",
    "last_updated",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "month_name",
    "weekday",
]


@task(name="load-postgres", log_prints=True)
def load_into_postgres(frame: pd.DataFrame) -> int:
    engine = create_engine(database_url(), pool_pre_ping=True)
    to_load = frame[LOAD_COLUMNS].copy()
    to_load["id"] = to_load["id"].astype(str)

    try:
        with engine.begin() as connection:
            connection.execute(text(TABLE_DDL))
            # Replace the source snapshot atomically so repeated runs are idempotent.
            connection.execute(text("DELETE FROM cpd_incidents"))
            to_load.to_sql(
                "cpd_incidents",
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500,
            )
    finally:
        engine.dispose()

    print(f"Loaded {len(to_load):,} incidents into PostgreSQL.")
    return len(to_load)
