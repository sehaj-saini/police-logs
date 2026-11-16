"""Extract the supplied CSV or the current Cambridge Police Log API data."""

from __future__ import annotations

import pandas as pd
from prefect import task
from sodapy import Socrata

from .config import DATASET_ID, SOCRATA_DOMAIN, csv_path, env


@task(name="extract-police-log", retries=3, retry_delay_seconds=10, log_prints=True)
def extract_data() -> pd.DataFrame:
    source = (env("DATA_SOURCE", "csv") or "csv").lower()

    if source == "csv":
        path = csv_path()
        if not path.is_file():
            raise FileNotFoundError(
                f"CSV source not found at {path}. Copy the supplied file to data/police_log.csv, "
                "or set DATA_SOURCE=api in .env."
            )
        frame = pd.read_csv(path, dtype="string", encoding="utf-8-sig")
        print(f"Read {len(frame):,} rows from {path.name}.")
        return frame

    if source != "api":
        raise ValueError("DATA_SOURCE must be either 'csv' or 'api'.")

    client = Socrata(
        SOCRATA_DOMAIN,
        env("SOCRATA_APP_TOKEN"),
        timeout=60,
    )
    try:
        records = list(client.get_all(DATASET_ID))
    finally:
        client.close()

    if not records:
        raise ValueError("The Socrata API returned no records.")

    frame = pd.DataFrame.from_records(records)
    print(f"Fetched {len(frame):,} rows from Socrata dataset {DATASET_ID}.")
    return frame
