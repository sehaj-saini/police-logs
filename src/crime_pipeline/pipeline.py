"""Prefect flow tying extract, validate, transform, and load together."""

from __future__ import annotations

import os

from prefect import flow
from prefect.client.schemas.schedules import CronSchedule

from .extract import extract_data
from .load import load_into_postgres
from .transform import transform_data
from .validate import validate_data
from .config import env


@flow(name="cpd-incident-etl", log_prints=True)
def etl() -> dict[str, int]:
    raw = extract_data()
    validated, quality = validate_data(raw)
    transformed = transform_data(validated)
    loaded = load_into_postgres(transformed)

    result = {**quality, "rows_loaded": loaded}
    print(f"ETL complete: {result}")
    return result


if __name__ == "__main__":
    run_mode = (os.getenv("RUN_MODE", "serve") or "serve").lower()
    if run_mode == "once":
        etl()
    elif run_mode == "serve":
        schedule = CronSchedule(
            cron=env("PREFECT_CRON", "0 0 * * *") or "0 0 * * *",
            timezone=env("PREFECT_TIMEZONE", "America/New_York") or "America/New_York",
        )
        etl.serve(
            name="cpd-daily-pipeline",
            schedules=[schedule],
            tags=["pai102", "crime-trends"],
        )
    else:
        raise ValueError("RUN_MODE must be 'once' or 'serve'.")
