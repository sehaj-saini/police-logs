"""Deduplicate incidents and add useful calendar fields for trend analysis."""

from __future__ import annotations

import pandas as pd
from prefect import task


@task(name="transform-police-log", log_prints=True)
def transform_data(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.drop_duplicates(subset=["id"], keep="first").copy()
    result["date_time"] = pd.to_datetime(result["date_time"])

    result["year"] = result["date_time"].dt.year.astype("int16")
    result["month"] = result["date_time"].dt.month.astype("int8")
    result["day"] = result["date_time"].dt.day.astype("int8")
    result["hour"] = result["date_time"].dt.hour.astype("int8")
    result["minute"] = result["date_time"].dt.minute.astype("int8")
    result["second"] = result["date_time"].dt.second.astype("int8")
    result["month_name"] = result["date_time"].dt.strftime("%B")
    result["weekday"] = result["date_time"].dt.strftime("%A")

    print(f"Transformed {len(result):,} unique incidents.")
    return result
