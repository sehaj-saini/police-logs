"""Normalize source headers and report/filter invalid incident rows."""

from __future__ import annotations

import re

import pandas as pd
from prefect import task


REQUIRED = ["date_time", "id", "type", "location"]
OPTIONAL = ["subtype", "description", "last_updated"]
EXPECTED = REQUIRED + OPTIONAL

HEADER_MAP = {
    "datetime": "date_time",
    "datetimeincident": "date_time",
    "id": "id",
    "type": "type",
    "subtype": "subtype",
    "location": "location",
    "description": "description",
    "lastupdated": "last_updated",
}


def _normalized_header(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


@task(name="validate-police-log", log_prints=True)
def validate_data(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Accept the notebook API schema and the uploaded CSV's display headers."""
    rename = {
        original: HEADER_MAP[_normalized_header(original)]
        for original in frame.columns
        if _normalized_header(original) in HEADER_MAP
    }
    result = frame.rename(columns=rename).copy()

    missing = [column for column in REQUIRED if column not in result.columns]
    if missing:
        raise ValueError(
            f"Unexpected source schema. Required columns missing: {missing}. "
            f"Columns found: {list(frame.columns)}"
        )

    for column in OPTIONAL:
        if column not in result.columns:
            result[column] = ""

    result = result[EXPECTED].copy()
    for column in EXPECTED:
        result[column] = result[column].astype("string").str.strip()
    for column in OPTIONAL[:2]:
        result[column] = result[column].fillna("")

    result["date_time"] = pd.to_datetime(
        result["date_time"], errors="coerce", format="mixed"
    )
    result["last_updated"] = pd.to_datetime(
        result["last_updated"], errors="coerce", format="mixed"
    )

    invalid_id = result["id"].isna() | ~result["id"].str.fullmatch(r"\d+", na=False)
    invalid_required = (
        result["date_time"].isna()
        | invalid_id
        | result["type"].isna()
        | result["type"].eq("")
        | result["location"].isna()
        | result["location"].eq("")
    )
    rows_before = len(result)
    duplicate_ids = int(result.loc[~invalid_required, "id"].duplicated(keep="first").sum())
    invalid_rows = int(invalid_required.sum())
    result = result.loc[~invalid_required].copy()

    quality = {
        "source_rows": rows_before,
        "invalid_required_rows_removed": invalid_rows,
        "duplicate_ids_found": duplicate_ids,
        "valid_rows": len(result),
        "blank_subtype_rows": int(result["subtype"].isna().sum() + result["subtype"].eq("").sum()),
        "blank_description_rows": int(
            result["description"].isna().sum() + result["description"].eq("").sum()
        ),
    }

    if result.empty:
        raise ValueError("No valid incidents remain after required-field validation.")

    print(
        f"Quality check: {quality['valid_rows']:,} valid rows; "
        f"{invalid_rows:,} invalid rows removed; {duplicate_ids:,} duplicate IDs found."
    )
    return result, quality
