"""SCD Type 2 helper functions.

The helpers are intentionally small and SQL-oriented. Databricks users can inspect the
generated statements directly, which is useful when validating temporal dimension logic.
"""

from __future__ import annotations

from hashlib import sha256
from typing import Iterable


def hash_attribute_values(values: Iterable[object]) -> str:
    """Create a deterministic hash for dimension change detection.

    None values are represented explicitly so that an empty string and a missing value
    do not accidentally produce the same hash.
    """

    # This Python version is useful in local tests and mirrors the Spark SQL hash
    # expression generated below.
    normalized = ["<NULL>" if value is None else str(value).strip() for value in values]
    payload = "||".join(normalized)
    return sha256(payload.encode("utf-8")).hexdigest()


def build_hash_expression(columns: list[str]) -> str:
    """Build a Spark SQL hash expression for a list of business attributes."""

    # The generated expression compares business attributes only. Technical fields
    # like ingestion time should not cause a new SCD2 version.
    normalized = [f"coalesce(cast({column} as string), '<NULL>')" for column in columns]
    return f"sha2(concat_ws('||', {', '.join(normalized)}), 256)"


def scd2_staged_view_sql(
    source_table: str,
    natural_key: str,
    attribute_columns: list[str],
    effective_ts_column: str,
    staged_view_name: str,
) -> str:
    """Return SQL that creates a staged SCD2 source view.

    The view keeps only the latest source record per natural key and effective timestamp.
    That protects the merge from duplicate snapshot rows within the same batch.
    """

    # Staging removes duplicates within the same source snapshot before MERGE runs.
    # Delta MERGE expects each target row to match at most one source row.
    attribute_select = ",\n    ".join(attribute_columns)
    hash_expr = build_hash_expression(attribute_columns)

    return f"""
CREATE OR REPLACE TEMP VIEW {staged_view_name} AS
WITH ranked_source AS (
  SELECT
    {natural_key},
    {attribute_select},
    cast({effective_ts_column} AS timestamp) AS effective_ts,
    {hash_expr} AS hash_diff,
    row_number() OVER (
      PARTITION BY {natural_key}, cast({effective_ts_column} AS timestamp)
      ORDER BY _ingest_ts DESC
    ) AS rn
  FROM {source_table}
)
SELECT *
FROM ranked_source
WHERE rn = 1
""".strip()


def scd2_merge_sql(
    target_table: str,
    staged_view_name: str,
    natural_key: str,
    attribute_columns: list[str],
    surrogate_key_column: str,
) -> str:
    """Return a Delta MERGE statement for SCD2 dimensions.

    The merge uses two source rows for changed records: one row closes the current
    version, and another row inserts the new current version.
    """

    # The merge pattern below creates two logical source streams:
    # 1. a row with merge_key NULL to insert the new version when attributes changed
    # 2. a row with merge_key populated to update/close the current version
    insert_columns = [
        surrogate_key_column,
        natural_key,
        *attribute_columns,
        "valid_from",
        "valid_to",
        "is_current",
        "hash_diff",
        "created_at",
        "updated_at",
    ]
    insert_values = [
        "uuid()",
        f"s.{natural_key}",
        *[f"s.{column}" for column in attribute_columns],
        "s.effective_ts",
        "timestamp('9999-12-31 00:00:00')",
        "true",
        "s.hash_diff",
        "current_timestamp()",
        "current_timestamp()",
    ]
    insert_columns_sql = ", ".join(insert_columns)
    insert_values_sql = ", ".join(insert_values)

    return f"""
MERGE INTO {target_table} AS t
USING (
  SELECT
    NULL AS merge_key,
    src.*
  FROM {staged_view_name} src
  INNER JOIN {target_table} current_t
    ON src.{natural_key} = current_t.{natural_key}
   AND current_t.is_current = true
   AND src.hash_diff <> current_t.hash_diff

  UNION ALL

  SELECT
    src.{natural_key} AS merge_key,
    src.*
  FROM {staged_view_name} src
) AS s
ON t.{natural_key} = s.merge_key
AND t.is_current = true
WHEN MATCHED AND s.hash_diff <> t.hash_diff THEN UPDATE SET
  t.valid_to = s.effective_ts - INTERVAL 1 MICROSECOND,
  t.is_current = false,
  t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT ({insert_columns_sql})
VALUES ({insert_values_sql})
""".strip()
