# Databricks notebook source
# Silver tables standardize datatypes, remove exact duplicates, and expose conformed
# business entities. The core order, payment, and order-item transforms live in
# src/ so local Spark tests validate the same logic deployed to Databricks.

# COMMAND ----------

import sys
from pathlib import Path

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# Bundle deployment uploads notebooks under the workspace bundle folder. The next few
# lines derive that uploaded project root so imports work from Databricks UI runs and
# bundle-triggered job runs.
workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = Path(f"/Workspace{workspace_path}")
project_root = workspace_file.parents[2]

for candidate in [str(project_root), str(project_root / "src"), str(Path.cwd()), str(Path.cwd() / "src")]:
    # The fallback entries support local Databricks notebook testing where the current
    # working directory can differ from the bundle workspace path.
    if candidate not in sys.path:
        sys.path.append(candidate)

from uber_eats_lakehouse.silver_transforms import clean_order_items, clean_orders, clean_payments

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    """Check whether a table exists without relying on workspace-specific catalog APIs."""

    # Querying a single row is slower than metadata lookup, but it works consistently
    # across Free Edition workspace variations and keeps the notebook portable.
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def merge_view_into_table(view_name: str, target_table: str, key_columns: list[str]) -> None:
    """Create a Delta target if needed, then merge rows from a temp view."""

    # The first run creates an empty Delta table from the view schema. Later runs merge
    # incrementally by the business key, allowing corrected records to overwrite older
    # silver rows.
    if not table_exists(target_table):
        spark.sql(f"CREATE TABLE {target_table} USING DELTA AS SELECT * FROM {view_name} WHERE 1 = 0")

    # Null-safe equality protects composite keys if a nullable component appears in a
    # source correction, while still avoiding duplicate inserts.
    join_expr = " AND ".join([f"t.{column} <=> s.{column}" for column in key_columns])
    spark.sql(
        f"""
MERGE INTO {target_table} t
USING {view_name} s
ON {join_expr}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
    )

# COMMAND ----------

clean_orders(spark.table(f"{catalog}.bronze.orders_raw")).createOrReplaceTempView("v_orders_clean")
merge_view_into_table("v_orders_clean", f"{catalog}.silver.orders", ["event_id"])

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW v_order_events_clean AS
SELECT
  cast(event_id AS STRING) AS event_id,
  cast(order_id AS STRING) AS order_id,
  cast(status AS STRING) AS status,
  to_timestamp(event_ts) AS event_ts,
  cast(actor_type AS STRING) AS actor_type,
  _ingest_ts,
  _source_file
FROM {catalog}.bronze.order_events_raw
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY _ingest_ts DESC) = 1
"""
)
# Event streams are standardized in SQL here because the logic is simple and keeping it
# visible in the notebook helps readers compare raw and refined shapes.
merge_view_into_table("v_order_events_clean", f"{catalog}.silver.order_status_events", ["event_id"])

# COMMAND ----------

clean_order_items(spark.table(f"{catalog}.bronze.order_items_raw")).createOrReplaceTempView(
    "v_order_items_clean"
)
merge_view_into_table("v_order_items_clean", f"{catalog}.silver.order_items", ["order_id", "line_number"])

# COMMAND ----------

clean_payments(spark.table(f"{catalog}.bronze.payments_raw")).createOrReplaceTempView("v_payments_clean")
merge_view_into_table("v_payments_clean", f"{catalog}.silver.payments", ["payment_id"])

# COMMAND ----------

for source_name, target_name, natural_key in [
    ("merchant_snapshots_raw", "merchant_changes", "merchant_id"),
    ("customer_snapshots_raw", "customer_changes", "customer_id"),
    ("courier_snapshots_raw", "courier_changes", "courier_id"),
    ("menu_item_snapshots_raw", "menu_item_changes", "menu_item_id"),
]:
    # Snapshot feeds become silver change tables. Gold SCD2 logic consumes these
    # changes and decides whether a new historical dimension row is needed.
    spark.sql(
        f"""
CREATE OR REPLACE TEMP VIEW v_{target_name} AS
SELECT
  *,
  to_timestamp(effective_ts) AS effective_from_ts
FROM {catalog}.bronze.{source_name}
QUALIFY row_number() OVER (
  PARTITION BY {natural_key}, to_timestamp(effective_ts)
  ORDER BY _ingest_ts DESC
) = 1
"""
    )
    merge_view_into_table(f"v_{target_name}", f"{catalog}.silver.{target_name}", [natural_key, "effective_from_ts"])

# COMMAND ----------

for source_name, target_name, key_column in [
    ("refunds_raw", "refunds", "refund_id"),
    ("ratings_raw", "ratings", "rating_id"),
    ("support_tickets_raw", "support_tickets", "ticket_id"),
]:
    # Sparse exception feeds use straightforward latest-record merges by their own
    # source identifiers.
    spark.sql(
        f"""
CREATE OR REPLACE TEMP VIEW v_{target_name} AS
SELECT *
FROM {catalog}.bronze.{source_name}
QUALIFY row_number() OVER (PARTITION BY {key_column} ORDER BY _ingest_ts DESC) = 1
"""
    )
    merge_view_into_table(f"v_{target_name}", f"{catalog}.silver.{target_name}", [key_column])

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW v_courier_locations AS
SELECT
  cast(courier_id AS STRING) AS courier_id,
  cast(order_id AS STRING) AS order_id,
  to_timestamp(event_ts) AS event_ts,
  cast(latitude AS DOUBLE) AS latitude,
  cast(longitude AS DOUBLE) AS longitude,
  cast(speed_mph AS DOUBLE) AS speed_mph,
  cast(battery_pct AS INT) AS battery_pct,
  _ingest_ts,
  _source_file
FROM {catalog}.bronze.courier_locations_raw
QUALIFY row_number() OVER (
  PARTITION BY courier_id, order_id, to_timestamp(event_ts)
  ORDER BY _ingest_ts DESC
) = 1
"""
)
# Courier telemetry has a naturally high row count, so silver keeps only clean typed
# points at the finest grain. Gold will aggregate this for efficient analysis.
merge_view_into_table(
    "v_courier_locations",
    f"{catalog}.silver.courier_locations",
    ["courier_id", "order_id", "event_ts"],
)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.silver"))
