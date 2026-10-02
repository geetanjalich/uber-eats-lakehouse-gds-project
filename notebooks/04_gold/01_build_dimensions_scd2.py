# Databricks notebook source
# This notebook builds slowly changing dimensions. Each dimension preserves history so
# facts can resolve the business attributes that were current when an event occurred.

# COMMAND ----------

import sys
from pathlib import Path

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# The bundle passes the catalog at runtime. The same notebook can therefore build
# isolated dev and prod dimensions inside one Databricks workspace.
workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = Path(f"/Workspace{workspace_path}")
project_root = workspace_file.parents[2]

for candidate in [str(project_root), str(project_root / "src"), str(Path.cwd()), str(Path.cwd() / "src")]:
    # Import resolution points at the uploaded bundle files first, then local fallbacks
    # for direct notebook experimentation.
    if candidate not in sys.path:
        sys.path.append(candidate)

from uber_eats_lakehouse.scd2 import scd2_merge_sql, scd2_staged_view_sql

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_customer (
  customer_sk STRING,
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email_hash STRING,
  phone_hash STRING,
  home_city_id STRING,
  loyalty_tier STRING,
  marketing_opt_in BOOLEAN,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  hash_diff STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
"""
)

# Customer attributes are tracked historically because loyalty tier and opt-in status
# can change after an order was placed.
spark.sql(
    scd2_staged_view_sql(
        source_table=f"{catalog}.silver.customer_changes",
        natural_key="customer_id",
        attribute_columns=[
            "first_name",
            "last_name",
            "email_hash",
            "phone_hash",
            "home_city_id",
            "loyalty_tier",
            "marketing_opt_in",
        ],
        effective_ts_column="effective_from_ts",
        staged_view_name="staged_dim_customer",
    )
)
spark.sql(
    scd2_merge_sql(
        target_table=f"{catalog}.gold.dim_customer",
        staged_view_name="staged_dim_customer",
        natural_key="customer_id",
        attribute_columns=[
            "first_name",
            "last_name",
            "email_hash",
            "phone_hash",
            "home_city_id",
            "loyalty_tier",
            "marketing_opt_in",
        ],
        surrogate_key_column="customer_sk",
    )
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_merchant (
  merchant_sk STRING,
  merchant_id STRING,
  merchant_name STRING,
  city_id STRING,
  cuisine_type STRING,
  commission_rate DECIMAL(8,4),
  is_active BOOLEAN,
  merchant_tier STRING,
  avg_prep_minutes INT,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  hash_diff STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
"""
)

# Merchant commission, tier, and prep speed affect marketplace economics, so facts
# should join to the version that was active at order time.
spark.sql(
    scd2_staged_view_sql(
        source_table=f"{catalog}.silver.merchant_changes",
        natural_key="merchant_id",
        attribute_columns=[
            "merchant_name",
            "city_id",
            "cuisine_type",
            "commission_rate",
            "is_active",
            "merchant_tier",
            "avg_prep_minutes",
        ],
        effective_ts_column="effective_from_ts",
        staged_view_name="staged_dim_merchant",
    )
)
spark.sql(
    scd2_merge_sql(
        target_table=f"{catalog}.gold.dim_merchant",
        staged_view_name="staged_dim_merchant",
        natural_key="merchant_id",
        attribute_columns=[
            "merchant_name",
            "city_id",
            "cuisine_type",
            "commission_rate",
            "is_active",
            "merchant_tier",
            "avg_prep_minutes",
        ],
        surrogate_key_column="merchant_sk",
    )
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_courier (
  courier_sk STRING,
  courier_id STRING,
  home_city_id STRING,
  vehicle_type STRING,
  signup_channel STRING,
  is_active BOOLEAN,
  delivery_mode STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  hash_diff STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
"""
)

# Courier delivery mode and active status can change. Preserving versions allows
# delivery analytics to explain historical operations accurately.
spark.sql(
    scd2_staged_view_sql(
        source_table=f"{catalog}.silver.courier_changes",
        natural_key="courier_id",
        attribute_columns=["home_city_id", "vehicle_type", "signup_channel", "is_active", "delivery_mode"],
        effective_ts_column="effective_from_ts",
        staged_view_name="staged_dim_courier",
    )
)
spark.sql(
    scd2_merge_sql(
        target_table=f"{catalog}.gold.dim_courier",
        staged_view_name="staged_dim_courier",
        natural_key="courier_id",
        attribute_columns=["home_city_id", "vehicle_type", "signup_channel", "is_active", "delivery_mode"],
        surrogate_key_column="courier_sk",
    )
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_menu_item (
  menu_item_sk STRING,
  menu_item_id STRING,
  merchant_id STRING,
  item_name STRING,
  category STRING,
  base_price DECIMAL(12,2),
  is_available BOOLEAN,
  tax_category STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOLEAN,
  hash_diff STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
"""
)

# Menu item pricing and availability are modeled as changing attributes so item facts
# can retain the correct historical product context.
spark.sql(
    scd2_staged_view_sql(
        source_table=f"{catalog}.silver.menu_item_changes",
        natural_key="menu_item_id",
        attribute_columns=["merchant_id", "item_name", "category", "base_price", "is_available", "tax_category"],
        effective_ts_column="effective_from_ts",
        staged_view_name="staged_dim_menu_item",
    )
)
spark.sql(
    scd2_merge_sql(
        target_table=f"{catalog}.gold.dim_menu_item",
        staged_view_name="staged_dim_menu_item",
        natural_key="menu_item_id",
        attribute_columns=["merchant_id", "item_name", "category", "base_price", "is_available", "tax_category"],
        surrogate_key_column="menu_item_sk",
    )
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_city
USING DELTA AS
SELECT * FROM VALUES
  ('nyc', 'New York', 'US', 'America/New_York'),
  ('sfo', 'San Francisco', 'US', 'America/Los_Angeles'),
  ('chi', 'Chicago', 'US', 'America/Chicago'),
  ('sea', 'Seattle', 'US', 'America/Los_Angeles'),
  ('aus', 'Austin', 'US', 'America/Chicago')
AS dim_city(city_id, city_name, country, timezone_name)
"""
)

# Date and time dimensions are recreated because they are deterministic reference data.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_date
USING DELTA AS
SELECT
  explode(sequence(date('2026-01-01'), date('2027-12-31'), interval 1 day)) AS calendar_date
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.dim_time
USING DELTA AS
SELECT
  hour_value AS hour_of_day,
  CASE
    WHEN hour_value BETWEEN 6 AND 10 THEN 'breakfast'
    WHEN hour_value BETWEEN 11 AND 14 THEN 'lunch'
    WHEN hour_value BETWEEN 17 AND 21 THEN 'dinner'
    ELSE 'off_peak'
  END AS daypart
FROM range(0, 24) AS t(hour_value)
"""
)

display(spark.sql(f"SHOW TABLES IN {catalog}.gold LIKE 'dim*'"))
