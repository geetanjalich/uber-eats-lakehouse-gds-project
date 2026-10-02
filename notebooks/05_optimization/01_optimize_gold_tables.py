# Databricks notebook source
# This notebook applies Databricks table optimizations to the trusted serving layer.
# Permission-sensitive commands are wrapped so Free Edition users can still complete
# the pipeline when a feature is unavailable in their workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# These commands are meant for initial setup or an explicit maintenance run, not for
# every incremental ingestion run. Predictive Optimization, once enabled, lets
# Databricks manage future maintenance where the workspace supports it.
optimization_statements = [
    # Liquid clustering definitions describe how Delta should organize data for common
    # query filters. The actual background maintenance depends on workspace capability.
    f"ALTER TABLE {catalog}.gold.fact_order CLUSTER BY (order_date, city_id, merchant_id)",
    f"ALTER TABLE {catalog}.gold.fact_delivery CLUSTER BY (delivery_date, city_id, courier_id)",
    f"ALTER TABLE {catalog}.gold.fact_payment CLUSTER BY (payment_date, merchant_sk)",
    f"ALTER TABLE {catalog}.gold.agg_merchant_daily_performance CLUSTER BY (order_date, merchant_id)",
    # OPTIMIZE is included for an explicit first compaction after table creation.
    f"OPTIMIZE {catalog}.gold.fact_order",
    f"OPTIMIZE {catalog}.gold.fact_delivery",
    f"OPTIMIZE {catalog}.gold.fact_payment",
    f"OPTIMIZE {catalog}.gold.agg_merchant_daily_performance",
    # Predictive Optimization is enabled once per table when supported. After that,
    # Databricks decides when maintenance is needed.
    f"ALTER TABLE {catalog}.gold.fact_order ENABLE PREDICTIVE OPTIMIZATION",
    f"ALTER TABLE {catalog}.gold.fact_delivery ENABLE PREDICTIVE OPTIMIZATION",
    f"ALTER TABLE {catalog}.gold.agg_merchant_daily_performance ENABLE PREDICTIVE OPTIMIZATION",
    # UniForm exposes selected Delta tables in Iceberg-compatible format for engines
    # outside Databricks that can read the same table layout.
    f"""
ALTER TABLE {catalog}.gold.fact_order SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
""",
    f"""
ALTER TABLE {catalog}.gold.agg_merchant_daily_performance SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
""",
]

# COMMAND ----------

for statement in optimization_statements:
    try:
        # Free Edition feature availability can vary. Skipping rejected statements keeps
        # the project runnable while still showing the production-grade commands.
        print(f"Running: {statement}")
        spark.sql(statement)
    except Exception as exc:
        print(f"Skipped optimization statement because the workspace rejected it: {exc}")

display(
    spark.sql(
        f"""
SELECT 'fact_order' AS table_name, count(*) AS rows FROM {catalog}.gold.fact_order
UNION ALL
SELECT 'fact_delivery' AS table_name, count(*) AS rows FROM {catalog}.gold.fact_delivery
UNION ALL
SELECT 'agg_merchant_daily_performance' AS table_name, count(*) AS rows
FROM {catalog}.gold.agg_merchant_daily_performance
"""
    )
)
