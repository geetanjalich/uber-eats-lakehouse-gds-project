# Databricks notebook source
# This notebook creates the Unity Catalog objects used by the marketplace lakehouse.
# It is intentionally idempotent so a failed pipeline can be executed again without
# manual cleanup.

# COMMAND ----------

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# The bundle passes a dev or prod catalog name into this widget. In Databricks Free
# Edition both environments can live in one workspace while remaining logically
# separated by catalog naming.
schemas = ["bronze", "silver", "gold", "dq", "audit", "config"]
bronze_volumes = ["landing_volume", "checkpoint_volume", "schema_volume", "artifact_volume"]

# COMMAND ----------

# Some Free Edition workspaces allow catalog creation; others expect use of an
# existing catalog. If creation is blocked, set the widget to an existing catalog
# where you have CREATE SCHEMA and CREATE VOLUME privileges.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as exc:
    print(f"Catalog creation skipped for {catalog}: {exc}")

spark.sql(f"USE CATALOG {catalog}")

for schema_name in schemas:
    # Schemas mirror the lakehouse responsibility boundaries: raw ingestion, refined
    # records, analytic serving, quality/audit state, and configuration artifacts.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

for volume_name in bronze_volumes:
    # Volumes store files and state that are outside Delta tables: source landing files,
    # Auto Loader checkpoints, evolving schemas, and generated artifacts.
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.{volume_name}")

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.audit.pipeline_watermarks (
  pipeline_name STRING COMMENT 'Logical pipeline name.',
  target_table STRING COMMENT 'Table controlled by the watermark.',
  last_processed_ts TIMESTAMP COMMENT 'Largest ingestion timestamp processed successfully.',
  updated_at TIMESTAMP COMMENT 'Timestamp when the watermark was updated.'
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.audit.pipeline_runs (
  run_id STRING COMMENT 'Unique identifier for each pipeline execution.',
  task_name STRING COMMENT 'Databricks task or notebook name.',
  status STRING COMMENT 'started, succeeded, failed, or skipped.',
  started_at TIMESTAMP COMMENT 'Task start timestamp.',
  finished_at TIMESTAMP COMMENT 'Task finish timestamp.',
  details STRING COMMENT 'JSON or text payload with operational detail.'
)
USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.dq.dq_run_summary (
  run_id STRING,
  dq_scope STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  total_rules INT,
  failed_rules INT,
  critical_failed_rules INT,
  status STRING
)
USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.dq.dq_rule_results (
  run_id STRING,
  rule_id STRING,
  source_table STRING,
  severity STRING,
  description STRING,
  failed_record_count BIGINT,
  status STRING,
  checked_at TIMESTAMP
)
USING DELTA
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {catalog}.dq.dq_failed_records (
  run_id STRING,
  rule_id STRING,
  source_table STRING,
  severity STRING,
  failed_at TIMESTAMP,
  source_record_json STRING
)
USING DELTA
"""
)

# A small visual confirmation is useful when running the notebook directly from the UI.
display(
    spark.sql(
        f"""
SELECT 'catalog_ready' AS setup_status, '{catalog}' AS catalog_name, current_timestamp() AS checked_at
"""
    )
)
