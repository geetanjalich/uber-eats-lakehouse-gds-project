# Databricks notebook source
# Bronze ingestion uses Auto Loader in incremental batch mode. Each dataset has its own
# checkpoint and schema tracking directory so re-execution does not duplicate rows.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# The job passes the target catalog at runtime. All volume paths are derived from that
# catalog so the same notebook works for dev and prod deployments.
landing_root = f"/Volumes/{catalog}/bronze/landing_volume"
checkpoint_root = f"/Volumes/{catalog}/bronze/checkpoint_volume"
schema_root = f"/Volumes/{catalog}/bronze/schema_volume"

# JSON feeds represent operational events and snapshots. Each dataset is processed
# independently to isolate schema evolution and checkpoint state.
json_datasets = [
    "orders",
    "order_events",
    "order_items",
    "refunds",
    "ratings",
    "courier_locations",
    "support_tickets",
    "merchant_snapshots",
    "customer_snapshots",
    "courier_snapshots",
    "menu_item_snapshots",
]

# Payments intentionally use CSV to demonstrate that Auto Loader can support
# dataset-specific file formats inside the same workflow.
csv_datasets = ["payments"]

# COMMAND ----------

def ingest_json_dataset(dataset_name: str) -> None:
    """Ingest one JSON source dataset into a raw Delta table."""

    # Each dataset has a dedicated checkpoint. Once a file is committed, a later
    # workflow run will skip it and process only newly uploaded date folders.
    source_path = f"{landing_root}/{dataset_name}"
    target_table = f"{catalog}.bronze.{dataset_name}_raw"
    checkpoint_path = f"{checkpoint_root}/autoloader/{dataset_name}"
    schema_path = f"{schema_root}/autoloader/{dataset_name}"

    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        # `addNewColumns` handles late-arriving fields such as app_version without
        # requiring a manual bronze table rebuild.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Malformed or unexpected payload fragments are preserved for investigation
        # instead of being silently dropped.
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_dataset", F.lit(dataset_name))
    )

    (
        raw_df.writeStream.option("checkpointLocation", checkpoint_path)
        # availableNow gives incremental batch behavior: run until current files are
        # consumed, then stop. It is not a continuously running streaming job.
        .trigger(availableNow=True)
        .toTable(target_table)
    )


def ingest_csv_dataset(dataset_name: str) -> None:
    """Ingest one CSV source dataset into a raw Delta table."""

    # CSV ingestion follows the same checkpoint pattern as JSON so re-execution is
    # deterministic across both source formats.
    source_path = f"{landing_root}/{dataset_name}"
    target_table = f"{catalog}.bronze.{dataset_name}_raw"
    checkpoint_path = f"{checkpoint_root}/autoloader/{dataset_name}"
    schema_path = f"{schema_root}/autoloader/{dataset_name}"

    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_dataset", F.lit(dataset_name))
    )

    (
        raw_df.writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )


# COMMAND ----------

for dataset in json_datasets:
    # Databricks executes these source ingestions sequentially inside this notebook.
    # Parallelism is controlled at the job-task level when separate notebooks/tasks are
    # declared in the bundle.
    ingest_json_dataset(dataset)

for dataset in csv_datasets:
    ingest_csv_dataset(dataset)

# COMMAND ----------

display(
    spark.sql(
        f"""
SELECT table_schema, table_name
FROM {catalog}.information_schema.tables
WHERE table_schema = 'bronze'
ORDER BY table_name
"""
    )
)
