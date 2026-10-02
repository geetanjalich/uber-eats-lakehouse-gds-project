"""Spark DataFrame transformations for the silver layer.

These functions mirror the logic used by the Databricks silver notebook. Keeping the
logic as plain functions makes it practical to unit test schema casting, duplicate
handling, and business normalization without adding an OOP framework.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def with_missing_columns(df: DataFrame, defaults: dict[str, object]) -> DataFrame:
    """Add optional source columns when older files do not contain them."""

    # Auto Loader can discover new columns after earlier files have already landed.
    # This helper lets downstream selects remain stable across old and new batches.
    enriched_df = df
    for column_name, default_value in defaults.items():
        if column_name not in enriched_df.columns:
            enriched_df = enriched_df.withColumn(column_name, F.lit(default_value))
    return enriched_df


def clean_orders(raw_orders_df: DataFrame) -> DataFrame:
    """Standardize raw order records and keep the latest row per event ID."""

    # `app_version` and `order_surface` are schema-evolution fields added by the
    # generator after the first few dates. `_rescued_data` captures malformed payloads.
    prepared_df = with_missing_columns(
        raw_orders_df,
        {
            "app_version": None,
            "order_surface": None,
            "_rescued_data": None,
        },
    )

    # Silver applies explicit types instead of relying on whatever Auto Loader inferred
    # from the current files. That gives gold stable contracts for timestamps, money,
    # integer flags, and lineage columns.
    typed_df = prepared_df.select(
        F.col("event_id").cast("string").alias("event_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.col("courier_id").cast("string").alias("courier_id"),
        F.col("city_id").cast("string").alias("city_id"),
        F.col("order_status").cast("string").alias("order_status"),
        F.to_timestamp("placed_at").alias("placed_at"),
        F.to_timestamp("accepted_at").alias("accepted_at"),
        F.to_timestamp("prepared_at").alias("prepared_at"),
        F.to_timestamp("picked_up_at").alias("picked_up_at"),
        F.to_timestamp("delivered_at").alias("delivered_at"),
        F.col("currency").cast("string").alias("currency"),
        F.col("subtotal_amount").cast("decimal(12,2)").alias("subtotal_amount"),
        F.col("discount_amount").cast("decimal(12,2)").alias("discount_amount"),
        F.col("estimated_delivery_minutes").cast("int").alias("estimated_delivery_minutes"),
        F.col("schema_version").cast("int").alias("schema_version"),
        F.col("app_version").cast("string").alias("app_version"),
        F.col("order_surface").cast("string").alias("order_surface"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file").cast("string").alias("_source_file"),
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
    )

    # If a source file is replayed or a corrected record arrives with the same event
    # id, the newest ingested record wins. The merge key in the notebook mirrors this.
    latest_window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc())
    return typed_df.withColumn("_rn", F.row_number().over(latest_window)).where("_rn = 1").drop("_rn")


def clean_payments(raw_payments_df: DataFrame) -> DataFrame:
    """Standardize payment records and keep the latest row per payment ID."""

    # Payment fields are cast to fixed decimals because binary floating point is not
    # suitable for financial reporting or quality checks on amounts.
    typed_df = raw_payments_df.select(
        F.col("payment_id").cast("string").alias("payment_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.col("payment_status").cast("string").alias("payment_status"),
        F.col("payment_method").cast("string").alias("payment_method"),
        F.col("subtotal_amount").cast("decimal(12,2)").alias("subtotal_amount"),
        F.col("delivery_fee").cast("decimal(12,2)").alias("delivery_fee"),
        F.col("service_fee").cast("decimal(12,2)").alias("service_fee"),
        F.col("tax_amount").cast("decimal(12,2)").alias("tax_amount"),
        F.col("discount_amount").cast("decimal(12,2)").alias("discount_amount"),
        F.col("total_amount").cast("decimal(12,2)").alias("total_amount"),
        F.to_timestamp("authorized_at").alias("authorized_at"),
        F.to_timestamp("captured_at").alias("captured_at"),
        F.col("currency").cast("string").alias("currency"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file").cast("string").alias("_source_file"),
    )

    # The latest payment row can represent progression from authorized to captured,
    # voided, or refunded depending on the order outcome.
    latest_window = Window.partitionBy("payment_id").orderBy(F.col("_ingest_ts").desc())
    return typed_df.withColumn("_rn", F.row_number().over(latest_window)).where("_rn = 1").drop("_rn")


def clean_order_items(raw_order_items_df: DataFrame) -> DataFrame:
    """Standardize order-line records and keep one row per order line."""

    # The natural grain for order items is order_id plus line_number. Preserving this
    # grain is important before building item-level revenue facts.
    typed_df = raw_order_items_df.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("line_number").cast("int").alias("line_number"),
        F.col("menu_item_id").cast("string").alias("menu_item_id"),
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.col("line_total").cast("decimal(12,2)").alias("line_total"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file").cast("string").alias("_source_file"),
    )

    # A corrected line item should replace the older version for the same order line.
    latest_window = Window.partitionBy("order_id", "line_number").orderBy(F.col("_ingest_ts").desc())
    return typed_df.withColumn("_rn", F.row_number().over(latest_window)).where("_rn = 1").drop("_rn")
