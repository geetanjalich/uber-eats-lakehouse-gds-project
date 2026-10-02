"""Spark DataFrame transformations for gold facts.

These functions contain the join and derivation logic that should be unit tested
outside Databricks. Notebook code decides where to read and write tables; this module
decides how trusted facts are shaped.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_fact_order(
    orders_df: DataFrame,
    payments_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_merchant_df: DataFrame,
    dim_courier_df: DataFrame,
) -> DataFrame:
    """Build the order fact with temporal SCD2 dimension lookups.

    The output grain is one row per order. Dimension keys are resolved using the order
    placement timestamp so facts retain historical attribute context.
    """

    # The SCD2 joins use the order placement timestamp, not the current dimension row.
    # This is the core historical lookup that prevents facts from being restated when
    # a customer, merchant, or courier attribute changes later.
    joined_df = (
        orders_df.alias("o")
        .join(payments_df.alias("p"), F.col("o.order_id") == F.col("p.order_id"), "left")
        .join(
            dim_customer_df.alias("c"),
            (F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.col("o.placed_at").between(F.col("c.valid_from"), F.col("c.valid_to"))),
            "left",
        )
        .join(
            dim_merchant_df.alias("m"),
            (F.col("o.merchant_id") == F.col("m.merchant_id"))
            & (F.col("o.placed_at").between(F.col("m.valid_from"), F.col("m.valid_to"))),
            "left",
        )
        .join(
            dim_courier_df.alias("d"),
            (F.col("o.courier_id") == F.col("d.courier_id"))
            & (F.col("o.placed_at").between(F.col("d.valid_from"), F.col("d.valid_to"))),
            "left",
        )
        .where(F.col("o.order_id").isNotNull())
    )

    # The fact keeps surrogate keys for analytic joins and natural keys for
    # traceability back to silver/source records.
    fact_df = joined_df.select(
        F.col("o.order_id"),
        F.col("c.customer_sk"),
        F.col("m.merchant_sk"),
        F.col("d.courier_sk"),
        F.col("o.customer_id"),
        F.col("o.merchant_id"),
        F.col("o.courier_id"),
        F.col("o.city_id"),
        F.to_date(F.col("o.placed_at")).alias("order_date"),
        F.hour(F.col("o.placed_at")).alias("order_hour"),
        F.col("o.order_status"),
        F.col("o.placed_at"),
        F.col("o.accepted_at"),
        F.col("o.prepared_at"),
        F.col("o.picked_up_at"),
        F.col("o.delivered_at"),
        F.col("o.subtotal_amount"),
        F.col("o.discount_amount"),
        F.col("p.delivery_fee"),
        F.col("p.service_fee"),
        F.col("p.tax_amount"),
        F.col("p.total_amount"),
        # Delivery duration is derived from event timestamps so SLA calculations are
        # reproducible and do not depend on precomputed source fields.
        (
            (F.col("o.delivered_at").cast("long") - F.col("o.placed_at").cast("long")) / F.lit(60)
        ).cast("int").alias("actual_delivery_minutes"),
        F.col("o.estimated_delivery_minutes"),
        F.when(F.col("o.delivered_at").isNull(), F.lit(False))
        .when(
            ((F.col("o.delivered_at").cast("long") - F.col("o.placed_at").cast("long")) / F.lit(60))
            <= F.col("o.estimated_delivery_minutes") + F.lit(10),
            F.lit(True),
        )
        .otherwise(F.lit(False))
        .alias("delivered_within_sla"),
        F.current_timestamp().alias("gold_updated_at"),
    )

    # The output grain is one row per order. This final guard protects gold from
    # accidental duplicate rows created by joins or replayed source data.
    latest_window = Window.partitionBy("order_id").orderBy(F.col("placed_at").desc())
    return fact_df.withColumn("_rn", F.row_number().over(latest_window)).where("_rn = 1").drop("_rn")
