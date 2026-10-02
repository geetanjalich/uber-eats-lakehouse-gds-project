"""Native Spark data-quality checks used by unit tests and notebooks.

The production notebook persists results and quarantined records. These functions focus
on the DataFrame-level failure logic so each rule can be tested with small fixtures.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def failed_orders_required_keys(orders_df: DataFrame) -> DataFrame:
    """Return orders missing required business keys."""

    # Required key failures make the order impossible to join reliably into facts and
    # dimensions, so these records are quarantined as critical failures.
    return orders_df.where(
        F.col("order_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("merchant_id").isNull()
        | F.col("city_id").isNull()
    )


def failed_orders_valid_time_sequence(orders_df: DataFrame) -> DataFrame:
    """Return delivered orders whose lifecycle timestamps are out of order."""

    # Only delivered orders are checked here. Cancelled or failed orders legitimately
    # may not have all downstream lifecycle timestamps populated.
    return orders_df.where(
        F.col("delivered_at").isNotNull()
        & (
            (F.col("accepted_at") < F.col("placed_at"))
            | (F.col("prepared_at") < F.col("accepted_at"))
            | (F.col("picked_up_at") < F.col("prepared_at"))
            | (F.col("delivered_at") < F.col("picked_up_at"))
        )
    )


def failed_payments_required_keys(payments_df: DataFrame) -> DataFrame:
    """Return payments missing required identifiers or currency."""

    # Payment identifiers and order linkage are mandatory because payment facts are
    # reconciled back to the order fact by order_id.
    return payments_df.where(
        F.col("payment_id").isNull()
        | F.col("order_id").isNull()
        | F.col("payment_status").isNull()
        | F.col("currency").isNull()
    )


def failed_payments_non_negative_amount(payments_df: DataFrame) -> DataFrame:
    """Return payments with negative monetary fields."""

    # Refunds are modeled separately, so negative captured amounts in the payment feed
    # are treated as source defects rather than financial adjustments.
    return payments_df.where(
        (F.col("subtotal_amount") < 0)
        | (F.col("delivery_fee") < 0)
        | (F.col("service_fee") < 0)
        | (F.col("tax_amount") < 0)
        | (F.col("total_amount") < 0)
    )


def failed_order_items_positive_amounts(order_items_df: DataFrame) -> DataFrame:
    """Return order items with invalid quantity, unit price, or line total."""

    # Item facts should never include zero or negative quantities/amounts because those
    # values distort basket-size and menu-item revenue metrics.
    return order_items_df.where(
        (F.col("quantity") <= 0) | (F.col("unit_price") <= 0) | (F.col("line_total") <= 0)
    )


def failed_orders_known_merchant(orders_df: DataFrame, merchant_changes_df: DataFrame) -> DataFrame:
    """Return orders whose merchant is absent from the merchant change feed."""

    # A left-anti join is the simplest Spark-native way to return only unmatched orders.
    known_merchants_df = merchant_changes_df.select("merchant_id").distinct()
    return orders_df.join(known_merchants_df, on="merchant_id", how="left_anti")


def quarantine_payload(failed_df: DataFrame, run_id: str, rule_id: str, source_table: str, severity: str) -> DataFrame:
    """Build the quarantine payload written to the DQ failed-record table."""

    # Failed rows can have different schemas depending on the rule. Storing the original
    # record as JSON gives analysts one common quarantine table while retaining detail.
    return (
        failed_df.withColumn("run_id", F.lit(run_id))
        .withColumn("rule_id", F.lit(rule_id))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("severity", F.lit(severity))
        .withColumn("failed_at", F.current_timestamp())
        .withColumn("source_record_json", F.to_json(F.struct(*[F.col(column) for column in failed_df.columns])))
        .select("run_id", "rule_id", "source_table", "severity", "failed_at", "source_record_json")
    )
