from decimal import Decimal

from uber_eats_lakehouse.gold_transforms import build_fact_order


def test_build_fact_order_resolves_scd2_dimension_by_order_timestamp(spark):
    """Verify order facts join to the historical dimension row active at order time."""

    # The order is placed after the second customer version starts. The transform should
    # select `cust_sk_current`, proving facts do not blindly use the earliest dimension.
    orders_df = spark.sql(
        """
        SELECT
          'order_1' AS order_id,
          'cust_1' AS customer_id,
          'merch_1' AS merchant_id,
          'courier_1' AS courier_id,
          'nyc' AS city_id,
          'delivered' AS order_status,
          timestamp('2026-06-15 10:00:00') AS placed_at,
          timestamp('2026-06-15 10:02:00') AS accepted_at,
          timestamp('2026-06-15 10:15:00') AS prepared_at,
          timestamp('2026-06-15 10:20:00') AS picked_up_at,
          timestamp('2026-06-15 10:45:00') AS delivered_at,
          cast(20.00 AS decimal(12,2)) AS subtotal_amount,
          cast(0.00 AS decimal(12,2)) AS discount_amount,
          45 AS estimated_delivery_minutes
        """
    )
    payments_df = spark.sql(
        """
        SELECT
          'pay_1' AS payment_id,
          'order_1' AS order_id,
          cast(3.00 AS decimal(12,2)) AS delivery_fee,
          cast(2.00 AS decimal(12,2)) AS service_fee,
          cast(1.50 AS decimal(12,2)) AS tax_amount,
          cast(26.50 AS decimal(12,2)) AS total_amount
        """
    )
    dim_customer_df = spark.sql(
        """
        SELECT 'cust_sk_old' AS customer_sk, 'cust_1' AS customer_id,
               timestamp('2026-01-01 00:00:00') AS valid_from,
               timestamp('2026-06-10 00:00:00') AS valid_to
        UNION ALL
        SELECT 'cust_sk_current' AS customer_sk, 'cust_1' AS customer_id,
               timestamp('2026-06-10 00:00:00') AS valid_from,
               timestamp('9999-12-31 00:00:00') AS valid_to
        """
    )
    dim_merchant_df = spark.sql(
        """
        SELECT 'merchant_sk_1' AS merchant_sk, 'merch_1' AS merchant_id,
               timestamp('2026-01-01 00:00:00') AS valid_from,
               timestamp('9999-12-31 00:00:00') AS valid_to
        """
    )
    dim_courier_df = spark.sql(
        """
        SELECT 'courier_sk_1' AS courier_sk, 'courier_1' AS courier_id,
               timestamp('2026-01-01 00:00:00') AS valid_from,
               timestamp('9999-12-31 00:00:00') AS valid_to
        """
    )

    fact = build_fact_order(
        orders_df,
        payments_df,
        dim_customer_df,
        dim_merchant_df,
        dim_courier_df,
    ).collect()[0]

    # The assertions cover temporal dimension lookup, delivery-duration derivation,
    # SLA calculation, and payment enrichment in one focused fact-level scenario.
    assert fact["customer_sk"] == "cust_sk_current"
    assert fact["merchant_sk"] == "merchant_sk_1"
    assert fact["courier_sk"] == "courier_sk_1"
    assert fact["actual_delivery_minutes"] == 45
    assert fact["delivered_within_sla"] is True
    assert fact["total_amount"] == Decimal("26.50")
