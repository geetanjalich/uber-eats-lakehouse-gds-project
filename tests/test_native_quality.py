from uber_eats_lakehouse.native_quality import (
    failed_order_items_positive_amounts,
    failed_orders_known_merchant,
    failed_orders_required_keys,
    failed_orders_valid_time_sequence,
    failed_payments_non_negative_amount,
    failed_payments_required_keys,
    quarantine_payload,
)


def test_order_quality_checks_find_missing_keys_and_bad_timestamps(spark):
    """Verify order-level DQ functions catch critical record defects."""

    # The fixture contains one valid order, one missing order_id, and one delivered
    # order where delivered_at is earlier than picked_up_at.
    orders_df = spark.sql(
        """
        SELECT * FROM VALUES
          ('order_1', 'cust_1', 'merch_1', 'nyc',
           timestamp('2026-06-01 10:00:00'), timestamp('2026-06-01 10:01:00'),
           timestamp('2026-06-01 10:15:00'), timestamp('2026-06-01 10:20:00'),
           timestamp('2026-06-01 10:45:00')),
          (NULL, 'cust_2', 'merch_1', 'nyc',
           timestamp('2026-06-01 11:00:00'), timestamp('2026-06-01 11:01:00'),
           timestamp('2026-06-01 11:15:00'), timestamp('2026-06-01 11:20:00'),
           timestamp('2026-06-01 11:40:00')),
          ('order_3', 'cust_3', 'merch_1', 'nyc',
           timestamp('2026-06-01 12:00:00'), timestamp('2026-06-01 12:01:00'),
           timestamp('2026-06-01 12:15:00'), timestamp('2026-06-01 12:20:00'),
           timestamp('2026-06-01 12:10:00'))
        AS orders(order_id, customer_id, merchant_id, city_id, placed_at, accepted_at,
                  prepared_at, picked_up_at, delivered_at)
        """
    )

    assert failed_orders_required_keys(orders_df).count() == 1
    assert failed_orders_valid_time_sequence(orders_df).count() == 1


def test_payment_and_order_item_quality_checks_find_invalid_amounts(spark):
    """Verify finance and item checks catch values that would corrupt metrics."""

    # Negative payment amounts and non-positive item quantities are critical because
    # they directly distort revenue, basket size, and item-performance facts.
    payments_df = spark.sql(
        """
        SELECT * FROM VALUES
          ('pay_1', 'order_1', 'captured', 'USD', 10.00, 2.00, 1.00, 0.80, 13.80),
          (NULL, 'order_2', 'captured', 'USD', 10.00, 2.00, 1.00, 0.80, 13.80),
          ('pay_3', 'order_3', 'captured', 'USD', -10.00, 2.00, 1.00, 0.80, -6.20)
        AS payments(payment_id, order_id, payment_status, currency, subtotal_amount,
                    delivery_fee, service_fee, tax_amount, total_amount)
        """
    )
    order_items_df = spark.sql(
        """
        SELECT * FROM VALUES
          ('order_1', 1, 2, 10.00, 20.00),
          ('order_2', 1, 0, 10.00, 0.00)
        AS order_items(order_id, line_number, quantity, unit_price, line_total)
        """
    )

    assert failed_payments_required_keys(payments_df).count() == 1
    assert failed_payments_non_negative_amount(payments_df).count() == 1
    assert failed_order_items_positive_amounts(order_items_df).count() == 1


def test_referential_quality_and_quarantine_payload(spark):
    """Verify referential failures are converted into durable quarantine payloads."""

    # One order points at an unknown merchant. The quality helper should return that
    # order and preserve the full failed row as JSON for later analysis.
    orders_df = spark.createDataFrame(
        [
            {"order_id": "order_1", "merchant_id": "merch_1"},
            {"order_id": "order_2", "merchant_id": "unknown_merchant"},
        ]
    )
    merchants_df = spark.createDataFrame([{"merchant_id": "merch_1"}])

    failed_df = failed_orders_known_merchant(orders_df, merchants_df)
    quarantined_df = quarantine_payload(
        failed_df,
        run_id="run_1",
        rule_id="orders_known_merchant",
        source_table="catalog.silver.orders",
        severity="critical",
    )

    payload = quarantined_df.collect()[0]

    # These assertions confirm both the failed-record count and the metadata required
    # by Genie/SQL investigation views.
    assert failed_df.count() == 1
    assert payload["run_id"] == "run_1"
    assert payload["rule_id"] == "orders_known_merchant"
    assert "unknown_merchant" in payload["source_record_json"]
