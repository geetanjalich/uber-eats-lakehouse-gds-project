from decimal import Decimal

from pyspark.sql import functions as F

from uber_eats_lakehouse.silver_transforms import clean_order_items, clean_orders, clean_payments


def test_clean_orders_deduplicates_latest_event_and_casts_types(spark):
    """Verify the silver order contract used before DQ and gold publication."""

    # Two source rows with the same event_id simulate a corrected/replayed order event.
    # The transform must retain the newest ingestion record and apply stable data types.
    raw_df = spark.createDataFrame(
        [
            {
                "event_id": "evt_1",
                "order_id": "order_1",
                "customer_id": "cust_1",
                "merchant_id": "merch_1",
                "courier_id": "courier_1",
                "city_id": "nyc",
                "order_status": "delivered",
                "placed_at": "2026-06-01T10:00:00Z",
                "accepted_at": "2026-06-01T10:02:00Z",
                "prepared_at": "2026-06-01T10:15:00Z",
                "picked_up_at": "2026-06-01T10:20:00Z",
                "delivered_at": "2026-06-01T10:40:00Z",
                "currency": "USD",
                "subtotal_amount": "25.123",
                "discount_amount": "2.5",
                "estimated_delivery_minutes": "35",
                "schema_version": "1",
                "_ingest_ts": "2026-06-01T11:00:00Z",
                "_source_file": "older.jsonl",
            },
            {
                "event_id": "evt_1",
                "order_id": "order_1",
                "customer_id": "cust_1",
                "merchant_id": "merch_1",
                "courier_id": "courier_1",
                "city_id": "nyc",
                "order_status": "delivered",
                "placed_at": "2026-06-01T10:00:00Z",
                "accepted_at": "2026-06-01T10:02:00Z",
                "prepared_at": "2026-06-01T10:15:00Z",
                "picked_up_at": "2026-06-01T10:20:00Z",
                "delivered_at": "2026-06-01T10:38:00Z",
                "currency": "USD",
                "subtotal_amount": "26.009",
                "discount_amount": "3.0",
                "estimated_delivery_minutes": "35",
                "schema_version": "1",
                "_ingest_ts": "2026-06-01T11:05:00Z",
                "_source_file": "newer.jsonl",
            },
        ]
    )

    result = clean_orders(raw_df).collect()

    # Decimal rounding, integer casting, and optional schema-evolution fields are all
    # part of the downstream contract that gold expects.
    assert len(result) == 1
    assert result[0]["_source_file"] == "newer.jsonl"
    assert result[0]["subtotal_amount"] == Decimal("26.01")
    assert result[0]["estimated_delivery_minutes"] == 35
    assert result[0]["app_version"] is None


def test_clean_payments_deduplicates_and_casts_money_fields(spark):
    """Verify payment status corrections and decimal money casting."""

    # The second row represents the same payment after capture. The silver table should
    # keep the latest version by payment_id.
    raw_df = spark.createDataFrame(
        [
            {
                "payment_id": "pay_1",
                "order_id": "order_1",
                "payment_status": "authorized",
                "payment_method": "card",
                "subtotal_amount": "20.00",
                "delivery_fee": "4.50",
                "service_fee": "2.10",
                "tax_amount": "1.80",
                "discount_amount": "0.00",
                "total_amount": "28.40",
                "authorized_at": "2026-06-01T10:01:00Z",
                "captured_at": None,
                "currency": "USD",
                "_ingest_ts": "2026-06-01T11:00:00Z",
                "_source_file": "first.csv",
            },
            {
                "payment_id": "pay_1",
                "order_id": "order_1",
                "payment_status": "captured",
                "payment_method": "card",
                "subtotal_amount": "20.00",
                "delivery_fee": "4.50",
                "service_fee": "2.10",
                "tax_amount": "1.80",
                "discount_amount": "0.00",
                "total_amount": "28.40",
                "authorized_at": "2026-06-01T10:01:00Z",
                "captured_at": "2026-06-01T10:42:00Z",
                "currency": "USD",
                "_ingest_ts": "2026-06-01T11:10:00Z",
                "_source_file": "second.csv",
            },
        ]
    )

    result = clean_payments(raw_df).collect()

    # Financial fields should remain fixed decimals so reporting calculations are
    # deterministic.
    assert len(result) == 1
    assert result[0]["payment_status"] == "captured"
    assert result[0]["total_amount"] == Decimal("28.40")


def test_clean_order_items_keeps_latest_order_line(spark):
    """Verify the order-item grain of order_id plus line_number."""

    # Corrected line-item quantities should replace older rows for the same order line.
    raw_df = spark.createDataFrame(
        [
            {
                "order_id": "order_1",
                "line_number": "1",
                "menu_item_id": "item_1",
                "merchant_id": "merch_1",
                "quantity": "1",
                "unit_price": "10.00",
                "line_total": "10.00",
                "_ingest_ts": "2026-06-01T11:00:00Z",
                "_source_file": "old.jsonl",
            },
            {
                "order_id": "order_1",
                "line_number": "1",
                "menu_item_id": "item_1",
                "merchant_id": "merch_1",
                "quantity": "2",
                "unit_price": "10.00",
                "line_total": "20.00",
                "_ingest_ts": "2026-06-01T11:10:00Z",
                "_source_file": "new.jsonl",
            },
        ]
    )

    result_df = clean_order_items(raw_df)

    # Counting the resulting rows and quantity sum catches both duplicate retention and
    # incorrect casting of the quantity field.
    assert result_df.count() == 1
    assert result_df.select(F.sum("quantity")).collect()[0][0] == 2
