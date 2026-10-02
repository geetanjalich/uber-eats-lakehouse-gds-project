"""Reusable data quality rule definitions.

The rule metadata is pure Python so it can be tested locally. Databricks notebooks turn
these definitions into Spark SQL checks and persist results in the dq schema.
"""

from __future__ import annotations

QualityRule = dict[str, str]


def marketplace_quality_rules(catalog: str) -> list[QualityRule]:
    """Return the core data quality rules for the marketplace pipeline."""

    # Rules target silver tables because silver is the trusted handoff point before
    # dimensional modeling. Gold should receive only records that pass critical checks.
    silver = f"{catalog}.silver"

    return [
        {
            "rule_id": "orders_required_keys",
            "source_table": f"{silver}.orders",
            "severity": "critical",
            "description": "Orders must contain order_id, customer_id, merchant_id, and city_id.",
            "failed_records_sql": f"""
            SELECT *
            FROM {silver}.orders
            WHERE order_id IS NULL
            OR customer_id IS NULL
            OR merchant_id IS NULL
            OR city_id IS NULL
            """.strip(),
        },
        {
            "rule_id": "orders_unique_event_id",
            "source_table": f"{silver}.orders",
            "severity": "critical",
            "description": "Order event IDs must be unique after silver standardization.",
            "failed_records_sql": f"""
            SELECT *
            FROM {silver}.orders
            WHERE event_id IN (
            SELECT event_id
            FROM {silver}.orders
            GROUP BY event_id
            HAVING count(*) > 1
            )
            """.strip(),
        },
        {
            "rule_id": "orders_valid_time_sequence",
            "source_table": f"{silver}.orders",
            "severity": "critical",
            "description": "Delivered orders must have timestamps in the expected lifecycle order.",
            "failed_records_sql": f"""
SELECT *
FROM {silver}.orders
WHERE delivered_at IS NOT NULL
  AND (
    accepted_at < placed_at
    OR prepared_at < accepted_at
    OR picked_up_at < prepared_at
    OR delivered_at < picked_up_at
  )
""".strip(),
        },
        {
            "rule_id": "payments_required_keys",
            "source_table": f"{silver}.payments",
            "severity": "critical",
            "description": "Payments must contain payment_id, order_id, payment_status, and currency.",
            "failed_records_sql": f"""
SELECT *
FROM {silver}.payments
WHERE payment_id IS NULL
   OR order_id IS NULL
   OR payment_status IS NULL
   OR currency IS NULL
""".strip(),
        },
        {
            "rule_id": "payments_non_negative_amount",
            "source_table": f"{silver}.payments",
            "severity": "critical",
            "description": "Payment amounts and fees must not be negative.",
            "failed_records_sql": f"""
SELECT *
FROM {silver}.payments
WHERE subtotal_amount < 0
   OR delivery_fee < 0
   OR service_fee < 0
   OR tax_amount < 0
   OR total_amount < 0
""".strip(),
        },
        {
            "rule_id": "payments_valid_currency",
            "source_table": f"{silver}.payments",
            "severity": "critical",
            "description": "Marketplace payments must use USD in this implementation.",
            "failed_records_sql": f"""
SELECT *
FROM {silver}.payments
WHERE currency <> 'USD'
""".strip(),
        },
        {
            "rule_id": "order_items_positive_quantity",
            "source_table": f"{silver}.order_items",
            "severity": "critical",
            "description": "Order item quantities and line totals must be positive.",
            "failed_records_sql": f"""
SELECT *
FROM {silver}.order_items
WHERE quantity <= 0
   OR unit_price <= 0
   OR line_total <= 0
""".strip(),
        },
        {
            "rule_id": "orders_known_merchant",
            "source_table": f"{silver}.orders",
            "severity": "critical",
            "description": "Each order must map to a known merchant snapshot.",
            "failed_records_sql": f"""
SELECT o.*
FROM {silver}.orders o
LEFT JOIN {silver}.merchant_changes m
  ON o.merchant_id = m.merchant_id
WHERE m.merchant_id IS NULL
""".strip(),
        },
        {
            "rule_id": "orders_late_delivery_warning",
            "source_table": f"{silver}.orders",
            "severity": "warning",
            "description": "Completed deliveries beyond the promised SLA buffer should be reviewed.",
            # A warning is persisted for analysis but does not block gold publication.
            # Late delivery can be a valid business outcome rather than a bad record.
            "failed_records_sql": f"""
SELECT *
FROM {silver}.orders
WHERE delivered_at IS NOT NULL
  AND timestampdiff(MINUTE, placed_at, delivered_at) > estimated_delivery_minutes + 10
""".strip(),
        },
    ]


def critical_rule_ids() -> set[str]:
    """Return rules that should prevent trusted gold publication when they fail."""

    # Critical rules represent data that can corrupt facts, dimensions, or finance
    # metrics. The DQ notebook raises an exception when any of these rules fail.
    return {
        "orders_required_keys",
        "orders_unique_event_id",
        "orders_valid_time_sequence",
        "payments_required_keys",
        "payments_non_negative_amount",
        "payments_valid_currency",
        "order_items_positive_quantity",
        "orders_known_merchant",
    }
