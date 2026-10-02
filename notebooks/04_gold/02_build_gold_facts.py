# Databricks notebook source
# Gold facts define trusted analytic grains for revenue, delivery reliability, payments,
# refunds, support interactions, and courier movement. The primary order fact uses the
# shared src/ transform so local Spark tests validate the deployed join logic.

# COMMAND ----------

import sys
from pathlib import Path

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# Resolve the uploaded project root so the notebook imports the tested gold transform
# from the bundle deployment rather than depending on a manually attached library.
workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = Path(f"/Workspace{workspace_path}")
project_root = workspace_file.parents[2]

for candidate in [str(project_root), str(project_root / "src"), str(Path.cwd()), str(Path.cwd() / "src")]:
    # Local fallback paths keep the notebook runnable during interactive debugging.
    if candidate not in sys.path:
        sys.path.append(candidate)

from uber_eats_lakehouse.gold_transforms import build_fact_order

# COMMAND ----------

build_fact_order(
    spark.table(f"{catalog}.silver.orders"),
    spark.table(f"{catalog}.silver.payments"),
    spark.table(f"{catalog}.gold.dim_customer"),
    spark.table(f"{catalog}.gold.dim_merchant"),
    spark.table(f"{catalog}.gold.dim_courier"),
).createOrReplaceTempView("v_fact_order")

# The primary order fact is rebuilt from trusted silver and current SCD2 dimensions on
# each workflow run. Upstream silver merges keep this input incremental and consistent.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_order
USING DELTA
AS SELECT * FROM v_fact_order
"""
)

# Validate the fact grain inside the build step. This is a model integrity assertion,
# not a separate post-gold DQ gate.
duplicate_order_count = spark.sql(
    f"""
SELECT count(*) AS duplicate_order_count
FROM (
  SELECT order_id
  FROM {catalog}.gold.fact_order
  GROUP BY order_id
  HAVING count(*) > 1
)
"""
).collect()[0]["duplicate_order_count"]

if duplicate_order_count > 0:
    # Failing here means the model grain itself is broken, so the task should stop
    # before dependent facts and aggregates are published.
    raise Exception(f"fact_order grain violation: {duplicate_order_count} duplicate order IDs")

# COMMAND ----------

# Item facts use the order timestamp to resolve the historical menu item version.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_order_item
USING DELTA
AS
SELECT
  oi.order_id,
  oi.line_number,
  mi.menu_item_sk,
  oi.menu_item_id,
  oi.merchant_id,
  fo.order_date,
  oi.quantity,
  oi.unit_price,
  oi.line_total,
  current_timestamp() AS gold_updated_at
FROM {catalog}.silver.order_items oi
LEFT JOIN {catalog}.gold.fact_order fo
  ON oi.order_id = fo.order_id
LEFT JOIN {catalog}.gold.dim_menu_item mi
  ON oi.menu_item_id = mi.menu_item_id
 AND fo.placed_at BETWEEN mi.valid_from AND mi.valid_to
WHERE oi.order_id IS NOT NULL
"""
)

# COMMAND ----------

# Delivery and payment facts are narrow projections of the order/payment model at
# business-friendly grains for SLA and finance analysis.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_delivery
USING DELTA
AS
SELECT
  order_id,
  courier_sk,
  courier_id,
  merchant_sk,
  merchant_id,
  city_id,
  order_date AS delivery_date,
  placed_at,
  accepted_at,
  prepared_at,
  picked_up_at,
  delivered_at,
  timestampdiff(MINUTE, placed_at, accepted_at) AS acceptance_minutes,
  timestampdiff(MINUTE, accepted_at, prepared_at) AS prep_minutes,
  timestampdiff(MINUTE, prepared_at, picked_up_at) AS pickup_wait_minutes,
  timestampdiff(MINUTE, picked_up_at, delivered_at) AS courier_travel_minutes,
  actual_delivery_minutes,
  delivered_within_sla,
  current_timestamp() AS gold_updated_at
FROM {catalog}.gold.fact_order
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_payment
USING DELTA
AS
SELECT
  p.payment_id,
  p.order_id,
  fo.customer_sk,
  fo.merchant_sk,
  fo.city_id,
  cast(p.authorized_at AS DATE) AS payment_date,
  p.payment_status,
  p.payment_method,
  p.subtotal_amount,
  p.delivery_fee,
  p.service_fee,
  p.tax_amount,
  p.discount_amount,
  p.total_amount,
  p.currency,
  p.authorized_at,
  p.captured_at,
  current_timestamp() AS gold_updated_at
FROM {catalog}.silver.payments p
LEFT JOIN {catalog}.gold.fact_order fo
  ON p.order_id = fo.order_id
"""
)

# Refunds are sparse exception records. The left join retains refund visibility even
# when an upstream order was filtered or delayed.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_refund
USING DELTA
AS
SELECT
  r.refund_id,
  r.order_id,
  fo.customer_sk,
  fo.merchant_sk,
  fo.city_id,
  cast(to_timestamp(r.requested_at) AS DATE) AS refund_date,
  r.refund_reason,
  cast(r.refund_amount AS DECIMAL(12,2)) AS refund_amount,
  r.refund_status,
  to_timestamp(r.requested_at) AS requested_at,
  current_timestamp() AS gold_updated_at
FROM {catalog}.silver.refunds r
LEFT JOIN {catalog}.gold.fact_order fo
  ON r.order_id = fo.order_id
"""
)

# COMMAND ----------

# Support facts are another exception workflow and are shaped for case-resolution
# analysis rather than order-volume reporting.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_customer_support
USING DELTA
AS
SELECT
  s.ticket_id,
  s.order_id,
  fo.customer_sk,
  fo.merchant_sk,
  fo.city_id,
  cast(to_timestamp(s.opened_at) AS DATE) AS opened_date,
  s.reason,
  s.priority,
  s.status,
  to_timestamp(s.opened_at) AS opened_at,
  to_timestamp(s.closed_at) AS closed_at,
  timestampdiff(HOUR, to_timestamp(s.opened_at), to_timestamp(s.closed_at)) AS resolution_hours,
  current_timestamp() AS gold_updated_at
FROM {catalog}.silver.support_tickets s
LEFT JOIN {catalog}.gold.fact_order fo
  ON s.order_id = fo.order_id
"""
)

# Courier telemetry is aggregated to an hourly grain so location health analytics do
# not have to scan every raw point.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.fact_courier_location_hourly
USING DELTA
AS
SELECT
  courier_id,
  date_trunc('HOUR', event_ts) AS location_hour,
  count(*) AS location_points,
  avg(speed_mph) AS avg_speed_mph,
  min(battery_pct) AS min_battery_pct,
  max(battery_pct) AS max_battery_pct,
  current_timestamp() AS gold_updated_at
FROM {catalog}.silver.courier_locations
GROUP BY courier_id, date_trunc('HOUR', event_ts)
"""
)

# COMMAND ----------

# Aggregates are materialized because they are the most common executive and operations
# queries and are good candidates for optimization/clustering.
spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.agg_merchant_daily_performance
USING DELTA
AS
SELECT
  fo.order_date,
  fo.merchant_id,
  dm.merchant_name,
  dm.city_id,
  dm.cuisine_type,
  count(*) AS orders,
  sum(CASE WHEN fo.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  sum(CASE WHEN fo.order_status LIKE 'cancelled%' THEN 1 ELSE 0 END) AS cancelled_orders,
  sum(fo.total_amount) AS gross_booking_amount,
  sum(fo.subtotal_amount * dm.commission_rate) AS estimated_platform_commission,
  avg(fo.actual_delivery_minutes) AS avg_delivery_minutes,
  avg(CASE WHEN fo.delivered_within_sla THEN 1 ELSE 0 END) AS sla_success_rate,
  current_timestamp() AS gold_updated_at
FROM {catalog}.gold.fact_order fo
LEFT JOIN {catalog}.gold.dim_merchant dm
  ON fo.merchant_sk = dm.merchant_sk
GROUP BY fo.order_date, fo.merchant_id, dm.merchant_name, dm.city_id, dm.cuisine_type
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE {catalog}.gold.agg_city_hourly_marketplace_health
USING DELTA
AS
SELECT
  order_date,
  order_hour,
  city_id,
  count(*) AS orders,
  sum(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  sum(CASE WHEN delivered_within_sla THEN 1 ELSE 0 END) AS orders_within_sla,
  avg(actual_delivery_minutes) AS avg_delivery_minutes,
  sum(total_amount) AS gross_booking_amount,
  current_timestamp() AS gold_updated_at
FROM {catalog}.gold.fact_order
GROUP BY order_date, order_hour, city_id
"""
)

display(spark.sql(f"SHOW TABLES IN {catalog}.gold LIKE 'fact*'"))
