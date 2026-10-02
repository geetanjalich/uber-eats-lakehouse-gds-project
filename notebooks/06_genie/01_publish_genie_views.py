# Databricks notebook source
# Genie works best with curated tables and plain-language views. This notebook publishes
# narrow, business-friendly views that analysts can add to a Genie space.

# COMMAND ----------

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")

# Some Databricks SQL commands in Free Edition require the active catalog to be set
# before referencing schemas. After this point, use two-part names such as gold.table.
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# Executive summary keeps the most common daily marketplace KPIs in one view that
# Genie can answer from without choosing multiple base tables.
spark.sql(
    f"""
CREATE OR REPLACE VIEW gold.vw_marketplace_executive_summary AS
SELECT
  order_date,
  city_id,
  count(*) AS orders,
  sum(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  sum(CASE WHEN order_status LIKE 'cancelled%' THEN 1 ELSE 0 END) AS cancelled_orders,
  sum(total_amount) AS gross_booking_amount,
  avg(actual_delivery_minutes) AS avg_delivery_minutes,
  avg(CASE WHEN delivered_within_sla THEN 1 ELSE 0 END) AS sla_success_rate
FROM gold.fact_order
GROUP BY order_date, city_id
"""
)

# Delivery SLA analysis joins facts to courier attributes so natural-language
# questions can compare city, courier, and vehicle behavior.
spark.sql(
    f"""
CREATE OR REPLACE VIEW gold.vw_delivery_sla_analysis AS
SELECT
  fd.delivery_date,
  fd.city_id,
  fd.courier_id,
  dc.vehicle_type,
  count(*) AS deliveries,
  avg(fd.prep_minutes) AS avg_prep_minutes,
  avg(fd.pickup_wait_minutes) AS avg_pickup_wait_minutes,
  avg(fd.courier_travel_minutes) AS avg_courier_travel_minutes,
  avg(CASE WHEN fd.delivered_within_sla THEN 1 ELSE 0 END) AS sla_success_rate
FROM gold.fact_delivery fd
LEFT JOIN gold.dim_courier dc
  ON fd.courier_sk = dc.courier_sk
GROUP BY fd.delivery_date, fd.city_id, fd.courier_id, dc.vehicle_type
"""
)

# Merchant profitability exposes the materialized aggregate with business-friendly
# columns and hides the implementation detail of the underlying gold table.
spark.sql(
    f"""
CREATE OR REPLACE VIEW gold.vw_merchant_profitability AS
SELECT
  order_date,
  merchant_id,
  merchant_name,
  city_id,
  cuisine_type,
  orders,
  delivered_orders,
  cancelled_orders,
  gross_booking_amount,
  estimated_platform_commission,
  avg_delivery_minutes,
  sla_success_rate
FROM gold.agg_merchant_daily_performance
"""
)

# Failed-record analysis makes quarantined rows available for natural-language
# troubleshooting while keeping raw JSON payloads visible for root-cause inspection.
spark.sql(
    f"""
CREATE OR REPLACE VIEW dq.vw_failed_record_analysis AS
SELECT
  r.run_id,
  r.rule_id,
  rr.description,
  r.source_table,
  r.severity,
  r.failed_at,
  r.source_record_json
FROM dq.dq_failed_records r
LEFT JOIN dq.dq_rule_results rr
  ON r.run_id = rr.run_id
 AND r.rule_id = rr.rule_id
"""
)

# DQ trends summarize rule behavior over time so users can ask about recurring data
# quality patterns, not just one failed run.
spark.sql(
    f"""
CREATE OR REPLACE VIEW dq.vw_dq_rule_trends AS
SELECT
  rule_id,
  source_table,
  severity,
  date(checked_at) AS checked_date,
  count(*) AS executions,
  sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_executions,
  sum(CASE WHEN failed_record_count > 0 THEN failed_record_count ELSE 0 END) AS failed_records
FROM dq.dq_rule_results
GROUP BY rule_id, source_table, severity, date(checked_at)
"""
)

# COMMAND ----------

# Display statements give immediate confirmation when this notebook is executed from
# the Databricks UI or as a job task.
display(spark.sql("SHOW VIEWS IN gold"))
display(spark.sql("SHOW VIEWS IN dq"))
