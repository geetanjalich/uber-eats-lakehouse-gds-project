-- Suggested verified questions and SQL patterns for a Genie space.
-- Replace ue_marketplace_lakehouse_dev with ue_marketplace_lakehouse_prod for prod.

-- Question: Which cities have the weakest delivery SLA this week?
SELECT
  order_date,
  city_id,
  orders,
  delivered_orders,
  sla_success_rate
FROM ue_marketplace_lakehouse_dev.gold.vw_marketplace_executive_summary
WHERE order_date >= current_date() - INTERVAL 7 DAYS
ORDER BY sla_success_rate ASC, orders DESC;

-- Question: Which merchants generated the highest estimated platform commission?
SELECT
  order_date,
  merchant_name,
  city_id,
  cuisine_type,
  gross_booking_amount,
  estimated_platform_commission,
  sla_success_rate
FROM ue_marketplace_lakehouse_dev.gold.vw_merchant_profitability
ORDER BY estimated_platform_commission DESC
LIMIT 25;

-- Question: Which DQ rules are producing failed records?
SELECT
  rule_id,
  source_table,
  severity,
  checked_date,
  failed_executions,
  failed_records
FROM ue_marketplace_lakehouse_dev.dq.vw_dq_rule_trends
ORDER BY checked_date DESC, failed_records DESC;
