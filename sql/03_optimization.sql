-- Serving-table optimization commands.
-- Run after gold tables have been published.
-- Replace ue_marketplace_lakehouse_dev with ue_marketplace_lakehouse_prod for prod.

ALTER TABLE ue_marketplace_lakehouse_dev.gold.fact_order
CLUSTER BY (order_date, city_id, merchant_id);

ALTER TABLE ue_marketplace_lakehouse_dev.gold.fact_delivery
CLUSTER BY (delivery_date, city_id, courier_id);

ALTER TABLE ue_marketplace_lakehouse_dev.gold.agg_merchant_daily_performance
CLUSTER BY (order_date, merchant_id);

OPTIMIZE ue_marketplace_lakehouse_dev.gold.fact_order;
OPTIMIZE ue_marketplace_lakehouse_dev.gold.fact_delivery;
OPTIMIZE ue_marketplace_lakehouse_dev.gold.agg_merchant_daily_performance;

ALTER TABLE ue_marketplace_lakehouse_dev.gold.fact_order
ENABLE PREDICTIVE OPTIMIZATION;

ALTER TABLE ue_marketplace_lakehouse_dev.gold.agg_merchant_daily_performance
ENABLE PREDICTIVE OPTIMIZATION;

ALTER TABLE ue_marketplace_lakehouse_dev.gold.fact_order SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
