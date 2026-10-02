-- Unity Catalog setup for the Uber Eats marketplace lakehouse.
-- Replace the catalog name with ue_marketplace_lakehouse_prod for the prod target.

CREATE CATALOG IF NOT EXISTS ue_marketplace_lakehouse_dev;
USE CATALOG ue_marketplace_lakehouse_dev;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS dq;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS config;

CREATE VOLUME IF NOT EXISTS bronze.landing_volume;
CREATE VOLUME IF NOT EXISTS bronze.checkpoint_volume;
CREATE VOLUME IF NOT EXISTS bronze.schema_volume;
CREATE VOLUME IF NOT EXISTS bronze.artifact_volume;
