# Uber Eats Marketplace Lakehouse: Technical Setup Deployment And Execution Guide

## 1. Purpose

This document is the operator runbook for deploying and executing the Uber Eats marketplace lakehouse on Databricks Free Edition. It covers local setup, source data generation, Unity Catalog volume preparation, source upload, Databricks Asset Bundle deployment, workflow execution, validation, CI/CD, and recovery runs.

The Databricks workflow does not generate source data. Source files are generated locally and uploaded into a Unity Catalog volume before the pipeline starts. The recurring pipeline begins with Auto Loader ingestion and then executes silver processing, data quality checks, dimensional modeling, gold publication, and Genie view publication. Unity Catalog setup and gold-table optimization are separate bundle jobs so they do not run on every incremental batch.

## 2. Architecture Execution Flow

```text
Local machine
  -> generate realistic source files
  -> upload files to Unity Catalog volume

Databricks bootstrap job
  -> setup/update UC objects

Databricks pipeline job
  -> Auto Loader bronze ingestion
  -> silver standardization
  -> SQL data quality enforcement
  -> SCD Type 2 dimensions
  -> gold facts and aggregates
  -> Genie-ready views

Databricks maintenance job
  -> Delta optimization and table-property maintenance for gold tables
```

## 3. Prerequisites

Required on the local machine:

- Python 3.10 or newer.
- Git.
- Databricks CLI 0.205 or newer.
- Access to a Databricks Free Edition workspace.
- A Databricks user or token that can deploy bundles and create schemas, tables, and volumes in the selected Unity Catalog catalog.

Recommended:

- GitHub repository for CI/CD.
- A local virtual environment for Python dependency isolation.
- Databricks SQL warehouse access for validation queries and Genie setup.

Notes:

- Databricks Free Edition is serverless-oriented and quota-limited, so start with the default or smoke-test data volume before increasing size.
- Data quality is implemented natively with Spark SQL checks and Delta quarantine tables, so it works cleanly on Free Edition serverless compute.

## 4. Repository Layout

```text
uber-eats-marketplace-lakehouse/
  databricks.yml
  resources/jobs.yml
  config/project_config.yml
  data_generator/
    generate_uber_eats_data.py
  scripts/
    generate_landing_data.sh
    upload_landing_data_to_volume.sh
    run_local_checks.sh
  notebooks/
    00_setup/
    01_bronze/
    02_silver/
    03_dq/
    04_gold/
    05_optimization/
    06_genie/
  sql/
  src/uber_eats_lakehouse/
  tests/
  .github/workflows/
  docs/
```

Important files:

- `pyproject.toml`: Python project metadata, local dependencies, pytest settings, and lint settings.
- `databricks.yml`: bundle configuration and deployment targets.
- `resources/jobs.yml`: Databricks workflow definition.
- `.github/workflows/cicd.yml`: GitHub Actions workflow for validation followed by bundle deployment.
- `data_generator/generate_uber_eats_data.py`: local data generator.
- `scripts/generate_landing_data.sh`: local wrapper for source generation.
- `scripts/upload_landing_data_to_volume.sh`: local wrapper for upload into UC volume.
- `notebooks/00_setup/00_create_uc_objects.py`: creates catalog, schemas, volumes, and control tables.
- `notebooks/01_bronze/01_autoloader_ingest.py`: Auto Loader ingestion.
- `notebooks/02_silver/01_build_silver.py`: silver merge and standardization.
- `notebooks/03_dq/01_run_data_quality.py`: required SQL DQ enforcement and quarantine.
- `notebooks/04_gold/01_build_dimensions_scd2.py`: SCD Type 2 dimensions.
- `notebooks/04_gold/02_build_gold_facts.py`: facts and aggregates.
- `notebooks/05_optimization/01_optimize_gold_tables.py`: liquid clustering, optimization, predictive optimization, and UniForm commands.
- `notebooks/06_genie/01_publish_genie_views.py`: curated Genie views.
- `docs/03_GENIE_AGENT_INSTRUCTIONS.md`: Genie space instructions, business vocabulary, and example analytical questions.

## 5. Codebase Anatomy

The repository separates orchestration from reusable logic.

`notebooks/` contains Databricks task entrypoints. These files are what the Databricks workflow runs. They read widgets, call Spark, create tables, and persist results.

`src/uber_eats_lakehouse/` contains plain Python functions that can be imported by notebooks and unit tests. This folder is not a separate application. It exists so important business logic can be tested without copying notebook code into tests.

Current `src` modules:

| File | Purpose |
|---|---|
| `config.py` | Helper functions for catalog/table/volume naming |
| `data_quality.py` | Native DQ rule registry with SQL failed-record queries |
| `native_quality.py` | Spark DataFrame DQ functions used by unit tests |
| `silver_transforms.py` | Spark DataFrame functions for silver casting and deduplication |
| `gold_transforms.py` | Spark DataFrame function for order fact construction and temporal dimension lookup |
| `scd2.py` | SQL builders for SCD Type 2 merge logic |

`tests/` contains PySpark unit tests for the transformation and DQ logic. These tests run locally and in GitHub Actions. They do not require Databricks, Unity Catalog, Auto Loader, or Delta table writes.

`pyproject.toml` is the Python project configuration file. In this repository it defines:

- project name and Python version
- runtime dependency: `pyyaml`
- development dependencies: `pyspark`, `pytest`, and `ruff`
- pytest discovery settings
- lint configuration

When you run:

```bash
python -m pip install -e ".[dev]"
```

Python installs the project in editable mode. Editable mode means tests and local scripts import code from the working folder directly, so code changes are picked up without reinstalling the package.

## 6. Install Local Tools

### 6.1 Install Python Dependencies

From the parent folder:

```bash
cd <workspacecd_parent>/uber-eats-marketplace-lakehouse
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Run local validation:

```bash
scripts/run_local_checks.sh
```

Expected result:

```text
Local checks passed
```

### 6.2 Install Databricks CLI

On macOS with Homebrew:

```bash
brew tap databricks/tap
brew trust databricks/tap
brew install databricks
```

Alternative Homebrew command:

```bash
brew install databricks/tap/databricks
```

Validate:

```bash
databricks --version
```

## 7. Authenticate To Databricks

Interactive login:

```bash
databricks auth login --host https://<your-workspace-url>
```

Confirm the active identity:

```bash
databricks current-user me
```

For token-based local use:

```bash
export DATABRICKS_HOST="https://<your-workspace-url>"
export DATABRICKS_TOKEN="<your-token>"
```

For named profile use:

```bash
databricks auth profiles
databricks bundle validate -t dev --profile <profile_name>
```

## 8. Select Catalog And Target

This project simulates dev and prod inside one Databricks Free Edition workspace. The environments are separated logically by target name, catalog name, job name, and workspace deployment path.

| Target | Catalog | Usage |
|---|---|---|
| `dev` | `ue_marketplace_lakehouse_dev` | Development and validation |
| `prod` | `ue_marketplace_lakehouse_prod` | Production-style deployment |

The same workspace will contain:

| Object Type | Dev | Prod |
|---|---|---|
| Catalog | `ue_marketplace_lakehouse_dev` | `ue_marketplace_lakehouse_prod` |
| Job | `uber-eats-marketplace-lakehouse-dev` | `uber-eats-marketplace-lakehouse-prod` |
| Bundle target | `dev` | `prod` |
| Bundle workspace path | `~/.bundle/uber-eats-marketplace-lakehouse/dev` | `~/.bundle/uber-eats-marketplace-lakehouse/prod` |

This is not physical workspace isolation. It is logical isolation inside one workspace, which is the right fit for Free Edition.

If your workspace does not allow catalog creation, choose an existing catalog where you can create schemas and volumes.

Example using an existing catalog:

```bash
export CATALOG="<existing_catalog_name>"
```

Bundle commands can override the catalog:

```bash
databricks bundle validate -t dev --var catalog="${CATALOG}"
```

For prod:

```bash
export CATALOG="ue_marketplace_lakehouse_prod"
databricks bundle validate -t prod --var catalog="${CATALOG}"
```

## 9. Validate And Deploy The Bundle

From the project root:

```bash
databricks bundle validate -t dev
```

Expected result:

```text
Validation OK!
```

Deploy:

```bash
databricks bundle deploy -t dev
```

With catalog override:

```bash
databricks bundle deploy -t dev --var catalog="${CATALOG}"
```

What deployment does:

- Uploads synced project files into the workspace bundle path.
- Creates or updates the Databricks workflow from `resources/jobs.yml`.
- Tracks deployed resources as bundle state.

What deployment does not do:

- It does not generate source files.
- It does not upload landing data.
- It does not run the pipeline automatically unless you trigger `bundle run`.

## 10. What `databricks bundle run` Does

`databricks bundle run` starts one runnable resource declared in the bundle configuration. A runnable resource can be a Databricks Job, pipeline, or script. This project defines three Databricks Job resources:

```text
uber_eats_marketplace_bootstrap
uber_eats_marketplace_lakehouse
uber_eats_marketplace_gold_maintenance
```

Use `uber_eats_marketplace_bootstrap` for one-time Unity Catalog setup:

```bash
databricks bundle run -t dev uber_eats_marketplace_bootstrap
```

Use `uber_eats_marketplace_lakehouse` for the recurring incremental batch pipeline:

```bash
databricks bundle run -t dev uber_eats_marketplace_lakehouse
```

Use `uber_eats_marketplace_gold_maintenance` after gold tables exist when you want to apply optimization/table-property maintenance:

```bash
databricks bundle run -t dev uber_eats_marketplace_gold_maintenance
```

Meaning:

- Databricks CLI reads `databricks.yml`.
- The CLI resolves target `dev`.
- The CLI finds the deployed job resource named in the command.
- Databricks creates a new job run in Workflows.
- Tasks execute according to `resources/jobs.yml`.
- The command waits for completion unless `--no-wait` is passed.

This command does not create the job definition from scratch. Job creation/update happens during:

```bash
databricks bundle deploy -t dev
```

Use `--only` when you want to run only selected task keys inside a multi-task job. For most project operations, prefer the dedicated runnable jobs above.

Use `--only +task_name` when you want to run a task and its upstream dependencies. Use `task_name+` when you want to run a task and its downstream dependencies.

## 11. Create Unity Catalog Objects Before Upload

The landing volume must exist before local files can be uploaded. Run the bootstrap job once after deployment.

Default catalog:

```bash
databricks bundle run -t dev uber_eats_marketplace_bootstrap
```

With catalog override:

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_bootstrap
```

Validate the volume exists:

```bash
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/landing_volume"
```

If using the default development catalog, set:

```bash
export CATALOG="ue_marketplace_lakehouse_dev"
```

## 12. Generate Source Data Locally

### 12.1 Smoke-Test Data

Use this for quick validation:

```bash
OUTPUT_DIR=generated_data \
START_DATE=2026-06-01 \
DAYS=3 \
ORDERS_PER_DAY=200 \
DEFECT_RATE=0.0 \
scripts/generate_landing_data.sh
```

Expected result:

```text
Generated marketplace landing files under generated_data
```

### 12.2 Standard Data Volume

Use this for the intended project volume:

```bash
OUTPUT_DIR=generated_data \
START_DATE=2026-06-01 \
DAYS=45 \
ORDERS_PER_DAY=1200 \
DEFECT_RATE=0.0 \
scripts/generate_landing_data.sh
```

This creates 54,000 orders plus related order items, lifecycle events, payments, courier locations, refunds, ratings, support tickets, and master-data snapshots.

### 12.3 Hard-Failure Test Data

Use this only when you want critical DQ failures:

```bash
OUTPUT_DIR=generated_data_failure_case \
START_DATE=2026-06-01 \
DAYS=5 \
ORDERS_PER_DAY=300 \
DEFECT_RATE=0.004 \
scripts/generate_landing_data.sh
```

This injects records such as missing order keys, invalid delivery timestamps, or negative payment amounts.

## 13. Inspect Local Landing Files

Check the generated folders:

```bash
find generated_data -maxdepth 3 -type f | sort | head -50
```

Expected structure:

```text
generated_data/orders/batch_date=2026-06-01/orders.jsonl
generated_data/payments/batch_date=2026-06-01/payments.csv
generated_data/merchant_snapshots/batch_date=2026-06-01/merchant_snapshot.jsonl
generated_data/customer_snapshots/batch_date=2026-06-01/customer_snapshot.jsonl
generated_data/courier_locations/batch_date=2026-06-01/courier_locations.jsonl
```

Count order rows locally:

```bash
wc -l generated_data/orders/batch_date=*/orders.jsonl
```

Review manifest:

```bash
cat generated_data/_manifest/manifest.jsonl
```

## 14. Upload Data To Unity Catalog Volume

Set the catalog and local data folder:

```bash
export CATALOG="ue_marketplace_lakehouse_dev"
export LOCAL_DATA_DIR="generated_data"
```

Upload:

```bash
scripts/upload_landing_data_to_volume.sh
```

This uploads every generated `batch_date` partition.

To upload only one selected date, set `BATCH_DATE`:

```bash
CATALOG="ue_marketplace_lakehouse_dev" \
LOCAL_DATA_DIR="generated_data" \
BATCH_DATE="2026-06-01" \
scripts/upload_landing_data_to_volume.sh
```

That copies only paths like:

```text
generated_data/orders/batch_date=2026-06-01
generated_data/payments/batch_date=2026-06-01
generated_data/order_items/batch_date=2026-06-01
```

into matching landing-volume paths:

```text
dbfs:/Volumes/ue_marketplace_lakehouse_dev/bronze/landing_volume/orders/batch_date=2026-06-01
dbfs:/Volumes/ue_marketplace_lakehouse_dev/bronze/landing_volume/payments/batch_date=2026-06-01
dbfs:/Volumes/ue_marketplace_lakehouse_dev/bronze/landing_volume/order_items/batch_date=2026-06-01
```

The script copies each dataset folder into:

```text
dbfs:/Volumes/<catalog>/bronze/landing_volume/<dataset_name>
```

Validate upload:

```bash
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/landing_volume"
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/landing_volume/orders"
databricks fs ls "dbfs:/Volumes/${CATALOG}/bronze/landing_volume/payments"
```

Expected top-level folders:

```text
orders
order_events
order_items
payments
refunds
ratings
courier_locations
support_tickets
merchant_snapshots
customer_snapshots
courier_snapshots
menu_item_snapshots
_manifest
```

## 15. Run The Full Databricks Workflow

Run the recurring pipeline with the default development catalog:

```bash
databricks bundle run -t dev uber_eats_marketplace_lakehouse
```

Run with an explicit catalog:

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

Workflow task order:

1. `ingest_bronze_autoloader`
2. `build_silver`
3. `enforce_silver_dq`
4. `build_dimensions_scd2`
5. `build_gold_facts`
6. `publish_genie_assets`

This job is intended to run repeatedly. Auto Loader checkpoints identify newly arrived files, silver merges protect trusted grains, and gold tables refresh from the current trusted silver layer.

Workflow dependency graph:

```text
ingest_bronze_autoloader
  -> build_silver
    -> enforce_silver_dq
      -> build_dimensions_scd2
        -> build_gold_facts
          -> publish_genie_assets
```

The DQ gate is placed before gold publication because the gold layer should be built only from trusted silver data. Gold table checks such as fact-grain uniqueness are model integrity assertions inside the gold build step, not a separate data-quality gate after the serving layer is already published.

Run gold maintenance separately after the first successful gold build, or whenever you intentionally want to refresh table optimization settings:

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_gold_maintenance
```

## 16. Manual Execution Path

Use this path when you want to run step by step from the Databricks workspace.

1. Deploy the bundle.
2. Run `uber_eats_marketplace_bootstrap`, or run `notebooks/00_setup/00_create_uc_objects.py` with `catalog=<catalog>`.
3. Generate data locally.
4. Upload data to the landing volume.
5. Run `notebooks/01_bronze/01_autoloader_ingest.py`.
6. Run `notebooks/02_silver/01_build_silver.py`.
7. Run `notebooks/03_dq/01_run_data_quality.py` with `dq_scope=silver`.
8. Run `notebooks/04_gold/01_build_dimensions_scd2.py`.
9. Run `notebooks/04_gold/02_build_gold_facts.py`.
10. Run `notebooks/06_genie/01_publish_genie_views.py`.
11. Run `notebooks/05_optimization/01_optimize_gold_tables.py` only when gold maintenance is needed.

## 17. Bronze Validation

Run in Databricks SQL:

```sql
SELECT count(*) AS orders_raw_count
FROM <catalog>.bronze.orders_raw;

SELECT count(*) AS payments_raw_count
FROM <catalog>.bronze.payments_raw;

SELECT _source_dataset, count(*) AS rows
FROM <catalog>.bronze.orders_raw
GROUP BY _source_dataset;

SELECT _source_file, count(*) AS rows
FROM <catalog>.bronze.orders_raw
GROUP BY _source_file
ORDER BY _source_file;

SELECT count(*) AS rescued_rows
FROM <catalog>.bronze.orders_raw
WHERE _rescued_data IS NOT NULL;
```

Expected outcome:

- Bronze row counts match uploaded source files.
- `_source_file` is populated.
- Auto Loader checkpoint and schema folders exist in UC volumes.
- Later batches include evolved fields such as `app_version` and `order_surface`.

## 18. Silver Validation

```sql
SELECT count(*) AS silver_orders
FROM <catalog>.silver.orders;

SELECT order_status, count(*) AS orders
FROM <catalog>.silver.orders
GROUP BY order_status
ORDER BY orders DESC;

SELECT count(*) AS silver_payments
FROM <catalog>.silver.payments;

SELECT count(*) AS merchant_change_rows
FROM <catalog>.silver.merchant_changes;

SELECT count(*) AS customer_change_rows
FROM <catalog>.silver.customer_changes;
```

Expected outcome:

- Silver records have normalized timestamps and decimal fields.
- Duplicate source events do not create duplicate silver keys.
- Snapshot data is ready for SCD Type 2 dimensions.

## 19. Data Quality Validation

```sql
SELECT *
FROM <catalog>.dq.dq_run_summary
ORDER BY finished_at DESC;

SELECT rule_id, source_table, severity, failed_record_count, status, checked_at
FROM <catalog>.dq.dq_rule_results
ORDER BY checked_at DESC, rule_id;

SELECT rule_id, severity, count(*) AS failed_records
FROM <catalog>.dq.dq_failed_records
GROUP BY rule_id, severity
ORDER BY failed_records DESC;
```

Expected outcome for standard data:

- Critical rules pass.
- Warning rules may produce failed records for SLA analysis.
- Quarantine records contain `source_record_json`.

Expected outcome for hard-failure data:

- One or more critical rules fail.
- The workflow stops before trusted downstream publication.
- Failed records are available in `dq.dq_failed_records`.

## 20. Native Data Quality Implementation

The DQ implementation is native to Databricks and does not require external packages, init scripts, or JAR dependencies. This keeps it compatible with Free Edition serverless compute.

Main files:

- `src/uber_eats_lakehouse/data_quality.py`: declarative rule registry.
- `notebooks/03_dq/01_run_data_quality.py`: rule execution, result persistence, quarantine, and critical-failure handling.
- `notebooks/00_setup/00_create_uc_objects.py`: DQ result and quarantine table creation.

Rule metadata contains:

- `rule_id`: stable identifier for audit and troubleshooting.
- `source_table`: silver table being checked.
- `severity`: `critical` or `warning`.
- `description`: business-readable explanation.
- `failed_records_sql`: SQL statement that returns failed records.

The DQ notebook executes each rule and writes:

- one row per rule to `<catalog>.dq.dq_rule_results`
- one run summary row to `<catalog>.dq.dq_run_summary`
- failed records to `<catalog>.dq.dq_failed_records`

Quarantined records are stored with:

- `run_id`
- `rule_id`
- `source_table`
- `severity`
- `failed_at`
- `source_record_json`

Critical rules stop the workflow before dimensions and gold facts are built. Warning rules write failed records for analysis but do not stop downstream processing.

Implemented critical rules:

- `orders_required_keys`
- `orders_unique_event_id`
- `orders_valid_time_sequence`
- `payments_required_keys`
- `payments_non_negative_amount`
- `payments_valid_currency`
- `order_items_positive_quantity`
- `orders_known_merchant`

Implemented warning rule:

- `orders_late_delivery_warning`

## 21. Gold Validation

```sql
SELECT count(*) AS fact_order_rows
FROM <catalog>.gold.fact_order;

SELECT order_date, city_id, count(*) AS orders
FROM <catalog>.gold.fact_order
GROUP BY order_date, city_id
ORDER BY order_date, city_id;

SELECT merchant_id, count(*) AS versions
FROM <catalog>.gold.dim_merchant
GROUP BY merchant_id
HAVING count(*) > 1
ORDER BY versions DESC;

SELECT *
FROM <catalog>.gold.agg_merchant_daily_performance
ORDER BY gross_booking_amount DESC
LIMIT 25;
```

Expected outcome:

- `fact_order` contains one row per valid order.
- Dimensions contain historical rows when attributes changed.
- Aggregates are ready for SQL analytics and Genie.

## 22. Optimization Validation

Run this job only after gold tables exist:

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_gold_maintenance
```

Then validate:

```sql
DESCRIBE EXTENDED <catalog>.gold.fact_order;
DESCRIBE EXTENDED <catalog>.gold.fact_delivery;
SHOW TBLPROPERTIES <catalog>.gold.fact_order;
```

Look for:

- clustering metadata
- `delta.enableIcebergCompatV2=true`
- `delta.universalFormat.enabledFormats=iceberg`
- predictive optimization status when the workspace permits it

The optimization notebook catches permission-sensitive failures and logs the skipped statement. This keeps the recurring ingestion pipeline usable even when Free Edition restricts a feature.

## 23. Genie Setup

After `publish_genie_assets` succeeds:

1. Open Databricks SQL.
2. Open Genie.
3. Create a Genie space for marketplace analytics.
4. Add these objects:
   - `<catalog>.gold.vw_marketplace_executive_summary`
   - `<catalog>.gold.vw_delivery_sla_analysis`
   - `<catalog>.gold.vw_merchant_profitability`
   - `<catalog>.dq.vw_failed_record_analysis`
   - `<catalog>.dq.vw_dq_rule_trends`
5. Add a SQL warehouse.
6. Add verified questions from `sql/02_genie_questions.sql`.
7. Add the instruction text from `docs/03_GENIE_AGENT_INSTRUCTIONS.md`.

Use `docs/03_GENIE_AGENT_INSTRUCTIONS.md` as the dedicated guide for:

- copy-paste Genie instructions
- business vocabulary
- marketplace health questions
- merchant performance questions
- delivery reliability questions
- DQ failure analysis questions
- incident-style investigation questions

## 24. GitHub Repository Setup And Environment Promotion

### 24.1 Create Repository

From the project root:

```bash
git init
git add .
git commit -m "Initial Uber Eats marketplace lakehouse implementation"
git branch -M dev
git remote add origin https://github.com/<org-or-user>/<repo-name>.git
git push -u origin dev
```

Create the prod branch after dev is stable:

```bash
git checkout -b main
git push -u origin main
git checkout dev
```

Recommended branch mapping:

| Git branch | GitHub action target | Databricks catalog | Databricks job |
|---|---|---|---|
| `dev` | `dev` | `ue_marketplace_lakehouse_dev` | `uber-eats-marketplace-lakehouse-dev` |
| `main` | `prod` | `ue_marketplace_lakehouse_prod` | `uber-eats-marketplace-lakehouse-prod` |
| `prod` | `prod` | `ue_marketplace_lakehouse_prod` | `uber-eats-marketplace-lakehouse-prod` |

### 24.2 Configure GitHub Secrets

In GitHub:

1. Open the repository.
2. Go to Settings.
3. Open Secrets and variables.
4. Open Actions.
5. Add repository secrets:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`

`DATABRICKS_HOST` example:

```text
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

`DATABRICKS_TOKEN` should belong to an identity with permission to validate and deploy the bundle.

### 24.3 CI/CD Workflow

File:

```text
.github/workflows/cicd.yml
```

The CI/CD workflow has two jobs.

`validate` runs first:

1. Checks out the repository.
2. Installs Python.
3. Installs local package dependencies.
4. Runs Ruff linting.
5. Compiles notebooks and scripts.
6. Runs unit tests.
7. Installs Databricks CLI.
8. Resolves the bundle target from the branch.
9. Validates the Databricks bundle when Databricks secrets are present.

`deploy` runs only after `validate` succeeds:

1. Checks out the repository.
2. Installs Databricks CLI.
3. Deploys the Databricks Asset Bundle.
4. Stops after deployment.

The deploy job does not run any Databricks Job resource. It does not run bootstrap, pipeline, or gold maintenance.

Trigger behavior:

- Pull requests do not trigger GitHub Actions in this setup.
- Push to `dev` validates target `dev`, then deploys target `dev`.
- Push to `main` or `prod` validates target `prod`, then deploys target `prod`.
- Manual run validates and deploys the selected target.

Local equivalent:

```bash
python -m pip install -e ".[dev]"
ruff check src data_generator tests
python -m compileall -q src data_generator tests notebooks
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src:. pytest -q
databricks bundle validate -t dev
```

Before running GitHub Actions:

1. Confirm GitHub secrets exist.
2. For manual runs, select `dev` or `prod`.

Important operational rule:

- GitHub Actions only validate and deploy the Databricks Asset Bundle.
- GitHub Actions do not generate source data.
- GitHub Actions do not upload source data.
- GitHub Actions do not run bootstrap.
- GitHub Actions do not run the recurring pipeline.
- GitHub Actions do not run gold maintenance.
- Run bootstrap, pipeline, and maintenance from Databricks Workflows UI or with `databricks bundle run`.

Recommended promotion flow:

1. Commit code changes to the `dev` branch.
2. GitHub validates target `dev`.
3. If validation succeeds, GitHub deploys target `dev`.
4. For the first dev setup, run `uber_eats_marketplace_bootstrap` from Databricks UI or CLI.
5. Upload test source files to `ue_marketplace_lakehouse_dev.bronze.landing_volume`.
6. Run `uber_eats_marketplace_lakehouse` from Databricks UI or CLI.
7. Inspect the `uber-eats-marketplace-lakehouse-dev` job run.
8. Validate dev tables and Genie views.
9. Merge `dev` into `main`.
10. GitHub validates target `prod`.
11. If validation succeeds, GitHub deploys target `prod`.
12. For first prod setup, run `uber_eats_marketplace_bootstrap` from Databricks UI or CLI.
13. Upload production-style source files to `ue_marketplace_lakehouse_prod.bronze.landing_volume`.
14. Run `uber_eats_marketplace_lakehouse` from Databricks UI or CLI.
15. Inspect the `uber-eats-marketplace-lakehouse-prod` job run.

## 25. Normal End-To-End Runbook

Use this complete sequence for a clean run:

```bash
cd <workspace_parent>/uber-eats-marketplace-lakehouse
source .venv/bin/activate
export CATALOG="ue_marketplace_lakehouse_dev"

scripts/run_local_checks.sh

databricks bundle validate -t dev --var catalog="${CATALOG}"
databricks bundle deploy -t dev --var catalog="${CATALOG}"

databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_bootstrap

OUTPUT_DIR=generated_data \
START_DATE=2026-06-01 \
DAYS=45 \
ORDERS_PER_DAY=1200 \
DEFECT_RATE=0.0 \
scripts/generate_landing_data.sh

LOCAL_DATA_DIR=generated_data \
CATALOG="${CATALOG}" \
scripts/upload_landing_data_to_volume.sh

databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

## 26. Incremental Batch Runbook

To simulate daily source arrivals with one already-generated local dataset:

```bash
CATALOG="ue_marketplace_lakehouse_dev" \
LOCAL_DATA_DIR="generated_data" \
BATCH_DATE="2026-06-01" \
scripts/upload_landing_data_to_volume.sh

databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

Then upload the next date and run the workflow again:

```bash
CATALOG="ue_marketplace_lakehouse_dev" \
LOCAL_DATA_DIR="generated_data" \
BATCH_DATE="2026-06-02" \
scripts/upload_landing_data_to_volume.sh

databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

Auto Loader checkpoints track files that have already been discovered. For the clearest incremental demonstration, upload a new `batch_date` each time. If the same file path must be replayed, reset the relevant bronze checkpoint and downstream tables first.

To create a fresh single-date local folder instead:

```bash
OUTPUT_DIR=generated_incremental_day \
START_DATE=2026-07-16 \
DAYS=1 \
ORDERS_PER_DAY=1200 \
DEFECT_RATE=0.0 \
scripts/generate_landing_data.sh

LOCAL_DATA_DIR=generated_incremental_day \
CATALOG="${CATALOG}" \
scripts/upload_landing_data_to_volume.sh

databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

Expected behavior:

- Auto Loader picks up only newly uploaded files.
- Silver merge logic prevents duplicate trusted rows.
- Dimensions update if snapshot attributes changed.
- Gold tables refresh from current trusted silver data.

## 27. Failure And Re-Execution Runbook

### 27.1 Generate Failure Data

```bash
OUTPUT_DIR=generated_failure_case \
START_DATE=2026-08-01 \
DAYS=3 \
ORDERS_PER_DAY=300 \
DEFECT_RATE=0.004 \
scripts/generate_landing_data.sh
```

### 27.2 Upload Failure Data

```bash
LOCAL_DATA_DIR=generated_failure_case \
CATALOG="${CATALOG}" \
scripts/upload_landing_data_to_volume.sh
```

### 27.3 Run Workflow

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

Expected behavior:

- Bronze ingestion succeeds.
- Silver standardization succeeds.
- DQ identifies critical failures.
- Failed records are written to `dq.dq_failed_records`.
- The workflow fails at the DQ enforcement task.

### 27.4 Analyze Failed Records

```sql
SELECT *
FROM <catalog>.dq.dq_run_summary
ORDER BY finished_at DESC;

SELECT rule_id, severity, source_table, source_record_json
FROM <catalog>.dq.dq_failed_records
ORDER BY failed_at DESC
LIMIT 100;
```

### 27.5 Recovery Options

Option A: Correct the bad source data and upload corrected files with new filenames.

Option B: Adjust a rule from critical to warning only when the business owner accepts the risk.

Option C: Keep the failed records quarantined and re-run from the failed task after the corrected source arrives.

Databricks job repair:

1. Open Workflows.
2. Open the failed run.
3. Select Repair run.
4. Start from the failed DQ task or the next appropriate task.

CLI re-run:

```bash
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_lakehouse
```

## 28. Cleanup And Reset

For a full reset in a non-production workspace:

```sql
DROP CATALOG IF EXISTS <catalog> CASCADE;
```

Then repeat:

1. Deploy bundle.
2. Run setup task.
3. Generate local source files.
4. Upload source files.
5. Run full workflow.

Use this only when it is acceptable to remove every schema, table, and volume under the catalog.

## 29. Troubleshooting

### 29.1 Databricks CLI Not Found

Symptom:

```text
command not found: databricks
```

Fix:

```bash
brew install databricks/tap/databricks
databricks --version
```

### 29.2 Upload Fails Because Volume Does Not Exist

Symptom:

```text
RESOURCE_DOES_NOT_EXIST
```

Fix:

```bash
databricks bundle deploy -t dev --var catalog="${CATALOG}"
databricks bundle run -t dev \
  --var catalog="${CATALOG}" \
  uber_eats_marketplace_bootstrap
```

Then upload again.

### 29.3 Catalog Creation Fails

Cause:

- The workspace identity can use an existing catalog but cannot create a new one.

Fix:

```bash
export CATALOG="<existing_catalog_name>"
databricks bundle deploy -t dev --var catalog="${CATALOG}"
```

### 29.4 DQ Fails During Standard Run

Possible causes:

- Failure-case data was uploaded.
- Old files remain in the landing volume.
- `DEFECT_RATE` was set above zero.

Fix:

1. Query `dq.dq_failed_records`.
2. Confirm the source files in `_source_file`.
3. Reset the catalog if this is a disposable environment.
4. Regenerate clean data with `DEFECT_RATE=0.0`.
5. Upload and rerun.

### 29.5 Optimization Statements Are Skipped

Cause:

- Some Free Edition workspaces restrict predictive optimization or UniForm settings.

Expected behavior:

- The optimization notebook logs the skipped statement.
- Core bronze, silver, DQ, SCD2, gold, and Genie views remain usable.

## 30. Reference Links

- Databricks CLI install: https://docs.databricks.com/aws/en/dev-tools/cli/install
- Databricks bundle commands: https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- Databricks bundle configuration: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- Unity Catalog volumes: https://docs.databricks.com/aws/en/volumes/
- Delta liquid clustering: https://docs.databricks.com/aws/en/delta/clustering
- Delta UniForm: https://docs.databricks.com/aws/en/delta/uniform
- Genie setup: https://docs.databricks.com/aws/genie/set-up
