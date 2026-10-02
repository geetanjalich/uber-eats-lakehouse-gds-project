#!/usr/bin/env bash
set -euo pipefail

# Generate local source files for the Uber Eats marketplace lakehouse.
# The output folder is intentionally outside Databricks; operators upload it to
# the Unity Catalog volume before running the Databricks workflow.

OUTPUT_DIR="${OUTPUT_DIR:-generated_data}"
START_DATE="${START_DATE:-2026-08-01}"
DAYS="${DAYS:-45}"
ORDERS_PER_DAY="${ORDERS_PER_DAY:-1200}"
SEED="${SEED:-20260819}"
DEFECT_RATE="${DEFECT_RATE:-0.0}"

python data_generator/generate_uber_eats_data.py \
  --output-dir "${OUTPUT_DIR}" \
  --start-date "${START_DATE}" \
  --days "${DAYS}" \
  --orders-per-day "${ORDERS_PER_DAY}" \
  --seed "${SEED}" \
  --defect-rate "${DEFECT_RATE}"

echo "Generated marketplace landing files under ${OUTPUT_DIR}"

