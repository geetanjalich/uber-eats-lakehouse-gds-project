#!/usr/bin/env bash
set -euo pipefail

# Run local checks that do not require a Databricks workspace.

export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
export PYTHONPATH="src:."
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(command -v python)}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$(command -v python)}"

python -m compileall src data_generator tests notebooks
pytest -q

echo "Local checks passed"
