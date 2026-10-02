  #!/usr/bin/env bash
  set -euo pipefail

  # Upload locally generated source files into the Unity Catalog landing volume.
  # Auto Loader expects each dataset directly under:
  # /Volumes/<catalog>/bronze/landing_volume/<dataset>

  CATALOG="${CATALOG:-ue_marketplace_lakehouse_dev}"
  LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-generated_data}"
  BATCH_DATE="${BATCH_DATE:-}"
  LANDING_VOLUME_URI="dbfs:/Volumes/${CATALOG}/bronze/landing_volume"

  if ! command -v databricks >/dev/null 2>&1; then
    echo "databricks CLI was not found. Install and authenticate the CLI before uploading."
    exit 1
  fi

  if [ ! -d "${LOCAL_DATA_DIR}" ]; then
    echo "Local data directory does not exist: ${LOCAL_DATA_DIR}"
    echo "Run scripts/generate_landing_data.sh first."
    exit 1
  fi

  databricks fs mkdir "${LANDING_VOLUME_URI}"

  for dataset_path in "${LOCAL_DATA_DIR}"/*; do
    if [ -d "${dataset_path}" ]; then
      dataset_name="$(basename "${dataset_path}")"
      if [ -n "${BATCH_DATE}" ] && [ "${dataset_name}" != "_manifest" ]; then
        partition_path="${dataset_path}/batch_date=${BATCH_DATE}"
        if [ -d "${partition_path}" ]; then
          echo "Uploading ${dataset_name}/batch_date=${BATCH_DATE} to ${LANDING_VOLUME_URI}/${dataset_name}/batch_date=${BATCH_DATE}"
          databricks fs cp \
            "${partition_path}" \
            "${LANDING_VOLUME_URI}/${dataset_name}/batch_date=${BATCH_DATE}" \
            -r \
            --overwrite
        else
          echo "Skipping ${dataset_name}; partition not found: ${partition_path}"
        fi
      else
        echo "Uploading ${dataset_name} to ${LANDING_VOLUME_URI}/${dataset_name}"
        databricks fs cp "${dataset_path}" "${LANDING_VOLUME_URI}/${dataset_name}" -r --overwrite
      fi
    fi
  done

  echo "Upload complete: ${LANDING_VOLUME_URI}"
