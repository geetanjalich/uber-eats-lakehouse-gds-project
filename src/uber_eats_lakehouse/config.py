"""Small configuration helpers shared by notebooks and tests.

The Databricks notebooks keep their orchestration logic visible, while these helpers
centralize naming and path rules. The intent is to avoid hidden behavior: every table
and volume path is still easy to infer from the project configuration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_project_config(config_path: str | Path) -> dict[str, Any]:
    """Read the YAML configuration used by both local tooling and Databricks notebooks."""

    # Loading the YAML through one helper keeps local scripts and notebooks aligned on
    # catalog, schema, and volume names without scattering string literals everywhere.
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def fq_table(config: dict[str, Any], schema_key: str, table_name: str) -> str:
    """Return a Unity Catalog three-part table name.

    Example: ue_marketplace_lakehouse_dev.gold.fact_order
    """

    # Table names are resolved from logical keys such as "gold" or "bronze" so the
    # same code can target dev/prod catalogs by changing configuration only.
    catalog = config["project"]["catalog"]
    schema_name = config["schemas"][schema_key]
    return f"{catalog}.{schema_name}.{table_name}"


def fq_volume(config: dict[str, Any], schema_key: str, volume_key: str) -> str:
    """Return the Unity Catalog volume path used by Spark reads and writes."""

    # Unity Catalog volume paths follow a fixed /Volumes/catalog/schema/volume pattern.
    # Keeping the path construction here prevents notebook-level path drift.
    catalog = config["project"]["catalog"]
    schema_name = config["schemas"][schema_key]
    volume_name = config["volumes"][volume_key]
    return f"/Volumes/{catalog}/{schema_name}/{volume_name}"


def landing_path(config: dict[str, Any], dataset: str) -> str:
    """Return the landing path for a named source dataset."""

    # Each source dataset gets its own child folder, for example
    # /Volumes/.../landing_volume/orders.
    return f"{fq_volume(config, 'bronze', 'landing')}/{dataset}"


def checkpoint_path(config: dict[str, Any], pipeline_name: str) -> str:
    """Return an Auto Loader or streaming checkpoint path."""

    # Checkpoints are part of pipeline state. Reusing the same checkpoint lets
    # available-now Auto Loader skip files that were already committed.
    return f"{fq_volume(config, 'bronze', 'checkpoints')}/{pipeline_name}"


def schema_path(config: dict[str, Any], pipeline_name: str) -> str:
    """Return the schema tracking path used by Auto Loader."""

    # Auto Loader writes inferred and evolved schema metadata here. Keeping schema
    # state separate per pipeline avoids accidental cross-feed schema contamination.
    return f"{fq_volume(config, 'bronze', 'schemas')}/{pipeline_name}"
