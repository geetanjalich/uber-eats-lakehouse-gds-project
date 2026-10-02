"""Reusable helpers for the Uber Eats marketplace lakehouse.

The package contains only small, testable functions. Databricks notebooks remain the
visible orchestration layer, while transformation logic that benefits from local Spark
tests lives here and is imported by the deployed notebooks.
"""

__all__ = [
    "config",
    "data_quality",
    "gold_transforms",
    "native_quality",
    "scd2",
    "silver_transforms",
]
