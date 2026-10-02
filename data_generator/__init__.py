"""Synthetic source data generator package.

The generator is kept outside the Databricks job workflow on purpose. It creates
landing files locally so the user can upload one or more selected `batch_date=...`
folders into the Unity Catalog volume and then run the lakehouse workflow against
only the newly arrived files.
"""
