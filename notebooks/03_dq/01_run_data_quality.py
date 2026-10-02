# Databricks notebook source
# Data quality checks produce durable rule results and record-level quarantine data.
# Critical failures stop trusted publication so downstream users do not consume bad facts.

# COMMAND ----------

import sys
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "ue_marketplace_lakehouse_dev", "Unity Catalog catalog")
dbutils.widgets.text("dq_scope", "silver", "silver, gold, or all")

# The job normally runs silver-scope checks before gold tables are built. The scope
# widget is still available for manual investigation runs from the Databricks UI.
catalog = dbutils.widgets.get("catalog")
dq_scope = dbutils.widgets.get("dq_scope")
run_id = f"dq_{uuid4().hex}"

# Resolve the uploaded bundle project root so this notebook can import the same rule
# definitions that local unit tests exercise.
workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = Path(f"/Workspace{workspace_path}")
project_root = workspace_file.parents[2]
"""
    The two Path.cwd() entries are backup search folders. 
    When someone runs the notebook by hand, Databricks may start Python in a different folder 
    (the driver's own folder) than in a job run. Adding the cwd folders too gives Python extra places
    to find src/."

Example

run mode	            where Python starts	        do the main folders work?
Bundle job	            project folder	            yes
Manual run in the UI	/databricks/driver	        maybe not, so the cwd backups help

"""
for candidate in [str(project_root), str(project_root / "src"), str(Path.cwd()), str(Path.cwd() / "src")]:
    # The cwd entries are fallbacks for interactive notebook runs where Databricks may
    # place execution under a different driver working directory.
    if candidate not in sys.path:
        sys.path.append(candidate)

from uber_eats_lakehouse.data_quality import critical_rule_ids, marketplace_quality_rules

# COMMAND ----------

def table_exists(table_name: str) -> bool:
    """Return true when a table can be queried."""

    # Missing tables are reported as skipped rules instead of failing immediately. That
    # makes partial development runs easier to diagnose.
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def rule_in_scope(rule: dict[str, str]) -> bool:
    """Keep rules relevant to the requested execution scope."""

    # Scope filtering keeps the same rule registry usable for both normal pipeline
    # gates and targeted troubleshooting runs.
    source_table = rule["source_table"]
    if dq_scope == "all":
        return True
    if dq_scope == "silver":
        return ".silver." in source_table
    if dq_scope == "gold":
        return ".gold." in source_table
    return True


# COMMAND ----------

rules = [rule for rule in marketplace_quality_rules(catalog) if rule_in_scope(rule)]
critical_rules = critical_rule_ids()
results = []
critical_failures = []

for rule in rules:
    # Each rule owns the SQL that returns failed records. A zero-row result means the
    # rule passed and no quarantine payload is written.
    if not table_exists(rule["source_table"]):
        results.append(
            {
                "run_id": run_id,
                "rule_id": rule["rule_id"],
                "source_table": rule["source_table"],
                "severity": rule["severity"],
                "description": rule["description"],
                "failed_record_count": -1,
                "status": "skipped_missing_table",
            }
        )
        continue

    failed_df = spark.sql(rule["failed_records_sql"])
    failed_count = failed_df.count()
    status = "passed" if failed_count == 0 else "failed"

    results.append(
        {
            "run_id": run_id,
            "rule_id": rule["rule_id"],
            "source_table": rule["source_table"],
            "severity": rule["severity"],
            "description": rule["description"],
            "failed_record_count": failed_count,
            "status": status,
        }
    )

    if failed_count > 0:
        # Quarantine rows are normalized into a common table. The complete failed row
        # is stored as JSON so Genie or SQL users can inspect root causes later.
        quarantined_df = (
            failed_df.withColumn("run_id", F.lit(run_id))
            .withColumn("rule_id", F.lit(rule["rule_id"]))
            .withColumn("source_table", F.lit(rule["source_table"]))
            .withColumn("severity", F.lit(rule["severity"]))
            .withColumn("failed_at", F.current_timestamp())
            .withColumn("source_record_json", F.to_json(F.struct(*[F.col(column) for column in failed_df.columns])))
            .select("run_id", "rule_id", "source_table", "severity", "failed_at", "source_record_json")
        )
        quarantined_df.write.mode("append").saveAsTable(f"{catalog}.dq.dq_failed_records")

    if failed_count > 0 and rule["rule_id"] in critical_rules:
        # Critical failures block the workflow before gold publication. Warning rules
        # remain visible in DQ results but do not stop processing.
        critical_failures.append(rule["rule_id"])

# COMMAND ----------

results_df = (
    # Rule results are appended for every run to support trend analysis over time.
    spark.createDataFrame(results)
    .withColumn("checked_at", F.current_timestamp())
    .select(
        "run_id",
        "rule_id",
        "source_table",
        "severity",
        "description",
        "failed_record_count",
        "status",
        "checked_at",
    )
)
results_df.write.mode("append").saveAsTable(f"{catalog}.dq.dq_rule_results")

summary_df = spark.createDataFrame(
    [
        {
            "run_id": run_id,
            "dq_scope": dq_scope,
            "total_rules": len(results),
            "failed_rules": sum(1 for item in results if item["status"] == "failed"),
            "critical_failed_rules": len(critical_failures),
            "status": "failed" if critical_failures else "passed",
        }
    ]
).withColumn("started_at", F.current_timestamp()).withColumn("finished_at", F.current_timestamp())

# The summary table gives workflow operators one row per DQ execution, while the rule
# result table gives the detailed breakdown.
summary_df.select(
    "run_id",
    "dq_scope",
    "started_at",
    "finished_at",
    "total_rules",
    "failed_rules",
    "critical_failed_rules",
    "status",
).write.mode("append").saveAsTable(f"{catalog}.dq.dq_run_summary")

display(results_df)

if critical_failures:
    # Raising an exception marks the Databricks task failed, which enables normal job
    # retry/re-execution behavior after the bad source batch is corrected.
    raise Exception(f"Critical data quality rules failed: {', '.join(critical_failures)}")
