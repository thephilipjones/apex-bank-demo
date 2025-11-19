# Databricks notebook source
# MAGIC %md
# MAGIC # Quarantine Layer: Quality Violations
# MAGIC
# MAGIC Captures records that failed quality checks for investigation.
# MAGIC This table is critical for:
# MAGIC - Root cause analysis
# MAGIC - Upstream data quality feedback
# MAGIC - Compliance audits
# MAGIC
# MAGIC Instead of failing the pipeline when bad data arrives, we route it to a quarantine table.
# MAGIC This ensures the fraud model always has fresh data, while engineers clean up the mess offline.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------------
# Fetch from DLT Pipeline Settings to allow CI/CD promotion without code changes.
# ---------------------------------------------------------
catalog = spark.conf.get("pipeline.catalog_name", "apex_bank_demo")
target_schema = spark.conf.get("pipeline.target_schema", "analytics")

# COMMAND ----------

@dp.table(
    name="transactions_quarantine",
    comment="Records that failed data quality expectations. Used for investigation and upstream feedback."
)
def transactions_quarantine():
    """
    Quarantine table: Records that failed expect_or_drop rules

    Captures:
    - Records with invalid card_present_flag (not 'Y' or 'N')
    - Records with null transaction_id
    - Includes reason for quarantine
    """
    return (
        dp.read("transactions_bronze")
            .where(
                (col("card_present_flag").isNull()) |
                (~col("card_present_flag").isin(['Y', 'N'])) |
                (col("transaction_id").isNull())
            )
            .withColumn("quarantine_reason",
                when(col("transaction_id").isNull(), "NULL_TRANSACTION_ID")
                .when(col("card_present_flag").isNull(), "NULL_CARD_PRESENT")
                .when(~col("card_present_flag").isin(['Y', 'N']), "INVALID_CARD_PRESENT")
                .otherwise("UNKNOWN")
            )
            .withColumn("quarantine_timestamp", current_timestamp())
    )
