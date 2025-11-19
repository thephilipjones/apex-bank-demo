# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Ingests raw transaction data from Unity Catalog Volumes using Auto Loader.
# MAGIC - No transformations
# MAGIC - No quality checks
# MAGIC - Preserves everything "as-is" for audit trail
# MAGIC
# MAGIC Uses `cloudFiles` for "Exactly-Once" processing and automatic Schema Drift handling.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------------------------------------
# Fetch from DLT Pipeline Settings to allow CI/CD promotion without code changes.
# ---------------------------------------------------------
catalog = spark.conf.get("pipeline.catalog_name", "apex_bank_demo")
target_schema = spark.conf.get("pipeline.target_schema", "analytics")

# Construct the Volume Path (Governed Storage)
volume_path = f"/Volumes/{catalog}/raw_data/landing_zone/"
checkpoint_base = f"/Volumes/{catalog}/raw_data/checkpoints/"

# COMMAND ----------

@dp.table(
    name="transactions_bronze",
    comment="Raw transaction data ingested from card processor. Contains all data quality issues.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "transaction_timestamp"
    }
)
def transactions_bronze():
    """
    Bronze table: Raw ingestion with Auto Loader
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        # Schema Evolution: If the bank adds 'credit_score' column, it is added automatically
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
        .option("pathGlobFilter", "synthetic_transactions*.csv")
        .load(volume_path)
    )
