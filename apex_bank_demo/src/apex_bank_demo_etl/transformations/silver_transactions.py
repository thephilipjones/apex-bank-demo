# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Data Quality & Validation
# MAGIC
# MAGIC Applies business rules and data quality expectations:
# MAGIC - **@dp.expect**: Log violations but allow records through
# MAGIC - **@dp.expect_or_drop**: Quarantine invalid records
# MAGIC
# MAGIC ### Data Quality Rules:
# MAGIC 1. `valid_amount`: Amount must be positive (log violations)
# MAGIC 2. `valid_timestamp`: Timestamp must not be null (log violations)
# MAGIC 3. `valid_card_present`: Card present flag must be 'Y' or 'N' (quarantine invalid)
# MAGIC 4. `valid_transaction_id`: Transaction ID must not be null (quarantine invalid)

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
    name="transactions_silver",
    comment="Validated and cleaned transaction data. PII columns prepared for masking in Gold.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "transaction_timestamp,account_id"
    }
)
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect_or_drop("valid_card_present", "card_present_flag IN ('Y', 'N')")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
def transactions_silver():
    """
    Silver table: Cleaned and validated transactions

    Quality enforcement:
    - Logs violations for valid_amount and valid_timestamp (allows records)
    - Drops records that fail valid_card_present or valid_transaction_id
    """
    return (
        dp.read("transactions_bronze")
            .select(
                col("transaction_id"),
                col("account_id"),
                col("transaction_timestamp").cast("timestamp"),
                col("amount").cast("decimal(10,2)"),
                col("merchant_category_code"),
                col("merchant_category_desc"),
                col("merchant_name"),
                col("card_present_flag"),
                col("transaction_type"),
                col("is_fraud").cast("int")
            )
            .withColumn("processing_timestamp", current_timestamp())
    )
