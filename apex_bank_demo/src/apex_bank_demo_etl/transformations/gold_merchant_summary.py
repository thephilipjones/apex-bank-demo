# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Daily Merchant Category Summary
# MAGIC
# MAGIC Business-ready aggregated views for analytics and ML.
# MAGIC Daily aggregations by merchant category for fraud detection and business intelligence.

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
    name="daily_merchant_category_summary",
    comment="Daily aggregations by merchant category for fraud detection and business intelligence"
)
def daily_merchant_category_summary():
    """
    Gold table: Daily aggregates by merchant category

    Used for:
    - Fraud detection (unusual spending patterns)
    - Business intelligence (revenue by category)
    - Customer segmentation
    """
    return (
        dp.read("transactions_silver")
            .groupBy(
                to_date("transaction_timestamp").alias("transaction_date"),
                "merchant_category_code",
                "merchant_category_desc"
            )
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                min("amount").alias("min_amount"),
                max("amount").alias("max_amount"),
                countDistinct("account_id").alias("unique_accounts"),
                sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count")
            )
    )
