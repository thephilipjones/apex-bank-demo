# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Account Transaction Features
# MAGIC
# MAGIC 7-day rolling aggregate features for fraud detection ML models.
# MAGIC Window functions on streaming data create real-time rolling aggregates for the fraud model.
# MAGIC
# MAGIC These features will feed into the Feature Store for fraud detection.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ---------------------------------------------------------
# Fetch from DLT Pipeline Settings to allow CI/CD promotion without code changes.
# ---------------------------------------------------------
catalog = spark.conf.get("pipeline.catalog_name", "apex_bank_demo")
target_schema = spark.conf.get("pipeline.target_schema", "analytics")

# COMMAND ----------

@dp.table(
    name="account_transaction_features",
    comment="7-day rolling aggregate features for fraud detection ML models"
)
def account_transaction_features():
    """
    Gold table: Account-level features for ML

    Includes:
    - Rolling totals and averages
    - Volatility measures (StdDev)
    - Risk indicators (Card Not Present counts)

    These features will feed into the Feature Store for fraud detection.
    Window functions create rolling aggregations.
    """
    # NOTE:
    # For this Pipelines demo (Micro-Batch), Window specs work perfectly and are easy to read.
    # If we required low-latency stateful streaming, we would use spark.readStream.window().
    window_spec = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").cast("long")).rangeBetween(-7*24*60*60, 0)

    return (
        dp.read("transactions_silver")
            .withColumn("txn_count_7day", count("*").over(window_spec))
            .withColumn("total_amount_7day", sum("amount").over(window_spec))
            .withColumn("avg_amount_7day", avg("amount").over(window_spec))
            .withColumn("stddev_amount_7day", stddev("amount").over(window_spec))
            .withColumn("max_amount_7day", max("amount").over(window_spec))
            .withColumn("card_not_present_count_7day",
                sum(when(col("card_present_flag") == "N", 1).otherwise(0)).over(window_spec)
            )
            .select(
                "transaction_id",
                "account_id",
                "transaction_timestamp",
                "amount",
                "txn_count_7day",
                "total_amount_7day",
                "avg_amount_7day",
                "stddev_amount_7day",
                "max_amount_7day",
                "card_not_present_count_7day",
                "is_fraud"
            )
    )
