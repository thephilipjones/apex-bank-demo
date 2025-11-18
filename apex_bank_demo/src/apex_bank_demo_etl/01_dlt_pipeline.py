# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Bank - Delta Live Tables Pipeline
# MAGIC
# MAGIC ## Business Context (Validated Industry Research)
# MAGIC - **Apex Bank**: $6B annual volume, FedNow launch Q2 2025
# MAGIC - **Current Fraud**: $18M annually (0.30% loss rate - digital bank standard)
# MAGIC - **FedNow Risk**: $22-24M exposure without real-time controls
# MAGIC - **Board Mandate**: Sub-10-second fraud detection required for launch
# MAGIC - **Current System**: 45-minute batch processing (blocks launch)
# MAGIC
# MAGIC This DLT pipeline demonstrates:
# MAGIC - **Bronze Layer**: Raw data ingestion with Auto Loader
# MAGIC - **Silver Layer**: Data quality enforcement with expectations
# MAGIC - **Gold Layer**: Business-ready aggregations for real-time fraud detection
# MAGIC
# MAGIC ## Pipeline Architecture
# MAGIC ```
# MAGIC GitHub CSV → Bronze (raw) → Silver (validated) → Gold (real-time features)
# MAGIC                              ↓ (quarantine)
# MAGIC                         Quality_Metrics
# MAGIC ```
# MAGIC
# MAGIC ## Business Impact
# MAGIC - Enables FedNow launch ($80M revenue opportunity)
# MAGIC - Reduces fraud from $18M → $10M (real-time detection + better features)
# MAGIC - Improves false positive ratio from 1:8 → 1:12 (40% reduction)
# MAGIC
# MAGIC ## Architecture Improvements (Solutions Architect style)
# MAGIC - **Ingestion**: Upgraded to **Auto Loader** (cloudFiles) for infinite scalability and schema evolution.
# MAGIC - **Storage**: Migrated from legacy DBFS Mounts to **Unity Catalog Volumes** for governance.
# MAGIC - **Configuration**: Decoupled environment logic using DLT Pipeline Configurations.
# MAGIC - **Observability**: leveraging native DLT Event Logs instead of manual count queries.
# MAGIC

# COMMAND ----------

from pyspark import pipelines
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

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Ingests raw transaction data from external source (GitHub or DBFS).
# MAGIC - No transformations
# MAGIC - No quality checks
# MAGIC - Preserves everything "as-is" for audit trail
# MAGIC
# MAGIC Switched from `spark.read.csv` to `cloudFiles`. 
# MAGIC This guarantees "Exactly-Once" processing and handles Schema Drift automatically.
# MAGIC

# COMMAND ----------

@pipelines.table(
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
        .load(volume_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Data Quality & Validation
# MAGIC
# MAGIC Applies business rules and data quality expectations:
# MAGIC - **@dlt.expect**: Log violations but allow records through
# MAGIC - **@dlt.expect_or_drop**: Quarantine invalid records
# MAGIC - **@dlt.expect_or_fail**: Stop pipeline on critical violations
# MAGIC
# MAGIC ### Data Quality Rules:
# MAGIC 1. ✅ `valid_amount`: Amount must be positive (log violations)
# MAGIC 2. ✅ `valid_timestamp`: Timestamp must not be null (log violations)
# MAGIC 3. ⛔ `valid_card_present`: Card present flag must be 'Y' or 'N' (quarantine invalid)
# MAGIC 4. ⛔ `valid_transaction_id`: Transaction ID must not be null (quarantine invalid)
# MAGIC
# MAGIC Use declarative expectations. 
# MAGIC Separate **Operational Issues** (Quarantine) from **Data Warnings** (Log only).

# COMMAND ----------

@pipelines.table(
    name="transactions_silver",
    comment="Validated and cleaned transaction data. PII columns prepared for masking in Gold.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "transaction_timestamp,account_id"
    }
)
@pipelines.expect("valid_amount", "amount > 0")
@pipelines.expect("valid_timestamp", "transaction_timestamp IS NOT NULL")
@pipelines.expect_or_drop("valid_card_present", "card_present_flag IN ('Y', 'N')")
@pipelines.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
def transactions_silver():
    """
    Silver table: Cleaned and validated transactions
    
    Quality enforcement:
    - Logs violations for valid_amount and valid_timestamp (allows records)
    - Drops records that fail valid_card_present or valid_transaction_id
    """
    return (
        pipelines.read("transactions_bronze")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗑️ Quarantine Layer: Quality Violations
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

@pipelines.table(
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
        pipelines.read("transactions_bronze")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Business Aggregations
# MAGIC
# MAGIC Business-ready aggregated views for analytics and ML.
# MAGIC
# MAGIC Window functions on streaming data to create real-time  
# MAGIC rolling aggregates (7-day lookback) for the fraud model.

# COMMAND ----------

@pipelines.table(
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
        pipelines.read("transactions_silver")
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

# COMMAND ----------

@pipelines.table(
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
    from pyspark.sql.window import Window
    
# ARCHITECT NOTE: 
    # For this ~DLT~ Pipelines demo (Micro-Batch), Window specs work perfectly and are easy to read.
    # If we required low-latency stateful streaming, we would use spark.readStream.window().
    window_spec = Window.partitionBy("account_id").orderBy("transaction_timestamp").rangeBetween(-7*24*60*60, 0)
    
    return (
        pipelines.read("transactions_silver")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Data Quality Metrics DISABLED
# MAGIC
# MAGIC Summary table of data quality violations for monitoring dashboard.

# COMMAND ----------

# @dlt.table(
#     name="quality_metrics_summary",
#     comment="Summary of data quality violations by rule for monitoring and alerting"
# )
# def quality_metrics_summary():
#     """
#     Monitoring table: Data quality metrics
    
#     Aggregates quality violations for:
#     - Real-time monitoring dashboards
#     - Alerting thresholds
#     - SLA tracking
#     """
#     bronze_count = dlt.read("transactions_bronze").count()
#     silver_count = dlt.read("transactions_silver").count()
#     quarantine_count = dlt.read("transactions_quarantine").count()
    
#     # Count specific violations
#     bronze_df = dlt.read("transactions_bronze")
    
#     null_amounts = bronze_df.where(col("amount").isNull()).count()
#     negative_amounts = bronze_df.where(col("amount") < 0).count()
#     null_timestamps = bronze_df.where(col("transaction_timestamp").isNull()).count()
#     invalid_card_present = bronze_df.where(
#         (col("card_present_flag").isNull()) | 
#         (~col("card_present_flag").isin(['Y', 'N']))
#     ).count()
    
#     metrics = [
#         ("bronze_records_total", bronze_count),
#         ("silver_records_total", silver_count),
#         ("quarantine_records_total", quarantine_count),
#         ("null_amounts", null_amounts),
#         ("negative_amounts", negative_amounts),
#         ("null_timestamps", null_timestamps),
#         ("invalid_card_present_flag", invalid_card_present)
#     ]
    
#     return spark.createDataFrame(metrics, ["metric_name", "metric_value"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Demo Talking Points
# MAGIC
# MAGIC ### When showing this pipeline:
# MAGIC
# MAGIC 1. **Bronze Layer**: "We ingest everything as-is. No data is lost. Complete audit trail. This is $6 billion in annual transaction volume."
# MAGIC
# MAGIC 2. **Silver Layer Expectations**: 
# MAGIC    - Point to `@dlt.expect` decorators: "These are declarative data quality rules"
# MAGIC    - "If amount is negative, we LOG it but keep the record"
# MAGIC    - "If card_present_flag is invalid, we QUARANTINE the record"
# MAGIC    - "The pipeline doesn't break. Downstream fraud detection stays healthy."
# MAGIC
# MAGIC 3. **DLT UI**: 
# MAGIC    - Show dependency graph: "Databricks figured out the DAG automatically"
# MAGIC    - Show data quality metrics: "~4,000 records quarantined - about 4% data quality issues"
# MAGIC    - "No Airflow. No orchestration code. Just Python. The platform does the rest."
# MAGIC
# MAGIC 4. **Quarantine Table**: 
# MAGIC    - "Bad data doesn't cascade to the fraud detection model"
# MAGIC    - "Data engineers investigate quarantine weekly, send quality reports upstream"
# MAGIC    - "This is how you prevent the 2am pages about broken pipelines"
# MAGIC
# MAGIC 5. **Gold Layer**: 
# MAGIC    - "Business-ready aggregations power real-time fraud detection"
# MAGIC    - "account_transaction_features creates the 7-day rolling windows our fraud model uses"
# MAGIC    - "This is what enables sub-10-second fraud scoring for FedNow instant payments"
# MAGIC
# MAGIC 6. **Business Value**: 
# MAGIC    - "This pipeline transforms 45-minute batch processing into 10-20 second real-time detection"
# MAGIC    - "That's the difference between blocking FedNow launch versus enabling an $80M revenue opportunity"
# MAGIC    - "It's the difference between $18M in fraud losses versus $10M through faster detection"
# MAGIC    - "And it's how we reduce false positives from 1:8 to 1:12 - protecting customer trust while catching fraud"
