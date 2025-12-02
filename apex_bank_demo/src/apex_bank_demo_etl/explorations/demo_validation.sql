-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Apex Bank Demo
-- MAGIC
-- MAGIC ## Story:
-- MAGIC 1. **The Mess** (Bronze) - Fraud reduction opportunity via latency and operational stability
-- MAGIC 2. **The Pipeline** (DLT) - Automated cleaning enables real-time detection
-- MAGIC 3. **The Governance** (Unity Catalog) - Self-service with guardrails
-- MAGIC 4. **The Value** (Gold + Features) - $14.8M quantified improvement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validating Problem 1: Fraud
-- MAGIC
-- MAGIC Industry research shows digital banks face 0.2-0.3% fraud rates.
-- MAGIC For Apex that means $18M annual fraud.

-- COMMAND ----------

-- Show total transaction volume
SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT account_id) as unique_accounts,
    ROUND(SUM(amount), 2) as total_volume,
    ROUND(AVG(amount), 2) as avg_transaction,
    MIN(transaction_timestamp) as earliest_transaction,
    MAX(transaction_timestamp) as latest_transaction
FROM apex_bank_demo.analytics.transactions_bronze;

-- COMMAND ----------

-- Fraud analysis
SELECT 
    'Total Transactions' as metric,
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM apex_bank_demo.analytics.transactions_bronze

UNION ALL

SELECT 
    'Fraudulent Transactions' as metric,
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM apex_bank_demo.analytics.transactions_bronze
WHERE is_fraud = 1

UNION ALL

SELECT 
    'Legitimate Transactions' as metric,
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM apex_bank_demo.analytics.transactions_bronze
WHERE is_fraud = 0

ORDER BY 
    CASE metric
        WHEN 'Total Transactions' THEN 1
        WHEN 'Fraudulent Transactions' THEN 2
        WHEN 'Legitimate Transactions' THEN 3
    END;

-- COMMAND ----------

-- Calculate fraud rate
SELECT 
    ROUND((SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as fraud_rate_pct,
    ROUND((SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) / SUM(amount) * 100), 2) as fraud_amount_pct,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) as fraud_amount
FROM apex_bank_demo.analytics.transactions_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validating Problem 2: False Positives

-- COMMAND ----------

-- Show false positive analysis from fraud labels
SELECT 
    investigation_status,
    COUNT(*) as count,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(SUM(service_ticket_cost), 2) as service_cost,
    ROUND(AVG(churn_risk_pct), 1) as avg_churn_risk_pct
FROM apex_bank_demo.analytics.fraud_labels
GROUP BY investigation_status
ORDER BY count DESC;

-- COMMAND ----------

-- Calculate false positive ratio
SELECT 
    SUM(CASE WHEN investigation_status = 'CONFIRMED_FRAUD' THEN 1 ELSE 0 END) as true_frauds,
    SUM(CASE WHEN investigation_status = 'FALSE_POSITIVE_DECLINED' THEN 1 ELSE 0 END) as false_positives,
    ROUND(
        SUM(CASE WHEN investigation_status = 'FALSE_POSITIVE_DECLINED' THEN 1 ELSE 0 END) * 1.0 /
        SUM(CASE WHEN investigation_status = 'CONFIRMED_FRAUD' THEN 1 ELSE 0 END),
        1
    ) as fp_per_fraud_ratio,
    ROUND(SUM(service_ticket_cost), 2) as total_service_cost
FROM apex_bank_demo.analytics.fraud_labels;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pipeline catching Data Quality issues

-- COMMAND ----------

-- Show data quality problems in bronze
SELECT 
    'Null Amounts' as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_bronze), 2) as pct_of_total
FROM apex_bank_demo.analytics.transactions_bronze
WHERE amount IS NULL

UNION ALL

SELECT 
    'Negative Amounts' as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_bronze), 2) as pct_of_total
FROM apex_bank_demo.analytics.transactions_bronze
WHERE amount < 0

UNION ALL

SELECT 
    'Null Timestamps' as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_bronze), 2) as pct_of_total
FROM apex_bank_demo.analytics.transactions_bronze
WHERE transaction_timestamp IS NULL

UNION ALL

SELECT 
    'Invalid Card Present Flag' as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_bronze), 2) as pct_of_total
FROM apex_bank_demo.analytics.transactions_bronze
WHERE card_present_flag NOT IN ('Y', 'N') OR card_present_flag IS NULL

ORDER BY record_count DESC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import dlt
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC @dlt.table(
-- MAGIC     comment="Raw transaction data from card processor"
-- MAGIC )
-- MAGIC def transactions_bronze():
-- MAGIC     return (
-- MAGIC         spark.readStream
-- MAGIC             .format("cloudFiles")
-- MAGIC             .option("cloudFiles.format", "json")
-- MAGIC             .option("cloudFiles.schemaLocation", "/mnt/schemas")
-- MAGIC             .load("/mnt/card-processor/transactions")
-- MAGIC     )
-- MAGIC
-- MAGIC @dlt.table(
-- MAGIC     comment="Validated transactions ready for fraud scoring"
-- MAGIC )
-- MAGIC @dlt.expect("valid_amount", "amount > 0")
-- MAGIC @dlt.expect("valid_timestamp", "transaction_timestamp IS NOT NULL")
-- MAGIC @dlt.expect_or_drop("valid_merchant", "merchant_id IS NOT NULL")
-- MAGIC def transactions_silver():
-- MAGIC     return (
-- MAGIC         dlt.read_stream("transactions_bronze")
-- MAGIC             .select(
-- MAGIC                 "transaction_id",
-- MAGIC                 "account_id", 
-- MAGIC                 "amount",
-- MAGIC                 "merchant_category",
-- MAGIC                 "transaction_timestamp",
-- MAGIC                 "card_present_flag"
-- MAGIC             )
-- MAGIC     )

-- COMMAND ----------

-- Show clean silver data
SELECT 
    COUNT(*) as total_clean_records,
    COUNT(DISTINCT account_id) as unique_accounts,
    ROUND(SUM(amount), 2) as total_value,
    ROUND(AVG(amount), 2) as avg_transaction_amount,
    MIN(transaction_timestamp) as earliest_clean_transaction,
    MAX(transaction_timestamp) as latest_clean_transaction
FROM apex_bank_demo.analytics.transactions_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quarantine: Automatic Data Quality Isolation

-- COMMAND ----------

-- Show quarantined records
SELECT 
    quarantine_reason,
    COUNT(*) as record_count,
    ROUND(AVG(amount), 2) as avg_amount,
    MIN(quarantine_timestamp) as first_quarantined,
    MAX(quarantine_timestamp) as last_quarantined
FROM apex_bank_demo.analytics.transactions_quarantine
GROUP BY quarantine_reason
ORDER BY record_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Operational Maturity

-- COMMAND ----------

-- Data quality comparison
SELECT 
    'Bronze (Raw)' as layer,
    (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_bronze) as total_records,
    'Contains all quality issues + PII' as status
UNION ALL
SELECT 
    'Silver (Clean)' as layer,
    (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_silver) as total_records,
    'Quality validated, ready for ML' as status
UNION ALL
SELECT 
    'Quarantine' as layer,
    (SELECT COUNT(*) FROM apex_bank_demo.analytics.transactions_quarantine) as total_records,
    'Isolated for investigation' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PII Masking

-- COMMAND ----------

-- Query the masked view (what data scientists see)
SELECT 
    transaction_id,
    account_id,  -- Shows as XXXX-XXXX-XXXX-1234
    transaction_timestamp,
    amount,
    merchant_category_desc,
    merchant_name,
    card_present_flag,
    is_fraud
FROM apex_bank_demo.analytics.transactions_silver_masked
LIMIT 10;

-- COMMAND ----------

-- Show lineage summary
SELECT * FROM apex_bank_demo.analytics.data_lineage_summary
-- ORDER BY depth_level;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Gold Layer powers BI Dashboards directly (Merchant Category analysis)

-- COMMAND ----------

-- Daily spend by merchant category
SELECT 
    transaction_date,
    merchant_category_desc,
    transaction_count,
    ROUND(total_amount, 2) as total_amount,
    ROUND(avg_amount, 2) as avg_amount,
    unique_accounts,
    fraud_count,
    ROUND(fraud_count * 100.0 / transaction_count, 2) as fraud_rate_pct
FROM apex_bank_demo.analytics.daily_merchant_category_summary
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY total_amount DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fraud Detection Insights

-- COMMAND ----------

-- Fraud rate by merchant category
SELECT 
    merchant_category_desc,
    SUM(transaction_count) as total_transactions,
    SUM(fraud_count) as total_fraud,
    ROUND(SUM(fraud_count) * 100.0 / SUM(transaction_count), 2) as fraud_rate_pct,
    ROUND(SUM(total_amount), 2) as total_amount
FROM apex_bank_demo.analytics.daily_merchant_category_summary
GROUP BY merchant_category_desc
HAVING SUM(fraud_count) > 0
ORDER BY fraud_rate_pct DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Features could come from account-level aggregates

-- COMMAND ----------

-- Preview ML features for fraud detection
SELECT 
    account_id,
    transaction_timestamp,
    amount,
    txn_count_7day,
    ROUND(avg_amount_7day, 2) as avg_amount_7day,
    ROUND(stddev_amount_7day, 2) as stddev_amount_7day,
    ROUND(max_amount_7day, 2) as max_amount_7day,
    card_not_present_count_7day,
    is_fraud
FROM apex_bank_demo.analytics.account_transaction_features
WHERE is_fraud = 1
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Go to Feature Store notebook
-- MAGIC
