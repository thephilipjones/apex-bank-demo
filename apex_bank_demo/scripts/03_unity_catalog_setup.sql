-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Setup for Apex Bank Demo
-- MAGIC 
-- MAGIC This notebook sets up:
-- MAGIC 1. Catalog and schema structure
-- MAGIC 2. PII masking policies
-- MAGIC 3. Role-based permissions
-- MAGIC 4. External data registration
-- MAGIC 
-- MAGIC Run this ONCE after creating your DLT pipeline.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Create Catalog Structure

-- COMMAND ----------

-- Create main catalog for the demo
CREATE CATALOG IF NOT EXISTS apex_bank
COMMENT 'Apex Bank card processing and fraud detection platform';

-- Use the catalog
USE CATALOG apex_bank;

-- COMMAND ----------

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS transactions
COMMENT 'Transaction processing data - bronze, silver, gold layers';

CREATE SCHEMA IF NOT EXISTS features
COMMENT 'Feature store for ML models';

CREATE SCHEMA IF NOT EXISTS models
COMMENT 'Registered ML models and monitoring';

CREATE SCHEMA IF NOT EXISTS compliance
COMMENT 'Audit logs and compliance reporting';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Register External Data Location
-- MAGIC 
-- MAGIC This step tells Unity Catalog where your CSV data lives.
-- MAGIC Adjust the path based on where you uploaded your synthetic data.

-- COMMAND ----------

-- Create external location for source data
-- Note: In production, this would be an S3/ADLS/GCS location
-- For demo, we use DBFS FileStore

CREATE EXTERNAL LOCATION IF NOT EXISTS apex_bank_source_data
URL 'dbfs:/FileStore/apex-bank-data/'
WITH (CREDENTIAL `databricks_file_store`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Create Masking Functions for PII
-- MAGIC 
-- MAGIC These functions mask sensitive data based on user roles.

-- COMMAND ----------

-- Function to mask account numbers (show last 4 digits only)
CREATE OR REPLACE FUNCTION mask_account_number(account_num STRING)
RETURNS STRING
COMMENT 'Masks account number to show only last 4 digits'
RETURN CONCAT('XXXX-XXXX-XXXX-', SUBSTRING(account_num, -4, 4));

-- COMMAND ----------

-- Function to mask cardholder names
CREATE OR REPLACE FUNCTION mask_cardholder_name(full_name STRING)
RETURNS STRING
COMMENT 'Masks cardholder name to first name initial + last name'
RETURN CONCAT(SUBSTRING(full_name, 1, 1), '. ', SPLIT(full_name, ' ')[1]);

-- COMMAND ----------

-- Function to mask email addresses
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
COMMENT 'Masks email to show only domain'
RETURN CONCAT('***@', SPLIT(email, '@')[1]);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Create Views with Conditional Masking
-- MAGIC 
-- MAGIC These views apply masking based on the current user's role.

-- COMMAND ----------

USE SCHEMA transactions;

-- COMMAND ----------

-- Create a masked view of the silver table for data scientists
CREATE OR REPLACE VIEW transactions_silver_masked AS
SELECT
    transaction_id,
    -- Apply masking to account_id based on user role
    CASE 
        WHEN is_member('compliance_team') THEN account_id
        WHEN is_member('data_engineers') THEN account_id
        ELSE mask_account_number(account_id)
    END AS account_id,
    transaction_timestamp,
    amount,
    merchant_category_code,
    merchant_category_desc,
    merchant_name,
    card_present_flag,
    transaction_type,
    is_fraud,
    processing_timestamp
FROM apex_bank.transactions.transactions_silver;

-- COMMAND ----------

-- Create accounts table with PII masking
-- First, we need to load the accounts data
-- This would typically be done via DLT or a separate ETL job

-- For demo purposes, create the table structure
CREATE TABLE IF NOT EXISTS apex_bank.transactions.accounts (
    account_id STRING,
    account_number STRING,
    cardholder_name STRING,
    cardholder_email STRING,
    account_open_date DATE,
    credit_limit DECIMAL(10,2),
    account_status STRING,
    risk_score DECIMAL(5,2)
)
USING DELTA
COMMENT 'Customer account master data with PII';

-- COMMAND ----------

-- Create fraud labels table (for ML training)
CREATE TABLE IF NOT EXISTS apex_bank.transactions.fraud_labels (
    transaction_id STRING,
    account_id STRING,
    is_fraud INT,
    investigation_date DATE,
    investigation_status STRING,
    false_positive_flag INT COMMENT '1 if transaction would be flagged but is legitimate (1:8 ratio)'
)
USING DELTA
COMMENT 'Fraud labels for ML training with false positive indicators';

-- COMMAND ----------

-- Create masked view of accounts table
CREATE OR REPLACE VIEW accounts_masked AS
SELECT
    account_id,
    -- Mask account number for non-privileged users
    CASE 
        WHEN is_member('compliance_team') THEN account_number
        WHEN is_member('data_engineers') THEN account_number
        ELSE mask_account_number(account_number)
    END AS account_number,
    -- Mask cardholder name
    CASE 
        WHEN is_member('compliance_team') THEN cardholder_name
        ELSE mask_cardholder_name(cardholder_name)
    END AS cardholder_name,
    -- Mask email
    CASE 
        WHEN is_member('compliance_team') THEN cardholder_email
        ELSE mask_email(cardholder_email)
    END AS cardholder_email,
    account_open_date,
    credit_limit,
    account_status,
    risk_score
FROM apex_bank.transactions.accounts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Create Data Quality Monitoring Views

-- COMMAND ----------

-- View for data quality dashboard
CREATE OR REPLACE VIEW data_quality_dashboard AS
SELECT
    DATE(current_timestamp()) as report_date,
    (SELECT COUNT(*) FROM apex_bank.transactions.transactions_bronze) as bronze_count,
    (SELECT COUNT(*) FROM apex_bank.transactions.transactions_silver) as silver_count,
    (SELECT COUNT(*) FROM apex_bank.transactions.transactions_quarantine) as quarantine_count,
    ROUND(
        (SELECT COUNT(*) FROM apex_bank.transactions.transactions_quarantine) * 100.0 / 
        (SELECT COUNT(*) FROM apex_bank.transactions.transactions_bronze),
        2
    ) as quarantine_rate_pct
FROM (SELECT 1); -- Dummy table for single row result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Set Up Permissions
-- MAGIC 
-- MAGIC Define role-based access control for different user groups.

-- COMMAND ----------

-- Grant permissions to data engineers (full access)
GRANT ALL PRIVILEGES ON CATALOG apex_bank TO `data_engineers`;

-- COMMAND ----------

-- Grant permissions to data scientists (read only + use masked views)
GRANT USAGE ON CATALOG apex_bank TO `data_scientists`;
GRANT USAGE ON SCHEMA apex_bank.transactions TO `data_scientists`;
GRANT USAGE ON SCHEMA apex_bank.features TO `data_scientists`;
GRANT SELECT ON apex_bank.transactions.transactions_silver_masked TO `data_scientists`;
GRANT SELECT ON apex_bank.transactions.accounts_masked TO `data_scientists`;

-- COMMAND ----------

-- Grant permissions to compliance team (read all, including PII)
GRANT USAGE ON CATALOG apex_bank TO `compliance_team`;
GRANT SELECT ON CATALOG apex_bank TO `compliance_team`;

-- COMMAND ----------

-- Grant permissions to ML engineers (read features, write models)
GRANT USAGE ON CATALOG apex_bank TO `ml_engineers`;
GRANT SELECT ON SCHEMA apex_bank.features TO `ml_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA apex_bank.models TO `ml_engineers`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Create Audit Log Table

-- COMMAND ----------

USE SCHEMA compliance;

CREATE TABLE IF NOT EXISTS access_audit_log (
    access_timestamp TIMESTAMP,
    user_name STRING,
    user_role STRING,
    table_name STRING,
    query_text STRING,
    rows_accessed BIGINT,
    pii_accessed BOOLEAN
)
USING DELTA
COMMENT 'Audit log for data access, especially PII access';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Create Lineage Documentation View

-- COMMAND ----------

USE SCHEMA transactions;

CREATE OR REPLACE VIEW data_lineage_summary AS
SELECT
    'bronze' as layer,
    'transactions_bronze' as table_name,
    'Raw ingestion from card processor' as description,
    'cloudFiles (CSV)' as source,
    0 as depth_level
UNION ALL
SELECT
    'silver' as layer,
    'transactions_silver' as table_name,
    'Cleaned and validated transactions' as description,
    'transactions_bronze' as source,
    1 as depth_level
UNION ALL
SELECT
    'gold' as layer,
    'daily_merchant_category_summary' as table_name,
    'Daily merchant category aggregations' as description,
    'transactions_silver' as source,
    2 as depth_level
UNION ALL
SELECT
    'gold' as layer,
    'account_transaction_features' as table_name,
    '7-day rolling account features for ML' as description,
    'transactions_silver' as source,
    2 as depth_level
UNION ALL
SELECT
    'features' as layer,
    'transaction_features' as table_name,
    'Feature store for fraud detection' as description,
    'account_transaction_features' as source,
    3 as depth_level;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Setup Complete
-- MAGIC 
-- MAGIC ### Next Steps:
-- MAGIC 1. **Verify catalog structure**: `SHOW CATALOGS;`
-- MAGIC 2. **Verify schemas**: `SHOW SCHEMAS IN apex_bank;`
-- MAGIC 3. **Check permissions**: `SHOW GRANTS ON CATALOG apex_bank;`
-- MAGIC 4. **Test masking**: Query as different users to verify PII masking
-- MAGIC 
-- MAGIC ### Demo Talking Points:
-- MAGIC 
-- MAGIC **Unity Catalog Structure:**
-- MAGIC - "We've organized our data into a catalog with clear schema boundaries"
-- MAGIC - "Transactions, Features, Models, and Compliance are logically separated"
-- MAGIC 
-- MAGIC **PII Masking:**
-- MAGIC - "Data scientists see masked account numbers: XXXX-XXXX-XXXX-1234"
-- MAGIC - "Masking is automatic - they can't accidentally access raw PII"
-- MAGIC - "Compliance team has full visibility when needed for PCI audits"
-- MAGIC - "This protects $6 billion in annual transaction volume"
-- MAGIC 
-- MAGIC **Permissions:**
-- MAGIC - "Data engineers have full access to build pipelines"
-- MAGIC - "Data scientists self-serve within governance guardrails"
-- MAGIC - "No more ticket queues for data access - saves 160+ hours per month"
-- MAGIC 
-- MAGIC **Lineage:**
-- MAGIC - "Every transformation tracked automatically"
-- MAGIC - "When auditors ask 'where does PII flow?' - export this graph"
-- MAGIC - "Banking case studies show 60% reduction in audit prep time"
-- MAGIC - "That's 40 hours per quarter down to 16 hours - documented and validated"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Verification Queries

-- COMMAND ----------

-- Check catalog structure
SHOW CATALOGS;

-- COMMAND ----------

-- Check schemas
SHOW SCHEMAS IN apex_bank;

-- COMMAND ----------

-- Check tables in transactions schema
SHOW TABLES IN apex_bank.transactions;

-- COMMAND ----------

-- Check permissions on catalog
SHOW GRANTS ON CATALOG apex_bank;

-- COMMAND ----------

-- Test data quality dashboard
SELECT * FROM apex_bank.transactions.data_quality_dashboard;

-- COMMAND ----------

-- Preview lineage
SELECT * FROM apex_bank.transactions.data_lineage_summary
ORDER BY depth_level;
