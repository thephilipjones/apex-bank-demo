-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog Governance Setup
-- MAGIC
-- MAGIC **Goal:** Transform raw DLT data into a governed, secure Data Product.
-- MAGIC
-- MAGIC **Prerequisites:** Catalog and schemas already created by `01_setup_infrastructure.ipynb`
-- MAGIC
-- MAGIC **This notebook sets up:**
-- MAGIC 1. PII masking functions with group-based access
-- MAGIC 2. Reference tables (accounts, fraud_labels, audit log)
-- MAGIC 3. Secure masked views for PII-restricted users
-- MAGIC 4. Data lineage documentation
-- MAGIC 5. Role-based permissions

-- COMMAND ----------

USE CATALOG apex_bank_demo;
USE SCHEMA analytics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Create Masking Functions
-- MAGIC
-- MAGIC These functions mask sensitive data based on group membership.
-- MAGIC Only members of `fraud_analysts` can see unmasked PII.

-- COMMAND ----------

-- Function to mask PII strings (show last 4 characters only)
CREATE OR REPLACE FUNCTION mask_pii_string(val STRING)
RETURNS STRING
COMMENT 'Masks PII to show only last 4 characters unless user is fraud analyst'
RETURN CASE
    WHEN is_account_group_member('fraud_analysts') THEN val
    ELSE CONCAT('***-', RIGHT(val, 4))
END;

-- COMMAND ----------

-- Function to mask account numbers (show last 4 digits in card format)
CREATE OR REPLACE FUNCTION mask_account_number(account_num STRING)
RETURNS STRING
COMMENT 'Masks account number to show only last 4 digits'
RETURN CASE
    WHEN is_account_group_member('fraud_analysts') THEN account_num
    ELSE CONCAT('XXXX-XXXX-XXXX-', RIGHT(account_num, 4))
END;

-- COMMAND ----------

-- Function to mask email addresses
CREATE OR REPLACE FUNCTION mask_email(val STRING)
RETURNS STRING
COMMENT 'Masks email to show only domain'
RETURN CASE
    WHEN is_account_group_member('fraud_analysts') THEN val
    ELSE CONCAT('*****@', SPLIT(val, '@')[1])
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Create Reference Tables
-- MAGIC
-- MAGIC These tables complete the data model for the demo.

-- COMMAND ----------

-- Accounts table with PII fields for masking demo
CREATE OR REPLACE TABLE accounts (
    account_id STRING,
    account_number STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    ssn_last4 STRING,
    risk_score DOUBLE,
    state STRING,
    account_open_date DATE
)
USING DELTA
COMMENT 'Customer account master data with PII';

-- Seed sample account data
INSERT INTO accounts VALUES
('ACC-1001', '4532-8721-0934-1234', 'John Smith', 'john.smith@email.com', '555-123-4567', '1234', 0.12, 'NY', '2022-03-15'),
('ACC-1002', '4532-9182-7364-5678', 'Jane Doe', 'jane.doe@email.com', '555-234-5678', '5678', 0.85, 'CA', '2021-08-22'),
('ACC-1003', '4532-7263-9182-9012', 'Bob Wilson', 'bob.wilson@email.com', '555-345-6789', '9012', 0.05, 'TX', '2023-01-10'),
('ACC-1004', '4532-1928-3746-3456', 'Alice Brown', 'alice.brown@email.com', '555-456-7890', '3456', 0.92, 'FL', '2020-11-05'),
('ACC-1005', '4532-8374-6251-7890', 'Carlos Garcia', 'carlos.garcia@email.com', '555-567-8901', '7890', 0.45, 'AZ', '2022-06-18');

-- COMMAND ----------

-- Fraud labels table for ML training and ROI calculations
CREATE OR REPLACE TABLE fraud_labels (
    transaction_id STRING,
    account_id STRING,
    is_fraud INT,
    investigation_date DATE,
    investigation_status STRING COMMENT 'CONFIRMED_FRAUD, FALSE_POSITIVE_DECLINED, PENDING',
    amount DOUBLE COMMENT 'Transaction amount flagged',
    service_ticket_cost DOUBLE COMMENT 'Cost to handle investigation ($142 avg)',
    churn_risk_pct DOUBLE COMMENT 'Customer churn probability (35% for false positives)',
    false_positive_flag INT COMMENT '1 if transaction flagged but legitimate'
)
USING DELTA
COMMENT 'Fraud labels for ML training with false positive cost tracking';

-- Seed representative fraud label data demonstrating 1:8 false positive ratio
INSERT INTO fraud_labels VALUES
-- Confirmed fraud cases
('TXN-10001', 'ACC-1002', 1, '2024-01-15', 'CONFIRMED_FRAUD', 2847.50, 142.00, 0.0, 0),
('TXN-10002', 'ACC-1004', 1, '2024-01-16', 'CONFIRMED_FRAUD', 1523.00, 142.00, 0.0, 0),
-- False positive cases (8x more common)
('TXN-10003', 'ACC-1001', 0, '2024-01-15', 'FALSE_POSITIVE_DECLINED', 892.00, 142.00, 0.35, 1),
('TXN-10004', 'ACC-1003', 0, '2024-01-15', 'FALSE_POSITIVE_DECLINED', 1247.00, 142.00, 0.35, 1),
('TXN-10005', 'ACC-1001', 0, '2024-01-16', 'FALSE_POSITIVE_DECLINED', 567.00, 142.00, 0.35, 1),
('TXN-10006', 'ACC-1005', 0, '2024-01-16', 'FALSE_POSITIVE_DECLINED', 2100.00, 142.00, 0.35, 1),
('TXN-10007', 'ACC-1003', 0, '2024-01-17', 'FALSE_POSITIVE_DECLINED', 445.00, 142.00, 0.35, 1),
('TXN-10008', 'ACC-1002', 0, '2024-01-17', 'FALSE_POSITIVE_DECLINED', 1890.00, 142.00, 0.35, 1),
('TXN-10009', 'ACC-1004', 0, '2024-01-18', 'FALSE_POSITIVE_DECLINED', 723.00, 142.00, 0.35, 1),
('TXN-10010', 'ACC-1005', 0, '2024-01-18', 'FALSE_POSITIVE_DECLINED', 1156.00, 142.00, 0.35, 1);

-- COMMAND ----------

-- Access audit log for compliance tracking
CREATE OR REPLACE TABLE access_audit_log (
    event_time TIMESTAMP,
    user_email STRING,
    action STRING,
    table_accessed STRING,
    rows_accessed BIGINT
)
USING DELTA
COMMENT 'Audit log for data access tracking';

-- Seed initial audit event
INSERT INTO access_audit_log VALUES
(current_timestamp(), 'system_admin@apexbank.com', 'GRANT_PERMISSION', 'transactions_silver', 0),
(current_timestamp(), 'data_engineer@apexbank.com', 'SELECT', 'transactions_bronze', 100000),
(current_timestamp(), 'analyst@apexbank.com', 'SELECT', 'transactions_silver_masked', 50000);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Create Secure Masked Views
-- MAGIC
-- MAGIC Users query these views instead of underlying tables.
-- MAGIC Masking is automatically applied based on group membership.

-- COMMAND ----------

-- Secure view of silver transactions with masked account IDs
CREATE OR REPLACE VIEW transactions_silver_masked AS
SELECT
    transaction_id,
    mask_pii_string(account_id) AS account_id,
    transaction_timestamp,
    amount,
    merchant_category_code,
    merchant_category_desc,
    merchant_name,
    card_present_flag,
    transaction_type,
    is_fraud,
    processing_timestamp
FROM transactions_silver;

-- COMMAND ----------

-- Secure view of accounts with all PII masked
CREATE OR REPLACE VIEW accounts_masked AS
SELECT
    mask_pii_string(account_id) AS account_id,
    mask_account_number(account_number) AS account_number,
    mask_pii_string(customer_name) AS customer_name,
    mask_email(email) AS email,
    CONCAT('***-***-', RIGHT(phone, 4)) AS phone,
    CONCAT('***', ssn_last4) AS ssn_last4,
    risk_score,
    state,
    account_open_date
FROM accounts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Data Lineage Summary
-- MAGIC
-- MAGIC Documents the medallion architecture flow for lineage demos.

-- COMMAND ----------

CREATE OR REPLACE VIEW data_lineage_summary AS
SELECT
    'Bronze' AS layer,
    'transactions_bronze' AS table_name,
    'Ingestion' AS stage_type,
    'Raw CSV Stream' AS source,
    'Auto Loader ingestion from landing zone' AS description
UNION ALL
SELECT
    'Silver',
    'transactions_silver',
    'Refinement',
    'transactions_bronze',
    'Validated transactions (DLT expectations enforced)'
UNION ALL
SELECT
    'Silver',
    'transactions_quarantine',
    'Data Quality',
    'transactions_bronze',
    'Failed validation records for review'
UNION ALL
SELECT
    'Silver',
    'transactions_silver_masked',
    'Governance',
    'transactions_silver',
    'PII-masked view for restricted users'
UNION ALL
SELECT
    'Gold',
    'daily_merchant_category_summary',
    'Analytics',
    'transactions_silver',
    'Daily aggregations by merchant category'
UNION ALL
SELECT
    'Gold',
    'account_transaction_features',
    'Feature Store',
    'transactions_silver',
    '7-day rolling features for ML fraud detection';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Set Up Permissions
-- MAGIC
-- MAGIC Create `pii_restricted` group that can only access masked views.

-- COMMAND ----------

-- Grant pii_restricted group access ONLY to masked views
-- They cannot see underlying tables with raw PII
GRANT USAGE ON CATALOG apex_bank_demo TO `pii_restricted`;
GRANT USAGE ON SCHEMA apex_bank_demo.analytics TO `pii_restricted`;
GRANT SELECT ON apex_bank_demo.analytics.transactions_silver_masked TO `pii_restricted`;
GRANT SELECT ON apex_bank_demo.analytics.accounts_masked TO `pii_restricted`;
GRANT SELECT ON apex_bank_demo.analytics.data_lineage_summary TO `pii_restricted`;

-- COMMAND ----------

-- Grant fraud_analysts full access (they see unmasked data through the masking functions)
GRANT USAGE ON CATALOG apex_bank_demo TO `fraud_analysts`;
GRANT USAGE ON SCHEMA apex_bank_demo.analytics TO `fraud_analysts`;
GRANT SELECT ON SCHEMA apex_bank_demo.analytics TO `fraud_analysts`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Setup Complete
-- MAGIC
-- MAGIC ### Verification Steps:
-- MAGIC 1. Test masking: Run queries as different users
-- MAGIC 2. Check tables: `SHOW TABLES IN apex_bank_demo.analytics;`
-- MAGIC 3. Check grants: `SHOW GRANTS ON SCHEMA apex_bank_demo.analytics;`
-- MAGIC
-- MAGIC ### Demo Talking Points:
-- MAGIC
-- MAGIC **PII Masking:**
-- MAGIC - "Non-privileged users see `XXXX-XXXX-XXXX-1234` - automatic enforcement"
-- MAGIC - "Fraud analysts see full data - same view, different results based on identity"
-- MAGIC - "No code changes needed - governance is built into the platform"
-- MAGIC
-- MAGIC **False Positive ROI:**
-- MAGIC - "Our fraud_labels table tracks the true cost: $142 per investigation"
-- MAGIC - "False positives create 35% churn risk - that's $80M in customer LTV at risk"
-- MAGIC - "Better ML reduces false positives while catching more fraud"
-- MAGIC
-- MAGIC **Compliance:**
-- MAGIC - "Every access logged automatically via access_audit_log"
-- MAGIC - "Lineage tracked end-to-end - export for PCI auditors in minutes, not days"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 📊 Verification Queries

-- COMMAND ----------

-- Verify tables created
SHOW TABLES IN apex_bank_demo.analytics;

-- COMMAND ----------

-- Test masked view (you should see stars unless you're in fraud_analysts)
SELECT * FROM transactions_silver_masked LIMIT 5;

-- COMMAND ----------

-- Test accounts masking
SELECT * FROM accounts_masked;

-- COMMAND ----------

-- View data lineage
SELECT * FROM data_lineage_summary ORDER BY layer;

-- COMMAND ----------

-- Check fraud labels for ROI demo
SELECT
    investigation_status,
    COUNT(*) AS cases,
    ROUND(SUM(service_ticket_cost), 2) AS total_service_cost,
    ROUND(AVG(churn_risk_pct) * 100, 1) AS avg_churn_risk_pct
FROM fraud_labels
GROUP BY investigation_status;
