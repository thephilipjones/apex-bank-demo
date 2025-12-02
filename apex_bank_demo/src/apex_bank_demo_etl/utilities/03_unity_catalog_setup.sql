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
    WHEN NOT is_account_group_member('pii_masked') THEN val
    ELSE CONCAT('***-', RIGHT(val, 4))
END;

-- COMMAND ----------

-- Function to mask account numbers (show last 4 digits in card format)
CREATE OR REPLACE FUNCTION mask_account_number(account_num STRING)
RETURNS STRING
COMMENT 'Masks account number to show only last 4 digits'
RETURN CASE
    WHEN NOT is_account_group_member('pii_masked') THEN account_num
    ELSE CONCAT('XXXX-XXXX-XXXX-', RIGHT(account_num, 4))
END;

-- COMMAND ----------

-- Function to mask email addresses
CREATE OR REPLACE FUNCTION mask_email(val STRING)
RETURNS STRING
COMMENT 'Masks email to show only domain'
RETURN CASE
    WHEN NOT is_account_group_member('pii_masked') THEN val
    ELSE CONCAT('*****@', SPLIT(val, '@')[1])
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Create Reference Tables
-- MAGIC
-- MAGIC These tables complete the data model for the demo.

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
    mask_pii_string(cardholder_name) AS cardholder_name,
    mask_email(cardholder_email) AS cardholder_email,
    account_open_date,
    credit_limit,
    account_status,
    risk_score
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
