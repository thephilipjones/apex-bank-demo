# Apex Bank Fraud Detection Demo

Databricks demo showcasing Delta Live Tables, Unity Catalog, and Feature Store for financial services fraud detection.

## Quick Start

1. Generate synthetic data:
   ```bash
   python generate_data.py
   ```

2. Upload to Databricks:
   ```bash
   databricks fs cp data/synthetic_transactions.csv dbfs:/FileStore/apex-bank-data/transactions/
   databricks fs cp data/synthetic_accounts.csv dbfs:/FileStore/apex-bank-data/accounts/
   databricks fs cp data/synthetic_fraud_labels.csv dbfs:/FileStore/apex-bank-data/fraud_labels/
   ```

3. Import notebooks to Databricks workspace and run in order:
   - `02_dlt_pipeline.py`
   - `03_unity_catalog_setup.sql`
   - `04_demo_walkthrough.sql`

## Requirements

- Python 3.8+
- Databricks CLI (`pip install databricks-cli`)
- Databricks workspace with Runtime 15.4 LTS+
