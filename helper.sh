#!/bin/bash
# Apex Bank Demo - Helper Commands
# Run these manually as needed during setup and demo preparation

# =============================================================================
# PREREQUISITES (one-time setup)
# =============================================================================

# Install Databricks CLI (modern version via Homebrew)
brew install databricks/tap/databricks

# Install Python dependencies for data generation
uv pip install pandas numpy faker

# Configure Databricks CLI (OAuth - recommended for modern CLI)
databricks auth login --host https://dbc-6a35a2dc-10db.cloud.databricks.com

# Alternative: token-based auth (legacy)
# databricks configure --token

# Verify connection
databricks auth profiles

# =============================================================================
# DATA GENERATION
# =============================================================================

# Generate synthetic data (creates 3 CSV files in data/ directory)
python apex_bank_demo/src/apex_bank_demo_etl/utilities/02_generate_data.py

# =============================================================================
# UPLOAD DATA TO DBFS
# =============================================================================

# Create target directory
databricks fs mkdirs dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone

# Upload all data files
databricks fs cp data/synthetic_transactions.csv dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone/
databricks fs cp data/synthetic_accounts.csv dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone/
databricks fs cp data/synthetic_fraud_labels.csv dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone/

# Verify uploads
databricks fs ls dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone/

# =============================================================================
# BUNDLE DEPLOYMENT
# =============================================================================

# Validate bundle configuration
databricks bundle validate -t prod

# Deploy bundle (creates/updates pipeline in Databricks)
databricks bundle deploy -t prod

# =============================================================================
# PIPELINE EXECUTION
# =============================================================================

# Run the DLT pipeline
databricks bundle run -t prod apex_bank_demo_etl

# Check pipeline status (get pipeline ID from Databricks UI or deploy output)
# databricks pipelines get <pipeline-id>

# =============================================================================
# UNITY CATALOG SETUP
# =============================================================================

# Create pii_restricted group for RBAC demo
# NOTE: Unity Catalog requires ACCOUNT-level groups (not workspace-level)
# Create via Account Console: https://accounts.cloud.databricks.com
#   → User management → Groups → Add group → "pii_restricted"
#
# Or via CLI with account auth:
# databricks auth login --account-id <account-id>
# databricks account groups create --display-name pii_restricted
#
# Add yourself to the group (for testing masked view access):
# databricks account groups patch <group-id> --json '{"operations": [{"op": "add", "path": "members", "value": [{"value": "<user-id>"}]}]}'
# Get group-id: databricks account groups list | grep pii_restricted
# Get user-id: databricks account users list | grep your@email.com

# =============================================================================
# CLEANUP (after demo or to reset)
# =============================================================================

# Drop orphaned dev/test schemas (run in Databricks SQL)
# DROP SCHEMA apex_bank_demo.philip CASCADE;
# DROP SCHEMA apex_bank_demo.prod CASCADE;

# Destroy bundle resources (removes pipeline from Databricks)
# databricks bundle destroy -t prod

# Delete uploaded data (to reset for fresh demo)
# databricks fs rm -r dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone/

# =============================================================================
# TROUBLESHOOTING
# =============================================================================

# Check what's in the catalog
# databricks schemas list apex_bank_demo

# View pipeline logs
# databricks pipelines list-updates <pipeline-id>

# Check bundle deployment status
databricks bundle summary -t prod
