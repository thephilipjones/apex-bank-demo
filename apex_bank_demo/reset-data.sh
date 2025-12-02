#!/bin/bash
# Apex Bank Demo - Reset Data Script
# End-to-end data regeneration, upload, and pipeline execution
# Run from apex_bank_demo/: ./reset-data.sh

set -e  # Exit on error

# =============================================================================
# COLORS AND HELPERS
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}=============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=============================================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

prompt_continue() {
    echo ""
    read -p "$(echo -e ${YELLOW}"$1 (y/n): "${NC})" response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo ""
        print_warning "Aborted by user."
        exit 0
    fi
}

# Returns: 0=yes, 1=no, 2=quit
prompt_step() {
    echo ""
    read -p "$(echo -e ${YELLOW}"$1 [y]es / [n]o / [q]uit: "${NC})" response
    case "$response" in
        [Yy]* ) return 0 ;;
        [Nn]* ) return 1 ;;
        [Qq]* )
            echo ""
            print_warning "Aborted by user."
            exit 0
            ;;
        * )
            print_warning "Invalid input. Skipping step."
            return 1
            ;;
    esac
}

# =============================================================================
# CONFIGURATION
# =============================================================================

BUNDLE_DIR="."
DATA_DIR="data"
GENERATE_SCRIPT="src/apex_bank_demo_etl/utilities/02_generate_data.py"
VOLUME_PATH="dbfs:/Volumes/apex_bank_demo/raw_data/landing_zone"
CHECKPOINT_PATH="dbfs:/Volumes/apex_bank_demo/raw_data/checkpoints"
PIPELINE_NAME="apex_bank_demo_etl"
TARGET="prod"

# SQL Warehouse ID for executing DROP TABLE commands via CLI
# Find yours at: Databricks UI → SQL Warehouses → click warehouse → copy ID from URL
WAREHOUSE_ID="3c2ebf1eea3c0d31"  #"5ad50a0df1eabac8"  # e.g., "abc123def456"

# =============================================================================
# STEP 1: GENERATE DATA
# =============================================================================

print_header "STEP 1: GENERATE SYNTHETIC DATA"
echo "Will generate 500K transactions with 0.30% fraud rate"

if prompt_step "Generate new data?"; then
    echo ""
    echo "Running data generation script..."
    python "$GENERATE_SCRIPT"

    # Verify files were created
    if [[ -f "$DATA_DIR/synthetic_transactions.csv" ]] && \
       [[ -f "$DATA_DIR/synthetic_accounts.csv" ]] && \
       [[ -f "$DATA_DIR/synthetic_fraud_labels.csv" ]]; then
        print_success "Data files generated successfully"
        echo ""
        echo "Files created:"
        ls -lh "$DATA_DIR"/*.csv
    else
        print_error "Data generation failed - missing files"
        exit 1
    fi
else
    print_warning "Skipped data generation"
fi

# =============================================================================
# STEP 2: DROP DLT TABLES (Optional)
# =============================================================================

print_header "STEP 2: DROP DLT TABLES (Optional)"
echo "Drop existing DLT tables for a complete reset"
echo ""
print_warning "Only needed if you want to fully replace data (not append)"

if prompt_step "Drop DLT tables?"; then
    echo ""

    # SQL statements to drop tables
    DROP_STATEMENTS=(
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_bronze"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_silver"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_quarantine"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.daily_merchant_category_summary"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.account_transaction_features"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.accounts"
        "DROP TABLE IF EXISTS apex_bank_demo.analytics.fraud_labels"
    )

    if [[ -n "$WAREHOUSE_ID" ]]; then
        echo "Executing DROP TABLE commands via CLI..."
        echo ""

        for stmt in "${DROP_STATEMENTS[@]}"; do
            echo "  $stmt"
            databricks api post /api/2.0/sql/statements \
                --json "{\"warehouse_id\": \"$WAREHOUSE_ID\", \"statement\": \"$stmt\", \"wait_timeout\": \"30s\"}" \
                > /dev/null 2>&1
        done

        print_success "Tables dropped via CLI"
    else
        echo "No WAREHOUSE_ID configured. Copy and run this SQL in Databricks SQL Editor:"
        echo ""
        echo -e "${YELLOW}-- Drop DLT pipeline tables${NC}"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_bronze;"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_silver;"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.transactions_quarantine;"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.daily_merchant_category_summary;"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.account_transaction_features;"
        echo ""
        echo -e "${YELLOW}-- Drop reference tables (reloaded by 03_unity_catalog_setup.sql)${NC}"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.accounts;"
        echo "DROP TABLE IF EXISTS apex_bank_demo.analytics.fraud_labels;"
        echo ""
        print_warning "Tip: Set WAREHOUSE_ID in this script to automate this step"
        echo ""

        prompt_continue "Press 'y' after running the SQL in Databricks"
        print_success "Tables dropped"
    fi
else
    print_warning "Skipped dropping tables (pipeline will append to existing data)"
fi

# =============================================================================
# STEP 3: CLEAR LANDING ZONE AND CHECKPOINTS
# =============================================================================

print_header "STEP 3: CLEAR LANDING ZONE AND CHECKPOINTS"

# Verify local files exist before clearing remote
if [[ ! -f "$DATA_DIR/synthetic_transactions.csv" ]] || \
   [[ ! -f "$DATA_DIR/synthetic_accounts.csv" ]] || \
   [[ ! -f "$DATA_DIR/synthetic_fraud_labels.csv" ]]; then
    print_error "Local data files not found in $DATA_DIR/"
    print_warning "Run Step 1 first to generate data, or skip this step"
    echo ""
    echo "Expected files:"
    echo "  - $DATA_DIR/synthetic_transactions.csv"
    echo "  - $DATA_DIR/synthetic_accounts.csv"
    echo "  - $DATA_DIR/synthetic_fraud_labels.csv"
    echo ""
fi

# Show local file sizes
if [[ -f "$DATA_DIR/synthetic_transactions.csv" ]]; then
    LOCAL_TXN_SIZE=$(stat -f%z "$DATA_DIR/synthetic_transactions.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_transactions.csv")
    LOCAL_ACC_SIZE=$(stat -f%z "$DATA_DIR/synthetic_accounts.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_accounts.csv")
    LOCAL_FRD_SIZE=$(stat -f%z "$DATA_DIR/synthetic_fraud_labels.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_fraud_labels.csv")

    echo "Local files ready for upload:"
    echo "  transactions: $(numfmt --to=iec $LOCAL_TXN_SIZE 2>/dev/null || echo "$LOCAL_TXN_SIZE bytes")"
    echo "  accounts:     $(numfmt --to=iec $LOCAL_ACC_SIZE 2>/dev/null || echo "$LOCAL_ACC_SIZE bytes")"
    echo "  fraud_labels: $(numfmt --to=iec $LOCAL_FRD_SIZE 2>/dev/null || echo "$LOCAL_FRD_SIZE bytes")"
    echo ""
fi

echo "Will remove existing files from: $VOLUME_PATH"
echo "Will remove checkpoints from: $CHECKPOINT_PATH"
echo ""
print_warning "This ensures Auto Loader re-ingests all data files from scratch"

if prompt_step "Clear landing zone and checkpoints?"; then
    echo ""
    echo "Removing existing data from landing zone..."
    databricks fs rm -r "$VOLUME_PATH" 2>/dev/null || true

    echo "Recreating landing zone directory..."
    databricks fs mkdirs "$VOLUME_PATH"

    print_success "Landing zone cleared"

    echo ""
    echo "Removing Auto Loader checkpoints..."
    databricks fs rm -r "$CHECKPOINT_PATH" 2>/dev/null || true

    echo "Recreating checkpoints directory..."
    databricks fs mkdirs "$CHECKPOINT_PATH"

    print_success "Checkpoints cleared - Auto Loader will re-process all files"
else
    print_warning "Skipped clearing landing zone and checkpoints"
fi

# =============================================================================
# STEP 4: UPLOAD DATA
# =============================================================================

print_header "STEP 4: UPLOAD DATA TO DATABRICKS"
echo "Will upload CSV files to: $VOLUME_PATH"

if prompt_step "Upload data files?"; then
    echo ""

    # Capture local file sizes for comparison
    LOCAL_TXN_SIZE=$(stat -f%z "$DATA_DIR/synthetic_transactions.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_transactions.csv")
    LOCAL_ACC_SIZE=$(stat -f%z "$DATA_DIR/synthetic_accounts.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_accounts.csv")
    LOCAL_FRD_SIZE=$(stat -f%z "$DATA_DIR/synthetic_fraud_labels.csv" 2>/dev/null || stat -c%s "$DATA_DIR/synthetic_fraud_labels.csv")

    echo "Uploading synthetic_transactions.csv..."
    databricks fs cp "$DATA_DIR/synthetic_transactions.csv" "$VOLUME_PATH/" --overwrite
    print_success "Uploaded transactions"

    echo "Uploading synthetic_accounts.csv..."
    databricks fs cp "$DATA_DIR/synthetic_accounts.csv" "$VOLUME_PATH/" --overwrite
    print_success "Uploaded accounts"

    echo "Uploading synthetic_fraud_labels.csv..."
    databricks fs cp "$DATA_DIR/synthetic_fraud_labels.csv" "$VOLUME_PATH/" --overwrite
    print_success "Uploaded fraud labels"

    echo ""
    echo "Verifying uploads..."
    echo ""

    # Get remote file listing with sizes
    REMOTE_FILES=$(databricks fs ls -l "$VOLUME_PATH" 2>/dev/null)
    echo "Remote files:"
    echo "$REMOTE_FILES"
    echo ""

    # Validate files were uploaded
    if echo "$REMOTE_FILES" | grep -q "synthetic_transactions.csv"; then
        echo "Expected sizes (local):"
        echo "  transactions: $(numfmt --to=iec $LOCAL_TXN_SIZE 2>/dev/null || echo "$LOCAL_TXN_SIZE bytes")"
        echo "  accounts:     $(numfmt --to=iec $LOCAL_ACC_SIZE 2>/dev/null || echo "$LOCAL_ACC_SIZE bytes")"
        echo "  fraud_labels: $(numfmt --to=iec $LOCAL_FRD_SIZE 2>/dev/null || echo "$LOCAL_FRD_SIZE bytes")"
        echo ""
        print_success "All data uploaded and verified"
    else
        print_error "Upload verification failed - files not found in landing zone"
        exit 1
    fi
else
    print_warning "Skipped data upload"
fi

# =============================================================================
# STEP 5: LOAD REFERENCE TABLES
# =============================================================================

print_header "STEP 5: LOAD REFERENCE TABLES"
echo "Will load accounts and fraud_labels from CSVs into analytics schema"

if prompt_step "Load reference tables?"; then
    echo ""

    if [[ -n "$WAREHOUSE_ID" ]]; then
        echo "Loading accounts table..."
        databricks api post /api/2.0/sql/statements \
            --json "{\"warehouse_id\": \"$WAREHOUSE_ID\", \"statement\": \"CREATE OR REPLACE TABLE apex_bank_demo.analytics.accounts USING CSV OPTIONS (path '/Volumes/apex_bank_demo/raw_data/landing_zone/synthetic_accounts.csv', header 'true', inferSchema 'true')\", \"wait_timeout\": \"30s\"}" \
            > /dev/null 2>&1
        print_success "Loaded accounts table"

        echo "Loading fraud_labels table..."
        databricks api post /api/2.0/sql/statements \
            --json "{\"warehouse_id\": \"$WAREHOUSE_ID\", \"statement\": \"CREATE OR REPLACE TABLE apex_bank_demo.analytics.fraud_labels USING CSV OPTIONS (path '/Volumes/apex_bank_demo/raw_data/landing_zone/synthetic_fraud_labels.csv', header 'true', inferSchema 'true')\", \"wait_timeout\": \"30s\"}" \
            > /dev/null 2>&1
        print_success "Loaded fraud_labels table"
    else
        print_warning "No WAREHOUSE_ID set - run this SQL manually in Databricks:"
        echo ""
        echo -e "${YELLOW}-- Load reference tables from CSV${NC}"
        echo "CREATE OR REPLACE TABLE apex_bank_demo.analytics.accounts"
        echo "USING CSV"
        echo "OPTIONS (path '/Volumes/apex_bank_demo/raw_data/landing_zone/synthetic_accounts.csv', header 'true', inferSchema 'true');"
        echo ""
        echo "CREATE OR REPLACE TABLE apex_bank_demo.analytics.fraud_labels"
        echo "USING CSV"
        echo "OPTIONS (path '/Volumes/apex_bank_demo/raw_data/landing_zone/synthetic_fraud_labels.csv', header 'true', inferSchema 'true');"
        echo ""
        print_warning "Tip: Set WAREHOUSE_ID in this script to automate this step"
        echo ""

        prompt_continue "Press 'y' after running the SQL in Databricks"
        print_success "Reference tables loaded"
    fi
else
    print_warning "Skipped loading reference tables"
fi

# =============================================================================
# STEP 6: RUN PIPELINE
# =============================================================================

print_header "STEP 6: RUN DLT PIPELINE"
echo "Will trigger: $PIPELINE_NAME (target: $TARGET)"

if prompt_step "Run pipeline?"; then
    echo ""
    echo "Triggering pipeline: $PIPELINE_NAME"
    echo "Target: $TARGET"
    echo ""
    print_warning "Pipeline status will stream below. Press Ctrl+C when complete."
    echo ""

    # Run pipeline - this will stream status to terminal
    # Must run from bundle directory where databricks.yml is located
    (cd "$BUNDLE_DIR" && databricks bundle run -t "$TARGET" "$PIPELINE_NAME")
else
    print_warning "Skipped pipeline run"
fi

# =============================================================================
# COMPLETE
# =============================================================================

print_header "RESET COMPLETE"

echo "Summary:"
print_success "Generated 500K transactions, 50K accounts"
print_success "Uploaded to $VOLUME_PATH"
print_success "Pipeline $PIPELINE_NAME triggered"
echo ""
echo "Next steps:"
echo "  - Monitor pipeline in Databricks UI"
echo "  - Run 03_unity_catalog_setup.sql after pipeline completes"
echo ""
