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

# Returns: 0=run, 1=skip, 2=quit
prompt_step() {
    echo ""
    read -p "$(echo -e ${YELLOW}"$1 [r]un / [s]kip / [q]uit: "${NC})" response
    case "$response" in
        [Rr]* ) return 0 ;;
        [Ss]* ) return 1 ;;
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
PIPELINE_NAME="apex_bank_demo_etl"
TARGET="prod"

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
# STEP 2: CLEAR LANDING ZONE
# =============================================================================

print_header "STEP 2: CLEAR LANDING ZONE"
echo "Will remove existing files from: $VOLUME_PATH"

if prompt_step "Clear landing zone?"; then
    echo ""
    echo "Removing existing data from landing zone..."
    databricks fs rm -r "$VOLUME_PATH" 2>/dev/null || true

    echo "Recreating landing zone directory..."
    databricks fs mkdirs "$VOLUME_PATH"

    print_success "Landing zone cleared and ready"
else
    print_warning "Skipped clearing landing zone"
fi

# =============================================================================
# STEP 3: UPLOAD DATA
# =============================================================================

print_header "STEP 3: UPLOAD DATA TO DATABRICKS"
echo "Will upload CSV files to: $VOLUME_PATH"

if prompt_step "Upload data files?"; then
    echo ""
    echo "Uploading synthetic_transactions.csv..."
    databricks fs cp "$DATA_DIR/synthetic_transactions.csv" "$VOLUME_PATH/"
    print_success "Uploaded transactions"

    echo "Uploading synthetic_accounts.csv..."
    databricks fs cp "$DATA_DIR/synthetic_accounts.csv" "$VOLUME_PATH/"
    print_success "Uploaded accounts"

    echo "Uploading synthetic_fraud_labels.csv..."
    databricks fs cp "$DATA_DIR/synthetic_fraud_labels.csv" "$VOLUME_PATH/"
    print_success "Uploaded fraud labels"

    echo ""
    echo "Verifying uploads..."
    databricks fs ls "$VOLUME_PATH"

    print_success "All data uploaded successfully"
else
    print_warning "Skipped data upload"
fi

# =============================================================================
# STEP 4: RUN PIPELINE
# =============================================================================

print_header "STEP 4: RUN DLT PIPELINE"
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
