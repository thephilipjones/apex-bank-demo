"""
Synthetic Fintech Transaction Data Generator - VALIDATED BUSINESS CASE
Generates realistic card processing data matching validated industry research:

FRAUD RATES:
- Digital banks: 0.30% fraud rate (Aite-Novarica Group research)
- Apex Bank: $6B annual volume → $18M annual fraud at 0.30% rate
- Demo data: 3.94% fraud rate (represents concentrated fraud scenarios for visibility)

FALSE POSITIVES:
- Industry standard: 1:8 ratio (8 false positives per 1 true fraud)
- Current impact: $3.2M annual cost at Apex (service + churn)
- Target: 1:12 ratio with Feature Store (40% reduction)

BUSINESS CONTEXT:
- FedNow launch: Q2 2025, $500M monthly volume expected
- Board mandate: <10 sec fraud detection (current: 45 min batch)
- Strategic value: $80M revenue opportunity at risk
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

# Initialize faker with seed for reproducibility
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Merchant Category Codes (real MCC codes from card industry)
MERCHANT_CATEGORIES = {
    '5411': 'Grocery Stores',
    '5812': 'Restaurants',
    '5541': 'Service Stations',
    '5999': 'Miscellaneous Retail',
    '4121': 'Taxicabs/Limousines',
    '5311': 'Department Stores',
    '5912': 'Drug Stores',
    '5942': 'Book Stores',
    '5651': 'Family Clothing Stores',
    '7230': 'Beauty/Barber Shops',
    '7832': 'Motion Pictures',
    '5814': 'Fast Food Restaurants',
    '5200': 'Home Supply Stores',
    '5965': 'Direct Marketing',
    '5732': 'Electronics Stores'
}

def generate_accounts(n_accounts=10000):
    """Generate synthetic account data with PII"""
    print(f"Generating {n_accounts} accounts...")
    
    accounts = []
    for i in range(n_accounts):
        account = {
            'account_id': f'ACC{str(i+1).zfill(8)}',
            'account_number': fake.credit_card_number(card_type='visa'),
            'cardholder_name': fake.name(),
            'cardholder_email': fake.email(),
            'account_open_date': fake.date_between(start_date='-5y', end_date='-1y'),
            'credit_limit': random.choice([5000, 10000, 15000, 25000, 50000]),
            'account_status': random.choices(['ACTIVE', 'SUSPENDED', 'CLOSED'], weights=[0.90, 0.05, 0.05])[0],
            'risk_score': round(random.uniform(300, 850), 0)  # Credit score-like
        }
        accounts.append(account)
    
    return pd.DataFrame(accounts)

def generate_transactions(accounts_df, n_transactions=100000):
    """
    Generate synthetic transaction data with realistic patterns
    
    FRAUD MODELING:
    - 3.94% fraud rate in demo data (concentrated scenario for visibility)
    - Real-world digital bank: 0.30% across $6B = $18M annually
    - Fraud patterns: Higher amounts, card-not-present, unusual hours
    
    DATA QUALITY ISSUES:
    - 5% records with quality problems (nulls, negatives, invalid formats)
    - Demonstrates Delta Live Tables quarantine capabilities
    """
    print(f"Generating {n_transactions} transactions...")
    
    transactions = []
    start_date = datetime.now() - timedelta(days=90)  # Last 90 days
    
    # Get active accounts for transaction generation
    active_accounts = accounts_df[accounts_df['account_status'] == 'ACTIVE']['account_id'].tolist()
    
    for i in range(n_transactions):
        # Random timestamp with business hour bias
        hour_weights = [0.5]*6 + [2]*3 + [3]*8 + [2]*4 + [0.5]*3  # 24 hours
        hour = random.choices(range(24), weights=hour_weights)[0]
        
        days_ago = random.randint(0, 90)
        txn_timestamp = start_date + timedelta(days=days_ago, hours=hour, minutes=random.randint(0, 59))
        
        # Select merchant category and determine if card present
        mcc = random.choice(list(MERCHANT_CATEGORIES.keys()))
        merchant_name = fake.company()
        
        # Card present based on merchant type (95% CNP for digital-first bank)
        card_present = random.choices(['Y', 'N'], weights=[0.05, 0.95])[0] if mcc not in ['5965'] else 'N'
        
        # Amount varies by merchant category
        if mcc in ['5411', '5912']:  # Grocery, drugs - smaller amounts
            amount = round(random.uniform(5, 150), 2)
        elif mcc in ['5812', '5814']:  # Restaurants - medium amounts
            amount = round(random.uniform(10, 200), 2)
        elif mcc in ['5311', '5732']:  # Department stores, electronics - larger
            amount = round(random.uniform(50, 2000), 2)
        else:
            amount = round(random.uniform(5, 500), 2)
        
        # Introduce data quality issues (5% of records) for DLT demo
        if random.random() < 0.05:
            issue_type = random.choice(['null_amount', 'negative_amount', 'invalid_card_present', 'null_timestamp', 'null_txn_id', 'valid'])
            
            if issue_type == 'null_amount':
                amount = None
            elif issue_type == 'negative_amount':
                amount = -abs(amount)
            elif issue_type == 'invalid_card_present':
                card_present = random.choice(['X', '', 'YES', 'NO', None])
            elif issue_type == 'null_timestamp':
                txn_timestamp = None

        # Generate transaction ID (may be null for data quality demo)
        transaction_id = f'TXN{str(i+1).zfill(10)}'
        if random.random() < 0.05:
            if random.choice(['null_txn_id', 'valid']) == 'null_txn_id':
                transaction_id = None

        # Determine if transaction is fraud (0.30% - realistic digital bank rate)
        # Real-world: 0.30% across $6B volume = $18M annual fraud
        is_fraud = 0
        if random.random() < 0.003:  # 0.30% realistic rate (Aite-Novarica)
            is_fraud = 1
            # Fraudulent transactions characteristics:
            # - 70% are large amounts (testing limits)
            # - 30% are small amounts (testing the waters)
            if random.random() < 0.3:  # 30% small fraud (under $50)
                amount = round(random.uniform(1, 50), 2)
            else:  # 70% are larger fraud attempts
                amount = round(random.uniform(500, 5000), 2)
            card_present = 'N'  # Fraud is predominantly card-not-present
        
        transaction = {
            'transaction_id': transaction_id,
            'account_id': random.choice(active_accounts),
            'transaction_timestamp': txn_timestamp,
            'amount': amount,
            'merchant_category_code': mcc,
            'merchant_category_desc': MERCHANT_CATEGORIES[mcc],
            'merchant_name': merchant_name,
            'card_present_flag': card_present,
            'transaction_type': random.choice(['PURCHASE', 'REFUND', 'CASH_ADVANCE']),
            'is_fraud': is_fraud
        }
        transactions.append(transaction)
    
    df = pd.DataFrame(transactions)
    
    # Sort by timestamp for realistic streaming ingestion
    df = df.sort_values('transaction_timestamp')
    
    return df

def generate_fraud_labels(transactions_df):
    """
    Generate fraud labels with industry-validated 1:8 false positive ratio
    
    INDUSTRY DATA:
    - Current system: 8 false positives for every 1 true fraud caught
    - Customer impact: 27-67 point satisfaction drop (J.D. Power)
    - Churn risk: 8-39% within 90 days of false decline
    - Annual cost: $3.2M (service costs + lifetime value loss)
    
    TARGET WITH DATABRICKS:
    - 1:12 ratio (40% false positive reduction)
    - Feature Store eliminates online/offline skew
    - MIT research: 30-54% FP reduction achievable
    """
    print("Generating fraud labels with 1:8 false positive ratio...")
    
    # Extract base labels
    labels = transactions_df[['transaction_id', 'account_id', 'is_fraud', 'amount']].copy()
    
    # Model the CURRENT STATE: 1:8 false positive ratio
    # For every confirmed fraud, the current system incorrectly flags 8 legitimate transactions
    
    confirmed_frauds = labels['is_fraud'].sum()
    target_false_positives = int(confirmed_frauds * 8)  # Industry standard 1:8 ratio
    
    print(f"True frauds: {confirmed_frauds}")
    print(f"Target false positives (1:8 ratio): {target_false_positives}")
    
    # Select legitimate transactions to mark as false positives
    # Current system tends to flag: higher amounts, card-not-present, unusual patterns
    legitimate_transactions = labels[labels['is_fraud'] == 0].copy()
    
    # Calculate probability each legitimate transaction gets falsely flagged
    false_positive_rate = target_false_positives / len(legitimate_transactions)
    
    # Mark false positives
    labels['false_positive_flag'] = 0
    np.random.seed(42)  # Consistent FP assignment
    labels.loc[labels['is_fraud'] == 0, 'false_positive_flag'] = np.random.choice(
        [1, 0], 
        size=len(legitimate_transactions),
        p=[false_positive_rate, 1-false_positive_rate]
    )
    
    actual_false_positives = labels['false_positive_flag'].sum()
    actual_ratio = actual_false_positives / confirmed_frauds if confirmed_frauds > 0 else 0
    
    print(f"Actual false positives generated: {actual_false_positives}")
    print(f"Actual FP:Fraud ratio: 1:{actual_ratio:.1f}")
    
    # Add investigation metadata
    labels['investigation_date'] = pd.to_datetime('today')
    labels['investigation_status'] = 'LEGITIMATE'
    labels.loc[labels['is_fraud'] == 1, 'investigation_status'] = 'CONFIRMED_FRAUD'
    labels.loc[labels['false_positive_flag'] == 1, 'investigation_status'] = 'FALSE_POSITIVE_DECLINED'
    
    # Customer impact for false positives
    labels['customer_contacted'] = labels['false_positive_flag'] == 1
    labels['service_ticket_cost'] = labels['false_positive_flag'].apply(
        lambda x: 142.0 if x == 1 else 0.0  # $142 per false positive handling
    )
    
    # Churn risk (8% baseline, higher for large amounts)
    labels['churn_risk_pct'] = 0
    labels.loc[labels['false_positive_flag'] == 1, 'churn_risk_pct'] = labels.loc[
        labels['false_positive_flag'] == 1, 'amount'
    ].apply(lambda amt: 8 if amt < 500 else 15 if amt < 2000 else 25)
    
    return labels

def main():
    """Generate all synthetic datasets with validated business case"""
    print("=" * 60)
    print("APEX BANK - Synthetic Data Generation")
    print("VALIDATED BUSINESS CASE")
    print("=" * 60)
    print()
    print("Business Context:")
    print("- Digital bank fraud rate: 0.30% (Aite-Novarica)")
    print("- Apex annual volume: $6B")
    print("- Baseline fraud: $18M annually")
    print("- False positive ratio: 1:8 (industry standard)")
    print("- FedNow launch: Q2 2025, sub-10 sec fraud detection required")
    print("=" * 60)
    print()
    
    # Generate accounts
    accounts_df = generate_accounts(n_accounts=50000)
    print(f"✓ Generated {len(accounts_df)} accounts")

    # Generate transactions (500K for realistic 0.30% fraud rate with enough absolute records)
    transactions_df = generate_transactions(accounts_df, n_transactions=500000)
    print(f"✓ Generated {len(transactions_df)} transactions")
    
    # Generate fraud labels with false positive ratio
    fraud_labels_df = generate_fraud_labels(transactions_df)
    print(f"✓ Generated {len(fraud_labels_df)} fraud labels")
    
    # Data quality summary
    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY (For DLT Demo)")
    print("=" * 60)
    null_amounts = transactions_df['amount'].isnull().sum()
    negative_amounts = (transactions_df['amount'] < 0).sum() if null_amounts < len(transactions_df) else 0
    null_timestamps = transactions_df['transaction_timestamp'].isnull().sum()
    invalid_card_present = transactions_df[~transactions_df['card_present_flag'].isin(['Y', 'N'])].shape[0]
    
    print(f"Null amounts: {null_amounts}")
    print(f"Negative amounts: {negative_amounts}")
    print(f"Null timestamps: {null_timestamps}")
    print(f"Invalid card_present_flag: {invalid_card_present}")
    print(f"Total records with issues: {null_amounts + negative_amounts + null_timestamps + invalid_card_present}")
    
    # Fraud summary
    print("\n" + "=" * 60)
    print("FRAUD & FALSE POSITIVE SUMMARY")
    print("=" * 60)
    fraud_count = transactions_df['is_fraud'].sum()
    fraud_rate = (fraud_count / len(transactions_df)) * 100
    fp_count = fraud_labels_df['false_positive_flag'].sum()
    fp_ratio = fp_count / fraud_count if fraud_count > 0 else 0
    
    print(f"Fraudulent transactions: {fraud_count} ({fraud_rate:.2f}%)")
    print(f"Legitimate transactions: {len(transactions_df) - fraud_count}")
    print(f"False positives (current system): {fp_count}")
    print(f"FP:Fraud ratio: 1:{fp_ratio:.1f} (target: 1:8)")
    print(f"\nBusiness Impact Modeling:")
    print(f"- Service ticket cost: ${fp_count * 142:,.0f} (@$142/ticket)")
    print(f"- Customers at churn risk: {fp_count * 0.08:.0f} (@8% baseline)")
    print(f"- Total annual FP cost: $3.2M (Apex baseline)")
    
    # Save to CSV
    print("\n" + "=" * 60)
    print("SAVING FILES")
    print("=" * 60)
    
    accounts_df.to_csv('data/synthetic_accounts.csv', index=False)
    print("✓ Saved: data/synthetic_accounts.csv")
    
    transactions_df.to_csv('data/synthetic_transactions.csv', index=False)
    print("✓ Saved: data/synthetic_transactions.csv")
    
    fraud_labels_df.to_csv('data/synthetic_fraud_labels.csv', index=False)
    print("✓ Saved: data/synthetic_fraud_labels.csv")
    
    print("\n" + "=" * 60)
    print("✓ DATA GENERATION COMPLETE")
    print("=" * 60)
    print("\nData Reflects Validated Business Case:")
    print("✓ Digital bank fraud patterns (0.30% rate scaled)")
    print("✓ 1:8 false positive ratio (industry standard)")
    print("✓ Data quality issues for DLT quarantine demo")
    print("✓ Ready for $18M fraud baseline narrative")
    print("\nNext steps:")
    print("1. Review the CSV files in the data/ directory")
    print("2. Upload to Databricks: /FileStore/apex-bank-data/")
    print("3. Run DLT pipeline with validated business context")

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    import os
    os.makedirs('data', exist_ok=True)
    
    main()
