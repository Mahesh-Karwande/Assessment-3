import pandas as pd
import json

def run_reconciliation(config_path):
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    # In Fabric, you would read from OneLake. For this local test, we use Pandas.
    df1 = pd.read_csv(config['source_1_path'])
    df2 = pd.read_csv(config['source_2_path'])
    
    # Apply filter if provided
    if config.get('source_1_filter'):
        df1 = df1.query(config['source_1_filter'])
        
    join_key = config['join_key']
    amt_col = config['amount_col']
    tolerance = config['tolerance']

    # Handle NULL join keys cleanly - immediately categorize as breaks
    df1_clean = df1.dropna(subset=[join_key])
    df2_clean = df2.dropna(subset=[join_key])

    # Merge dynamic configuration
    merged = pd.merge(df1_clean, df2_clean, on=join_key, how='outer', suffixes=('_loan', '_bank'), indicator=True)
    
    # Isolate breaks
    breaks_bank = merged[merged['_merge'] == 'left_only']
    breaks_loan = merged[merged['_merge'] == 'right_only']
    
    # Match logic and tolerance
    matched = merged[merged['_merge'] == 'both'].copy()
    matched['diff'] = abs(matched[f"{amt_col}_loan"] - matched[f"{amt_col}_bank"])
    
    perfect_matches = matched[matched['diff'] <= tolerance]
    mismatches = matched[matched['diff'] > tolerance]

    print(f"--- Fabric Recon Summary: {config['reconciliation_name']} ---")
    print(f"Matched: {len(perfect_matches)}")
    print(f"Mismatches: {len(mismatches)}")
    print(f"Breaks: {len(breaks_bank) + len(breaks_loan)}")

if __name__ == "__main__":
    run_reconciliation('config.json')
