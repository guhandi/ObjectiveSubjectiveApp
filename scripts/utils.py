import os
import pandas as pd

# Function to map item IDs to numeric values
def map_item_ids(summary):
    # Example mapping logic
    return {k: int(v) for k, v in summary.items()}

# Function to reverse-code items
def reverse_code_items(summary):
    # Example reverse-coding logic
    reverse_coded = summary.copy()
    if 'stress' in reverse_coded:
        reverse_coded['stress'] = 6 - reverse_coded['stress']
    return reverse_coded

# Function to save DataFrame to Parquet
def save_to_parquet(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
