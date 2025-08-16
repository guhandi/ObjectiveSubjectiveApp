import os
import json
import pandas as pd
from utils import save_to_parquet

# Define the path to the raw data
RAW_DATA_PATH = 'data/raw'
PROCESSED_DATA_PATH = 'data/processed'

# Function to parse cognition task JSON files

def parse_cognition(subject_id):
    cognition_path = os.path.join(RAW_DATA_PATH, subject_id, 'apps', 'cognition')
    features = []

    for root, _, files in os.walk(cognition_path):
        for file in files:
            if file.endswith('.json'):
                with open(os.path.join(root, file), 'r') as f:
                    data = json.load(f)
                    trials = data.get('trials', [])
                    # Compute per-session metrics
                    n_trials = len(trials)
                    n_correct = sum(trial['correct'] for trial in trials)
                    accuracy = n_correct / n_trials if n_trials > 0 else 0
                    median_rt_ms = pd.Series([trial['rt_ms'] for trial in trials]).median()
                    session_features = {
                        'subject_id': subject_id,
                        'app_id': data.get('app_id', ''),
                        'session_id': data.get('session_id', ''),
                        'local_date': data.get('started_ts_utc', '')[:10],
                        'n_trials': n_trials,
                        'n_correct': n_correct,
                        'accuracy': accuracy,
                        'median_rt_ms': median_rt_ms
                    }
                    features.append(session_features)

    # Convert to DataFrame and save
    df = pd.DataFrame(features)
    save_to_parquet(df, os.path.join(PROCESSED_DATA_PATH, subject_id, 'cognition', 'features_session.parquet'))

if __name__ == '__main__':
    # Example usage
    parse_cognition('subject_001')
