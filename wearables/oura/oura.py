import pandas as pd
import os


def load_oura_data(data_dir):
    files = os.listdir(data_dir)
    csv_files = [os.path.join(data_dir, file) for file in files if file.endswith('.csv')]
    use_keys = ['dailysleep', 'dailyresilience', 'dailystress', 'dailyreadiness', 'sleep', 'dailyspo2', 'dailycardiovascularage', 'dailyactivity']
    hr_key = ['heartrate']

    df_dict = {}
    for fn in csv_files:
        tdf = pd.read_csv(fn)
        key = fn.split('/')[-1].split('_')[0]
        if key in use_keys:
            df_dict[key] = tdf
        elif key in hr_key:
            hrdf = tdf

    # Format Heartrate
    hrdf['timestamp'] = pd.to_datetime(hrdf['timestamp'])
    hrdf['day'] = hrdf['timestamp'].dt.date
    hrdf['minute'] = hrdf['timestamp'].dt.floor('T')  # Floor to the nearest minute
    unique_minutes = hrdf.groupby(['day', 'minute']).size().reset_index(name='count')
    compliance = unique_minutes.groupby('day').size().reset_index(name='minutes_with_data')
    compliance['compliance'] = (compliance['minutes_with_data'] / 1440)  # 1440 minutes in a day
    mean_bpm = hrdf.groupby('day')['bpm'].mean().reset_index(name='bpm')
    hr_daily = pd.merge(mean_bpm, compliance, on='day').set_index('day')


    # Combine and merge for each day
    processed_dfs = [hr_daily]
    for key, df in df_dict.items():
        df['day'] = pd.to_datetime(df['day']).dt.date
        numeric_cols = df.select_dtypes(include='number').columns
        if len(numeric_cols) == 0: continue
        df = df.groupby('day')[numeric_cols].agg('mean')
        df_renamed = df.rename(columns={col: f"{col}-{key}" for col in numeric_cols})
        processed_dfs.append(df_renamed)

    combined_df = pd.concat(processed_dfs, axis=1)
    return combined_df


