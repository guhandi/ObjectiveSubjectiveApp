# survey_db_utils.py
import pandas as pd
import numpy as np
import json
import ast
import re
from typing import List, Optional, Tuple

def load_data(conn):
    events_df = pd.read_sql_query("SELECT * FROM events", conn)
    events_df['ts_utc'] = pd.to_datetime(events_df['ts_utc'])
    first_timestamps = events_df.groupby('session_id')['ts_utc'].first().reset_index()
    events_df = events_df.merge(first_timestamps, on='session_id', suffixes=('', '_first'))
    events_df.rename(columns={'ts_utc_first': 'session_timestamp'}, inplace=True)
    return events_df


def parse_survey_data(
        events_df,
        app_id,
        drop_items = ['subject_id', 'session_id', 'start_timestamp'],
        id_cols = ['subject_id', 'session_id', 'session_timestamp'],
):

    app_df = events_df.query("app_id == @app_id").set_index(id_cols)
    res_df = (
        app_df['payload'].apply(json.loads).apply(pd.Series)
        .query("item_id not in @drop_items")
        .reset_index()
        .pivot(index=id_cols, columns='item_id', values='value')
    )
    return res_df


def parse_pvt(
        events_df,
        app_id = 'pvt_1min_v1',
        use_items = ['trial_index', 'rt_ms', 'correct'],
        id_cols = ['subject_id', 'session_id', 'session_timestamp'],
        trial_level = False
):

    app_df = events_df.query("app_id == @app_id").set_index(id_cols)
    res_df = (
        app_df['payload'].apply(json.loads).apply(pd.Series)
        .query("phase != 'practice'")[use_items]
    )
    res_df["trial_index"] = (
        res_df.sort_values(["session_id", "trial_index"])
            .groupby("session_id")
            .cumcount() + 1
    )
    if not trial_level:
        res_df = res_df.groupby(id_cols)[['rt_ms']].mean()

    return res_df    


def parse_stroop(
        events_df,
        app_id = 'stroop',
        use_items = ['trial_index', 'rt', 'correct'],
        id_cols = ['subject_id', 'session_id', 'session_timestamp'],
):

    app_df = events_df.query("app_id == @app_id").set_index(id_cols)
    res_df = app_df['payload'].apply(json.loads).apply(pd.Series)
    res_df['correct'] = (res_df['key_pressed'] == res_df['expected_key']).astype(int)
    res_df['congruent'] = (res_df['word'].str.lower() == res_df['font_color'].str.lower()).astype(int)
    
    def score_stroop(trials_df):
        # keep only real task, correct trials for RT
        task_df = trials_df.query("phase != 'practice'")
        rt_correct = task_df[task_df["correct"] == 1]

        results = {}
        # accuracy
        results["acc_overall"] = task_df["correct"].mean()
        results["acc_congruent"] = task_df.loc[task_df["congruent"]==1,"correct"].mean()
        results["acc_incongruent"] = task_df.loc[task_df["congruent"]==0,"correct"].mean()
        results["acc_interference"] = results["acc_incongruent"] - results["acc_congruent"]

        # RT (ms)
        results["rt_mean_congruent"] = rt_correct.loc[rt_correct["congruent"]==1,"rt_ms"].mean()
        results["rt_mean_incongruent"] = rt_correct.loc[rt_correct["congruent"]==0,"rt_ms"].mean()
        results["rt_interference"] = results["rt_mean_incongruent"] - results["rt_mean_congruent"]

        # IES
        results["ies_congruent"] = results["rt_mean_congruent"] / results["acc_congruent"]
        results["ies_incongruent"] = results["rt_mean_incongruent"] / results["acc_incongruent"]
        results["ies_interference"] = results["ies_incongruent"] - results["ies_congruent"]

        return pd.Series(results)

    stroop_scores = (
        res_df
        .groupby(id_cols)
        .apply(score_stroop)
        .reset_index()
    )

    return stroop_scores