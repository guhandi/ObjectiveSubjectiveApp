# survey_db_utils.py
import pandas as pd
import numpy as np
import json
import ast
import re
from typing import List, Optional, Tuple

# ---------------------------
# Helpers
# ---------------------------

def _parse_payload_cell(cell) -> dict:
    """Parse payload cell that might be JSON or a Python-like dict string."""
    if isinstance(cell, dict):
        return cell
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return {}
    s = str(cell).strip()
    if not s:
        return {}
    # Try strict JSON
    try:
        return json.loads(s)
    except Exception:
        pass
    # Fallback to Python literal (handles single quotes, True/False, None)
    try:
        return ast.literal_eval(s)
    except Exception:
        return {}

def _coerce_scalar(x):
    """Coerce string numerics to int/float when possible; leave others."""
    if isinstance(x, (int, float, np.number)):
        return x
    if isinstance(x, str):
        xs = x.strip()
        if xs == "":
            return np.nan
        try:
            if re.fullmatch(r"[+-]?\d+", xs):
                return int(xs)
            if re.fullmatch(r"[+-]?\d*\.\d+", xs):
                return float(xs)
        except Exception:
            return x
    return x

def _last_nonnull(s: pd.Series):
    """Given a time-sorted Series, return the last non-null value (NaN if none)."""
    s_nonnull = s.dropna()
    return s_nonnull.iloc[-1] if len(s_nonnull) else np.nan

def _flatten_survey_rows(df_survey: pd.DataFrame) -> pd.DataFrame:
    """
    For each survey row, flatten its payload into columns:
    <item_id>__<metric> = value  (excluding 'item_id' key itself).
    Returns a table with identifiers + flat metric columns.
    """
    flat_records: List[dict] = []
    for _, r in df_survey.iterrows():
        payload = r.get("_payload", {}) or {}
        item_id = payload.get("item_id", None)

        base = {
            "subject_id": r.get("subject_id"),
            "session_id": r.get("session_id"),
            "app_id": r.get("app_id"),
            "app_type": r.get("app_type"),
            "ts_utc": r.get("ts_utc"),
            "server_ts": r.get("server_ts"),
            "event_index": r.get("event_index"),
        }
        for k, v in payload.items():
            if k == "item_id":
                continue
            col = f"{item_id}__{k}" if item_id else f"no_item__{k}"
            base[col] = _coerce_scalar(v)
        flat_records.append(base)

    return pd.DataFrame(flat_records)

# ---------------------------
# DB access
# ---------------------------

_SURVEY_WHERE = """
LOWER(app_type) = 'survey' OR LOWER(COALESCE(event_type, '')) LIKE '%survey%'
"""

def _load_survey_events_from_db(
    conn,
    *,
    subject_id: Optional[str] = None,
    app_id: Optional[str] = None,
    table: str = "events",
) -> pd.DataFrame:
    """
    Pull survey rows from the DB with optional subject/app filters.
    """
    clauses = [f"({_SURVEY_WHERE})"]
    params: List[object] = []

    if subject_id is not None:
        clauses.append("CAST(subject_id AS TEXT) = CAST(? AS TEXT)")
        params.append(str(subject_id))

    if app_id is not None:
        clauses.append("CAST(app_id AS TEXT) = CAST(? AS TEXT)")
        params.append(str(app_id))

    where_sql = " AND ".join(clauses)
    sql = f"SELECT * FROM {table} WHERE {where_sql}"

    df = pd.read_sql_query(sql, conn, params=params)

    # Normalize expected columns in case some are missing
    for col in ["app_type", "event_type", "subject_id", "session_id", "app_id", "ts_utc", "server_ts", "event_index", "payload"]:
        if col not in df.columns:
            df[col] = pd.NA

    # Parse payloads and timestamps
    df["_payload"] = df["payload"].apply(_parse_payload_cell)
    for tcol in ["ts_utc", "server_ts"]:
        df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True)
    df["event_index"] = pd.to_numeric(df["event_index"], errors="coerce")

    # Sort so that "last" means latest in time/index
    df = df.sort_values(
        by=["subject_id", "session_id", "app_id", "ts_utc", "server_ts", "event_index"],
        ascending=[True, True, True, True, True, True],
    )

    return df

def _prepare_wide_by_app_from_db(
    conn,
    *,
    subject_id: Optional[str] = None,
    app_id: Optional[str] = None,
    table: str = "events",
) -> pd.DataFrame:
    """
    Load survey rows from DB, flatten payloads, and aggregate to one row per
    (subject_id, session_id, app_id, app_type) taking the latest non-null per column.
    """
    df_survey = _load_survey_events_from_db(conn, subject_id=subject_id, app_id=app_id, table=table)
    if df_survey.empty:
        return pd.DataFrame(columns=["subject_id", "session_id", "app_id", "app_type", "ts_utc"])

    flat = _flatten_survey_rows(df_survey)

    # Use the start timestamp as a unique session identifier
    flat['session_id'] = flat.groupby(['subject_id', 'app_id', 'app_type'])['ts_utc'].transform('min')

    group_cols = ["subject_id", "session_id", "app_id", "app_type"]
    metric_cols = [c for c in flat.columns if c not in group_cols + ["server_ts", "event_index"]]

    if not metric_cols:
        return flat[group_cols].drop_duplicates()

    wide = (
        flat.groupby(group_cols, dropna=False)[metric_cols]
        .agg(_last_nonnull)
        .reset_index()
    )
    return wide

def _resolve_survey_app_id_from_db(
    conn,
    survey: str,
    *,
    subject_id: Optional[str] = None,
    table: str = "events",
) -> str:
    """
    Resolve user-provided 'survey' to a concrete app_id using DB contents.
    - Prefer exact (case-insensitive) match on app_id.
    - Else allow substring match on app_id or app_type.
    - If ambiguous, raise with candidates.
    """
    s = str(survey).strip().lower()

    # Get distinct app_id/app_type (optionally filtered by subject for tighter matching)
    clauses = [f"({_SURVEY_WHERE})"]
    params: List[object] = []
    if subject_id is not None:
        clauses.append("CAST(subject_id AS TEXT) = CAST(? AS TEXT)")
        params.append(str(subject_id))
    where_sql = " AND ".join(clauses)

    sql = f"""
        SELECT DISTINCT CAST(app_id AS TEXT) AS app_id, CAST(app_type AS TEXT) AS app_type
        FROM {table}
        WHERE {where_sql}
    """
    df = pd.read_sql_query(sql, conn, params=params)

    # Exact app_id match
    exact = df.loc[df["app_id"].str.lower() == s, "app_id"].unique().tolist()
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise ValueError(f"Ambiguous survey '{survey}': multiple exact app_id matches: {exact}")

    # Substring on app_id or app_type
    mask = df["app_id"].str.lower().str.contains(s, na=False) | df["app_type"].str.lower().str.contains(s, na=False)
    candidates = df.loc[mask, "app_id"].unique().tolist()

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        available = df["app_id"].dropna().astype(str).unique().tolist()
        raise ValueError(
            f"No survey matched '{survey}'. Available app_id values include: {sorted(available)[:25]}..."
        )
    raise ValueError(f"Ambiguous survey '{survey}': candidates {candidates}. Please specify one app_id exactly.")

# ---------------------------
# Public API
# ---------------------------

def list_available_surveys(conn, *, subject_id: Optional[str] = None, table: str = "events") -> pd.DataFrame:
    """
    Return distinct surveys present (optionally for one subject).
    Columns: app_id, app_type, n_rows.
    """
    clauses = [f"({_SURVEY_WHERE})"]
    params: List[object] = []
    if subject_id is not None:
        clauses.append("CAST(subject_id AS TEXT) = CAST(? AS TEXT)")
        params.append(str(subject_id))
    where_sql = " AND ".join(clauses)

    sql = f"""
        SELECT CAST(app_id AS TEXT) AS app_id,
               CAST(app_type AS TEXT) AS app_type,
               COUNT(1) AS n_rows
        FROM {table}
        WHERE {where_sql}
        GROUP BY app_id, app_type
        ORDER BY app_id, app_type
    """
    return pd.read_sql_query(sql, conn, params=params)

def get_survey_data_for_subject(
    subject_id: str,
    survey: str,
    conn,
    *,
    table: str = "events",
    include_session_as_index: bool = True,
) -> pd.DataFrame:
    """
    Return a dataframe for a given subject and survey (app_id) where:
      - rows = sessions (session_id)
      - columns = metrics from that survey's payload, e.g., "<item_id>__value", "<item_id>__rt"
    Works for all surveys present in the DB.

    Args:
        subject_id: subject identifier to filter on.
        survey: survey identifier (prefer exact app_id, e.g., 'demo_all_inputs_v1').
                Substrings are allowed and resolved if unambiguous.
        conn: sqlite3 connection (e.g., sqlite3.connect(DB_PATH)).
        table: DB table name (default 'events').
        include_session_as_index: if True, return df indexed by session_id.

    Returns:
        pd.DataFrame with rows as session_id and columns as survey metric columns.
    """
    app_id = _resolve_survey_app_id_from_db(conn, survey, subject_id=subject_id, table=table)

    # Build a wide table for just this subject & this survey
    wide = _prepare_wide_by_app_from_db(conn, subject_id=subject_id, app_id=app_id, table=table)
    if wide.empty:
        raise ValueError(f"No rows found for subject_id='{subject_id}' and survey(app_id)='{app_id}'.")

    # Keep only metric columns (drop identifiers)
    id_cols = {"subject_id", "session_id", "app_id", "app_type"}
    metric_cols = [c for c in wide.columns if c not in id_cols]

    out = wide[["session_id"] + metric_cols].copy()

    # One row per session_id (if duplicates, take last non-null per column)
    out = (
        out.groupby("session_id", dropna=False)
        .agg(_last_nonnull)
        .reset_index()
        .sort_values("session_id")
    )

    if include_session_as_index:
        out = out.set_index("session_id")

    # Consistent column order
    out = out.reindex(sorted(out.columns), axis=1)
    return out
