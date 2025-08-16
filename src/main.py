from fastapi import FastAPI, HTTPException, Request, File, UploadFile, Form, Header
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import sqlite3
from fastapi.staticfiles import StaticFiles
import json
from datetime import datetime
import os
import uuid
from fastapi.responses import JSONResponse
import pandas as pd
from src.item_registry import is_valid_item
import logging
from fastapi_pagination import Page, paginate
from fastapi_pagination.paginator import paginate as custom_paginate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

db_path = '/Users/guhansundar/Documents/GuData/ObjectiveSubjectiveHealth/data/database.db'

# Database setup
conn = sqlite3.connect(db_path, check_same_thread=False)
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id TEXT,
    session_id TEXT,
    app_id TEXT,
    app_type TEXT,
    event_type TEXT,
    event_index INTEGER,
    ts_utc TEXT,
    tz TEXT,
    payload TEXT,
    server_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id TEXT,
    session_id TEXT,
    app_id TEXT,
    app_type TEXT,
    started_ts_utc TEXT,
    ended_ts_utc TEXT,
    tz TEXT,
    summary TEXT,
    events_count INTEGER,
    session_file_path TEXT,
    server_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id TEXT,
    session_id TEXT,
    modality TEXT,
    subtype TEXT,
    ts_utc TEXT,
    tz TEXT,
    path TEXT,
    meta_json TEXT,
    server_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

conn.commit()

# Models
class LogEvent(BaseModel):
    subject_id: str
    session_id: str
    app_id: str
    app_type: str
    event_type: str
    event_index: int
    ts_utc: str
    tz: str
    payload: Dict[str, Any]

class SessionComplete(BaseModel):
    subject_id: str
    session_id: str
    app_id: str
    app_type: str
    started_ts_utc: str
    ended_ts_utc: str
    tz: str
    summary: Dict[str, Any]
    events_count: int

# Pydantic models for request validation
class SessionStart(BaseModel):
    subject_id: str
    app_id: str
    app_type: str = Field(..., pattern='^(survey|task)$')
    tz: Optional[str] = None
    device_info: Optional[str] = None
    session_meta: Optional[dict] = None

class EventLog(BaseModel):
    session_id: str
    event_index: int
    ts_utc: str
    event_type: str
    item_id: str
    tz: Optional[str] = None
    payload_json: dict

class SessionFinish(BaseModel):
    session_id: str
    ts_end_utc: str

# Endpoints
@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI application!"}

class StartBody(BaseModel):
    subject_id: str
    app_id: str
    app_type: str = Field(..., pattern='^(survey|task)$')
    tz: Optional[str] = None
    device_info: Optional[str] = None
    session_meta: Optional[dict] = None

@app.post("/start-survey")
async def start_survey(body: StartBody):
    # Back-compat shim that calls /sessions/start
    return await start_session(SessionStart(**body.dict()))

@app.post("/api/log")
async def legacy_log_event(event: LogEvent):
    # For legacy callers that still send subject/app fields, write into current events schema
    try:
        # Resolve session metadata if present
        cursor.execute('SELECT subject_id, app_id, app_type FROM sessions WHERE session_id = ?', (event.session_id,))
        row = cursor.fetchone()
        subject_id, app_id, app_type = (row if row else (None, None, None))
        tz_val = event.tz or 'UTC'
        payload_text = json.dumps(event.payload_json)
        cursor.execute('''
        INSERT INTO events (subject_id, session_id, app_id, app_type, event_type, event_index, ts_utc, tz, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (subject_id, event.session_id, app_id, app_type, event.event_type, event.event_index, event.ts_utc, tz_val, payload_text))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/session_complete")
async def session_complete(session: SessionComplete):
    try:
        session_file_path = f"/data/raw/{session.subject_id}/apps/{session.app_id}/{session.started_ts_utc[:10]}/{session.session_id}.json"
        cursor.execute('''
        INSERT INTO sessions (subject_id, session_id, app_id, app_type, started_ts_utc, ended_ts_utc, tz, summary, events_count, session_file_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (session.subject_id, session.session_id, session.app_id, session.app_type, session.started_ts_utc, session.ended_ts_utc, session.tz, str(session.summary), session.events_count, session_file_path))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-speech")
async def upload_speech(subject_id: str = Form(...), session_id: str = Form(...), prompt_id: str = Form(...), speechFile: UploadFile = File(...)):
    # Define the directory path
    directory_path = f"/Users/guhansundar/Documents/GuData/ObjectiveSubjectiveHealth/data/raw/{subject_id}/speech/{datetime.utcnow().strftime('%Y-%m-%d')}"
    os.makedirs(directory_path, exist_ok=True)

    # Define the file path
    file_location = f"{directory_path}/{prompt_id}_{datetime.utcnow().strftime('%H%M%S')}_{speechFile.filename}"
    with open(file_location, "wb") as file:
        file.write(await speechFile.read())

    # Log the upload event to the database
    cursor.execute('''
    INSERT INTO assets (subject_id, session_id, modality, subtype, ts_utc, tz, path, meta_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (subject_id, session_id, "speech", prompt_id, datetime.utcnow().isoformat(), "UTC", file_location, "{}"))
    conn.commit()

    return {"info": f"file '{speechFile.filename}' saved at '{file_location}'"}

# Mount the tasks directory to serve static files
app.mount("/tasks", StaticFiles(directory="/Users/guhansundar/Documents/GuData/ObjectiveSubjectiveHealth/tasks"), name="tasks")

# Ensure the session complete event includes a summary of the survey
@app.post("/submit-survey")
async def submit_survey(form_data: Dict[str, Any]):
    event_index = 0
    for key, value in form_data.items():
        # This endpoint is deprecated; keep as no-op aggregator if still hit
        event_index += 1
    # Send session complete event
    response = await fetch(
        '/api/session_complete',
        method='POST',
        headers={'Content-Type': 'application/json'},
        body=json.dumps({
            'subject_id': 'demo',
            'session_id': 'sess_demo',
            'app_id': 'demo_all_inputs_v1',
            'app_type': 'survey',
            'started_ts_utc': datetime.utcnow().isoformat() + 'Z',
            'ended_ts_utc': datetime.utcnow().isoformat() + 'Z',
            'tz': 'UTC',
            'summary': form_data,
            'events_count': len(form_data)
        })
    )
    return {"status": "success"} 

@app.post("/sessions/start")
async def start_session(session: SessionStart):
    # Generate values
    session_id = str(uuid.uuid4())
    ts_start = datetime.utcnow().isoformat() + 'Z'
    tz_val = session.tz or 'UTC'

    # Insert session row (subject table optional; do best-effort insert if exists)
    try:
        cursor.execute('''
        INSERT INTO sessions (subject_id, session_id, app_id, app_type, started_ts_utc, tz, summary, events_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (session.subject_id, session_id, session.app_id, session.app_type, ts_start, tz_val, None, None))
        conn.commit()
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Failed to create session: {e}")

    logging.info(f"Session started: subject_id={session.subject_id}, session_id={session_id}, app_id={session.app_id}")
    return {"session_id": session_id, "ts_start_utc": ts_start, "tz": tz_val}

@app.post("/events")
async def log_event(event: EventLog, idempotency_key: Optional[str] = Header(None)):
    # Resolve session metadata
    cursor.execute('SELECT subject_id, app_id, app_type FROM sessions WHERE session_id = ?', (event.session_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Session not found")
    subject_id, app_id, app_type = row

    # Best-effort item validation
    try:
        if not is_valid_item(app_id, event.item_id):
            logging.warning(f"Unknown item_id '{event.item_id}' for app_id '{app_id}'")
    except Exception:
        pass

    # Idempotency check (simple)
    if idempotency_key:
        cursor.execute('''
        SELECT id FROM events WHERE session_id = ? AND event_index = ?
        ''', (event.session_id, event.event_index))
        if cursor.fetchone():
            logging.info(f"Duplicate event ignored: session_id={event.session_id}, event_index={event.event_index}")
            return {"status": "duplicate event ignored"}

    server_ts = datetime.utcnow().isoformat() + 'Z'
    tz_val = event.tz or 'UTC'
    try:
        cursor.execute('''
        INSERT INTO events (subject_id, session_id, app_id, app_type, event_type, event_index, ts_utc, tz, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (subject_id, event.session_id, app_id, app_type, event.event_type, event.event_index, event.ts_utc, tz_val, json.dumps({"item_id": event.item_id, **event.payload_json})))
        conn.commit()
        logging.info(f"Event logged: session_id={event.session_id}, event_index={event.event_index}, item_id={event.item_id}")
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Event insert failed: {e}")
    return {"status": "event logged"}

@app.post("/sessions/finish")
async def finish_session(session: SessionFinish):
    # Check if the session exists
    cursor.execute('''
    SELECT session_id FROM sessions WHERE session_id = ?
    ''', (session.session_id,))
    result = cursor.fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Update the session end time
    cursor.execute('''
    UPDATE sessions SET ended_ts_utc = ? WHERE session_id = ?
    ''', (session.ts_end_utc, session.session_id))
    conn.commit()
    
    # Log the session finish
    logging.info(f"Session finished: session_id={session.session_id}")
    return {"status": "session finished"}

@app.get("/wide")
async def get_wide_table(subject_id: str, app_id: str, app_version: Optional[int] = None):
    # Load events for the subject and app
    query = '''
    SELECT e.session_id, e.event_index, e.ts_utc, e.event_type, e.item_id, e.payload_json, s.ts_start_utc, s.ts_end_utc, s.app_id, s.app_version
    FROM events e
    JOIN sessions s ON e.session_id = s.session_id
    WHERE s.subject_id = ? AND s.app_id = ?
    '''
    params = [subject_id, app_id]
    if app_version is not None:
        query += " AND s.app_version = ?"
        params.append(app_version)
    query += " ORDER BY s.ts_start_utc"
    
    df = pd.read_sql_query(query, conn, params=params)
    
    # Pivot the table
    if df.empty:
        return JSONResponse(content={"message": "No data found"}, status_code=404)
    
    df['payload'] = df['payload_json'].apply(lambda x: eval(x))  # Convert JSON string to dict
    df = df.drop(columns=['payload_json'])
    
    # Flatten payload and create wide format
    payload_df = pd.json_normalize(df['payload'])
    df = df.drop(columns=['payload'])
    wide_df = pd.concat([df, payload_df], axis=1)
    
    # Create column names with item_id and field name
    wide_df.columns = [f"{item_id}__{col}" if col not in ['session_id', 'ts_start_utc', 'ts_end_utc', 'app_id', 'app_version'] else col for item_id, col in zip(df['item_id'], wide_df.columns)]
    
    # Group by session_id and take the last non-null value for each item_id__field
    wide_df = wide_df.groupby(['session_id', 'ts_start_utc', 'ts_end_utc', 'app_id', 'app_version']).last().reset_index()
    
    # Sort by ts_start_utc
    wide_df = wide_df.sort_values(by='ts_start_utc')
    
    return JSONResponse(content=wide_df.to_dict(orient='records')) 

@app.get("/health")
async def health_check():
    return {"status": "healthy"} 

@app.get("/subjects", response_model=Page[dict])
async def list_subjects():
    cursor.execute('''
    SELECT subject_id, created_at_utc, demographics_json, meta_json FROM subjects
    ''')
    subjects = cursor.fetchall()
    return paginate([dict(zip([column[0] for column in cursor.description], row)) for row in subjects])

@app.get("/sessions", response_model=Page[dict])
async def list_sessions(subject_id: Optional[str] = None):
    query = '''
    SELECT session_id, subject_id, app_id, app_version, ts_start_utc, ts_end_utc, tz, device_info, meta_json FROM sessions
    '''
    params = []
    if subject_id:
        query += " WHERE subject_id = ?"
        params.append(subject_id)
    cursor.execute(query, params)
    sessions = cursor.fetchall()
    return paginate([dict(zip([column[0] for column in cursor.description], row)) for row in sessions])

@app.get("/apps", response_model=Page[dict])
async def list_apps():
    cursor.execute('''
    SELECT app_id, app_type, app_version, schema_json FROM apps
    ''')
    apps = cursor.fetchall()
    return paginate([dict(zip([column[0] for column in cursor.description], row)) for row in apps]) 