from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from pydantic import BaseModel
from typing import Dict, Any
import sqlite3
from fastapi.staticfiles import StaticFiles
import json
from datetime import datetime
import os
import uuid

app = FastAPI()

db_path = '/Users/guhansundar/Documents/GuData/ObjectiveSubjectiveHealth/data/database.db'

# Database setup
conn = sqlite3.connect(db_path)
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

# Endpoints
@app.get("/")
async def read_root():
    return {"message": "Welcome to the FastAPI application!"}

@app.post("/start-survey")
async def start_survey(subject_id: str, app_id: str, app_type: str):
    # Generate unique session_id
    session_id = str(uuid.uuid4())
    
    # Capture start timestamp
    start_timestamp = datetime.utcnow().isoformat() + 'Z'
    
    # Store session_id and start_timestamp in the database
    cursor.execute('''
    INSERT INTO sessions (subject_id, session_id, app_id, app_type, started_ts_utc)
    VALUES (?, ?, ?, ?, ?)
    ''', (subject_id, session_id, app_id, app_type, start_timestamp))
    conn.commit()
    
    return {"session_id": session_id, "start_timestamp": start_timestamp}

@app.post("/api/log")
async def log_event(event: LogEvent):
    try:
        cursor.execute('''
        INSERT INTO events (subject_id, session_id, app_id, app_type, event_type, event_index, ts_utc, tz, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (event.subject_id, event.session_id, event.app_id, event.app_type, event.event_type, event.event_index, event.ts_utc, event.tz, str(event.payload)))
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
        await log_event(key, value, event_index)
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