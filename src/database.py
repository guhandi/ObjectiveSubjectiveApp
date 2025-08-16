import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('/Users/guhansundar/Documents/GuData/ObjectiveSubjectiveHealth/data/database.db')
cursor = conn.cursor()

# Create subjects table
cursor.execute('''
CREATE TABLE IF NOT EXISTS subjects (
    subject_id TEXT PRIMARY KEY,
    created_at_utc TEXT NOT NULL,
    demographics_json TEXT,
    meta_json TEXT
)
''')

# Create apps table
cursor.execute('''
CREATE TABLE IF NOT EXISTS apps (
    app_id TEXT PRIMARY KEY,
    app_type TEXT CHECK(app_type IN ('survey', 'task')) NOT NULL,
    app_version INTEGER DEFAULT 1,
    schema_json TEXT
)
''')

# Create sessions table
cursor.execute('''
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    subject_id TEXT,
    app_id TEXT,
    app_version INTEGER,
    ts_start_utc TEXT NOT NULL,
    ts_end_utc TEXT,
    tz TEXT,
    device_info TEXT,
    meta_json TEXT,
    FOREIGN KEY(subject_id) REFERENCES subjects(subject_id),
    FOREIGN KEY(app_id) REFERENCES apps(app_id)
)
''')

# Create events table
cursor.execute('''
CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    event_index INTEGER,
    ts_utc TEXT NOT NULL,
    server_ts TEXT NOT NULL,
    event_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY(session_id) REFERENCES sessions(session_id),
    UNIQUE(session_id, event_index)
)
''')

# Commit changes and close the connection
conn.commit()
conn.close()
