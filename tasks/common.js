(function(){
  window.current_subject_id = null;
  window.current_session_id = null;
  window.current_start_ts_utc = null;

  async function startSession(appId, appType) {
    const subjectId = await promptSubject();
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const res = await fetch('/sessions/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ subject_id: subjectId, app_id: appId, app_type: appType, tz })
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error('Failed to start session: ' + txt);
    }
    const data = await res.json();
    window.current_subject_id = subjectId;
    window.current_session_id = data.session_id;
    window.current_start_ts_utc = data.ts_start_utc;
  }

  function promptSubject() {
    return new Promise((resolve) => {
      const existing = localStorage.getItem('subject_id');
      if (existing) return resolve(existing);
      const id = window.prompt('Enter Subject ID:');
      localStorage.setItem('subject_id', id);
      resolve(id);
    });
  }

  async function logEventGeneric({ eventType, itemId, eventIndex, payload }) {
    if (!window.current_session_id) throw new Error('Session not started');
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const ts = new Date().toISOString();
    await fetch('/events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: window.current_session_id,
        event_index: eventIndex,
        ts_utc: ts,
        tz: tz,
        event_type: eventType,
        item_id: itemId,
        payload_json: payload || {}
      })
    });
  }

  async function finishSession() {
    if (!window.current_session_id) return;
    const tsEnd = new Date().toISOString();
    await fetch('/sessions/finish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: window.current_session_id, ts_end_utc: tsEnd })
    });
  }

  window.initializeSessionMetadata = async function(appId, appType) {
    if (!window.current_session_id) {
      await startSession(appId, appType);
    }
  };
  window.logEventGeneric = logEventGeneric;
  window.finishSession = finishSession;
})();
