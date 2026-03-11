
import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('data/copilot.db')
print('Recently updated offsets (last 60 mins):')
cutoff = (datetime.now() - timedelta(minutes=60)).isoformat()
rows = conn.execute("SELECT chat_id, last_run_id, is_test, updated_at FROM sync_offsets").fetchall()
for r in rows:
    # Handle both T-separator and space-separator ISO formats
    updated_at = r[3]
    if not updated_at: continue
    try:
        dt = datetime.fromisoformat(updated_at.replace(' ', 'T'))
        if dt > datetime.now() - timedelta(minutes=60):
            print(r)
    except:
        pass
conn.close()
