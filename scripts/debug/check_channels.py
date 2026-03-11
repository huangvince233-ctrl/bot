
import sqlite3
conn = sqlite3.connect('data/copilot.db')
print('=== distinct channels ===')
for r in [1, 2, 3]:
    channels = conn.execute(f"SELECT DISTINCT original_chat_id FROM messages WHERE sync_run_id={r}").fetchall()
    print(f"Run {r}: {channels}")
conn.close()
