
import sqlite3
conn = sqlite3.connect('data/copilot.db')
print('=== messages min/max by channel ===')
for r in conn.execute('SELECT sync_run_id, original_chat_id, MIN(original_msg_id), MAX(original_msg_id) FROM messages GROUP BY sync_run_id, original_chat_id').fetchall():
    print(r)
conn.close()
