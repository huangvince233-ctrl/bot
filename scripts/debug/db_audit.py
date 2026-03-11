
import sqlite3
import json

conn = sqlite3.connect('data/copilot.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

data = {}

# 1. sync_runs
data['sync_runs'] = [dict(r) for r in cursor.execute('SELECT * FROM sync_runs ORDER BY run_id ASC').fetchall()]

# 2. sync_offsets
data['sync_offsets'] = [dict(r) for r in cursor.execute('SELECT * FROM sync_offsets').fetchall()]

# 3. messages summary for the specific channel
chat_id = -1005051247857
norm_id = 5051247857
data['messages_summary'] = [dict(r) for r in cursor.execute('''
    SELECT sync_run_id, original_chat_id, MIN(original_msg_id) as min_id, MAX(original_msg_id) as max_id, COUNT(*) as count
    FROM messages 
    WHERE original_chat_id IN (?, ?, ?)
    GROUP BY sync_run_id, original_chat_id
''', (chat_id, norm_id, -norm_id)).fetchall()]

# 4. Check for any messages with ID > 15277
data['high_id_messages'] = [dict(r) for r in cursor.execute('''
    SELECT * FROM messages WHERE original_msg_id > 15277 AND original_chat_id IN (?, ?, ?)
''', (chat_id, norm_id, -norm_id)).fetchall()]

with open('scripts/debug/full_audit.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Audit data saved to scripts/debug/full_audit.json")
conn.close()
