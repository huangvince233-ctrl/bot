import sqlite3
import os

db_path = 'data/copilot.db'
if not os.path.exists(db_path):
    print("DB not found")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

chat_id = -1002784674222
cursor.execute("SELECT msg_id, res_id, res_video_id, res_photo_id FROM global_messages WHERE chat_id = ? AND res_id IS NOT NULL LIMIT 10", (chat_id,))
rows = cursor.fetchall()

if not rows:
    print(f"No indexed messages found for chat_id {chat_id}")
else:
    print(f"Found {len(rows)} messages with IDs:")
    for r in rows:
        print(r)

conn.close()
