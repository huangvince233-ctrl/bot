import sqlite3
import os

db_path = 'data/copilot.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

chat_id = -1002784674222
cursor.execute("SELECT COUNT(*) FROM global_messages WHERE chat_id = ?", (chat_id,))
total = cursor.fetchone()[0]
print(f"Total messages for {chat_id}: {total}")

if total > 0:
    cursor.execute("SELECT msg_id, res_id, msg_type FROM global_messages WHERE chat_id = ? LIMIT 5", (chat_id,))
    rows = cursor.fetchall()
    print("First 5 messages:")
    for r in rows:
        print(r)
    
    cursor.execute("SELECT msg_id, res_id FROM global_messages WHERE chat_id = ? AND res_id IS NOT NULL ORDER BY msg_id DESC LIMIT 5", (chat_id,))
    rows = cursor.fetchall()
    print("Latest 5 indexed messages:")
    for r in rows:
        print(r)

conn.close()
