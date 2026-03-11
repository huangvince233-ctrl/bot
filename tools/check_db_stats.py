import sqlite3
import os

db_path = 'data/copilot.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), SUM(LENGTH(COALESCE(text_content, '')) + LENGTH(COALESCE(file_name, ''))) FROM global_messages")
    count, total_chars = cursor.fetchone()
    print(f"Messages: {count}")
    print(f"Total Characters: {total_chars}")
    conn.close()
else:
    print("Database not found.")
