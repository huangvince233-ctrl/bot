import sqlite3
import json

db_path = 'f:/funny_project/tgporncopilot/data/copilot.db'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("--- Last 5 Backup Runs ---")
cursor.execute('SELECT run_id, is_test, formal_number, start_time, total_channels, total_messages, channels_detail FROM backup_runs ORDER BY run_id DESC LIMIT 5')
rows = cursor.fetchall()
for row in rows:
    detail = row['channels_detail'] if row['channels_detail'] else "[]"
    print(f"RunID: {row['run_id']}, Test: {row['is_test']}, Num: {row['formal_number']}, Time: {row['start_time']}")
    print(f"  Channels: {row['total_channels']}, Total Msgs: {row['total_messages']}")
    # print(f"  Detail: {detail[:200]}...")

print("\n--- Backup Offsets (Sample 5) ---")
cursor.execute('SELECT * FROM backup_offsets LIMIT 5')
rows = cursor.fetchall()
for row in rows:
    print(dict(row))

conn.close()
