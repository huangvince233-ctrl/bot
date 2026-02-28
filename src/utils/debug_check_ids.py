import sqlite3
import os

db_path = 'data/copilot.db'
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 检查最近同步任务 (RunID = 3) 的前 10 条消息
print("Checking Run ID 3 messages for res_text_id and res_msg_id...")
cursor.execute('''
    SELECT msg_type, res_text_id, res_msg_id, res_id 
    FROM messages 
    WHERE sync_run_id = 3 
    ORDER BY id ASC LIMIT 20
''')

rows = cursor.fetchall()
print(f"{'Type':<15} | {'Text ID':<8} | {'ResMsg ID':<10} | {'Total ID':<8}")
print("-" * 50)
for r in rows:
    print(f"{r[0]:<15} | {str(r[1]):<8} | {str(r[2]):<10} | {str(r[3]):<8}")

conn.close()
