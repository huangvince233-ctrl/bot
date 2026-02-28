import sqlite3
import os

db_path = 'data/copilot.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 清空同步记录相关表
    tables_to_clear = ['sync_runs', 'messages', 'sync_offsets', 'resource_counters']
    
    for table in tables_to_clear:
        try:
            cursor.execute(f"DELETE FROM {table}")
            cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
            print(f"✅ Table {table} cleared and sequence reset.")
        except sqlite3.OperationalError as e:
            print(f"⚠️ Could not clear table {table}: {e}")
            
    conn.commit()
    conn.close()
    print("🏁 Database reset complete. Next sync will be TEST-1.")
else:
    print("❌ Database file not found.")
