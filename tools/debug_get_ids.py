import sqlite3
import os

db_path = 'data/copilot.db'
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    print("--- Sync Runs (bot_name & chat_id) ---")
    try:
        # Check if bot_name column exists
        c.execute("PRAGMA table_info(sync_runs)")
        cols = [col[1] for col in c.fetchall()]
        
        if 'bot_name' in cols:
            c.execute("SELECT DISTINCT bot_name FROM sync_runs")
            print(f"Bots found in sync_runs: {c.fetchall()}")
        
        # Look for group IDs in global_messages if available
        c.execute("SELECT DISTINCT chat_id FROM global_messages LIMIT 20")
        print(f"Distinct chat_ids in global_messages: {c.fetchall()}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    conn.close()
else:
    print("Database not found.")
