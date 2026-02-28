import sqlite3
import json
import os

DB_PATH = 'data/tg_archives.db'

def get_schema():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"Tables: {tables}")
    
    for (table,) in tables:
        print(f"\n--- {table} ---")
        schema = cursor.execute(f"PRAGMA table_info({table})").fetchall()
        for col in schema:
            print(col)
            
    conn.close()

def get_latest_timestamps():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Try to find latest sync per channel
    # Usually sync_runs has start_time and we link messages via sync_run_id
    try:
        syncs = cursor.execute('''
            SELECT original_chat_id, MAX(r.start_time) 
            FROM messages m
            JOIN sync_runs r ON m.sync_run_id = r.run_id
            GROUP BY original_chat_id
        ''').fetchall()
        print(f"\nLatest Syncs: {syncs}")
    except Exception as e:
        print(f"Sync query failed: {e}")

    # Try to find latest backup per channel
    try:
        # Check if backup_runs table exists
        backups = cursor.execute('''
            SELECT chat_id, MAX(start_time)
            FROM backup_runs
            GROUP BY chat_id
        ''').fetchall()
        print(f"\nLatest Backups: {backups}")
    except Exception as e:
        print(f"Backup query failed: {e}")
        
    conn.close()

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        get_schema()
        get_latest_timestamps()
    else:
        print(f"DB not found at {DB_PATH}")
