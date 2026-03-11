import sqlite3
import json

def inspect_db():
    conn = sqlite3.connect('data/copilot.db')
    cursor = conn.cursor()
    
    # 1. List all tables
    print("--- Tables ---")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    print(tables)
    
    # 2. Check backup_runs content
    print("\n--- backup_runs (first 5) ---")
    cursor.execute("SELECT * FROM backup_runs LIMIT 5")
    print(cursor.fetchall())
    
    # 3. Check backup_runs schema
    print("\n--- backup_runs schema ---")
    cursor.execute("PRAGMA table_info(backup_runs)")
    print(cursor.fetchall())
    
    # 4. Check sync_runs content
    print("\n--- sync_runs (first 5) ---")
    cursor.execute("SELECT * FROM sync_runs LIMIT 5")
    print(cursor.fetchall())
    
    conn.close()

if __name__ == "__main__":
    inspect_db()
