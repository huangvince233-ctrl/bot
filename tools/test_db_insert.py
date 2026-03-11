import sqlite3
import os
from datetime import datetime

def test_insert():
    db_path = 'data/copilot.db'
    print(f"Testing insertion into {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check current count
        cursor.execute("SELECT COUNT(*) FROM backup_runs")
        before = cursor.fetchone()[0]
        print(f"Count before: {before}")
        
        # Try insert
        cursor.execute('''
            INSERT INTO backup_runs (is_test, formal_number, start_time, bot_name)
            VALUES (?, ?, ?, ?)
        ''', (0, 999, datetime.now().isoformat(), 'test_bot'))
        conn.commit()
        
        # Check after
        cursor.execute("SELECT COUNT(*) FROM backup_runs")
        after = cursor.fetchone()[0]
        print(f"Count after: {after}")
        
        if after > before:
            print("✅ Insertion SUCCESS")
        else:
            print("❌ Insertion FAILED (no error but count didn't increase)")
            
    except Exception as e:
        print(f"❌ Error during insertion: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_insert()
