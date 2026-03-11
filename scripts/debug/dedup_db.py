
import sqlite3
from datetime import datetime

def deduplicate():
    conn = sqlite3.connect('data/copilot.db')
    cursor = conn.cursor()
    
    def normalize_id(chat_id):
        if chat_id is None: return None
        return abs(int(chat_id)) % 1000000000000

    print("--- Dedup sync_offsets ---")
    rows = cursor.execute('SELECT chat_id, is_test, last_msg_id, updated_at, last_run_id FROM sync_offsets').fetchall()
    print(f"Total current rows: {len(rows)}")
    
    deduped = {}
    for chat_id, is_test, last_msg_id, updated_at, last_run_id in rows:
        norm_id = normalize_id(chat_id)
        key = (norm_id, is_test)
        
        # Parse timestamp if exists
        ts = 0
        if updated_at:
            try:
                ts = datetime.fromisoformat(updated_at).timestamp()
            except:
                pass
        
        if key not in deduped or ts > deduped[key]['ts']:
            deduped[key] = {
                'chat_id': norm_id,
                'is_test': is_test,
                'last_msg_id': last_msg_id,
                'updated_at': updated_at,
                'last_run_id': last_run_id,
                'ts': ts
            }
            
    cursor.execute('DELETE FROM sync_offsets')
    for d in deduped.values():
        cursor.execute('''
            INSERT INTO sync_offsets (chat_id, is_test, last_msg_id, updated_at, last_run_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (d['chat_id'], d['is_test'], d['last_msg_id'], d['updated_at'], d['last_run_id']))
    
    print(f"Final rows: {len(deduped)}")

    print("\n--- Dedup backup_offsets ---")
    rows = cursor.execute('SELECT chat_id, is_test, last_msg_id, updated_at FROM backup_offsets').fetchall()
    print(f"Total current rows: {len(rows)}")
    
    deduped_b = {}
    for chat_id, is_test, last_msg_id, updated_at in rows:
        norm_id = normalize_id(chat_id)
        key = (norm_id, is_test)
        
        ts = 0
        if updated_at:
            try:
                ts = datetime.fromisoformat(updated_at).timestamp()
            except:
                pass
        
        if key not in deduped_b or ts > deduped_b[key]['ts']:
            deduped_b[key] = {
                'chat_id': norm_id,
                'is_test': is_test,
                'last_msg_id': last_msg_id,
                'updated_at': updated_at,
                'ts': ts
            }
            
    cursor.execute('DELETE FROM backup_offsets')
    for d in deduped_b.values():
        cursor.execute('''
            INSERT INTO backup_offsets (chat_id, is_test, last_msg_id, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (d['chat_id'], d['is_test'], d['last_msg_id'], d['updated_at']))
        
    print(f"Final rows: {len(deduped_b)}")
    
    conn.commit()
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    deduplicate()
