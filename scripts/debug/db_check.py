import sqlite3
import json
import os

db_path = 'data/copilot.db'
if not os.path.exists(db_path):
    print(json.dumps({"error": f"Database not found at {db_path}"}))
    exit()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def get_count(table):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]
    except:
        return 0

results = {
    'sync_runs': get_count('sync_runs'),
    'backup_runs': get_count('backup_runs'),
    'global_messages': get_count('global_messages'),
    'entities': get_count('entities'),
}

try:
    cursor.execute("SELECT status, COUNT(*) FROM entities GROUP BY status")
    results['entities_by_status'] = dict(cursor.fetchall())
except:
    pass

try:
    cursor.execute("SELECT COUNT(*) FROM target_groups WHERE is_active = 1")
    results['active_target_groups'] = cursor.fetchone()[0]
except:
    pass

# Also list the last few backup labels
try:
    cursor.execute("SELECT run_id, formal_number, is_test FROM backup_runs ORDER BY run_id DESC LIMIT 5")
    results['last_backups'] = cursor.fetchall()
except:
    pass

# Check sync runs
try:
    cursor.execute("SELECT run_id, formal_number, is_test FROM sync_runs ORDER BY run_id DESC LIMIT 5")
    results['last_syncs'] = cursor.fetchall()
except:
    pass

print(json.dumps(results, indent=2))
conn.close()
