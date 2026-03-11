
import sqlite3
import json
import os

db_path = 'data/copilot.db'
if not os.path.exists(db_path):
    print(json.dumps({"error": "Database not found"}))
    exit()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def get_runs(is_test):
    cursor.execute("SELECT run_id, formal_number, is_test, start_time FROM sync_runs WHERE is_test = ? ORDER BY run_id DESC", (is_test,))
    return cursor.fetchall()

test_runs = get_runs(1)
formal_runs = get_runs(0)

print(f"Test Sync Runs ({len(test_runs)}):")
for r in test_runs:
    print(f"  ID: {r[0]}, Label: TEST-{r[0]}, Time: {r[3]}")

print(f"\nFormal Sync Runs ({len(formal_runs)}):")
for r in formal_runs:
    print(f"  ID: {r[0]}, Label: #{r[1]}, Time: {r[3]}")

# Check for messages with forwarded_msg_id = 0
cursor.execute("SELECT COUNT(*) FROM messages WHERE forwarded_msg_id = 0 OR forwarded_msg_id IS NULL")
zero_fwd = cursor.fetchone()[0]
print(f"\nMessages with missing/zero forwarded_msg_id: {zero_fwd}")

# Check for messages with header_msg_id = 0
cursor.execute("SELECT COUNT(*) FROM messages WHERE header_msg_id = 0 OR header_msg_id IS NULL")
zero_hdr = cursor.fetchone()[0]
print(f"Messages with missing/zero header_msg_id: {zero_hdr}")

conn.close()
