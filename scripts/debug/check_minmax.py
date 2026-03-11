
import sqlite3
conn = sqlite3.connect('data/copilot.db')
print('=== messages min/max ===')
for r in [1, 2, 3]:
    min_id = conn.execute(f"SELECT MIN(original_msg_id) FROM messages WHERE sync_run_id={r}").fetchone()[0]
    max_id = conn.execute(f"SELECT MAX(original_msg_id) FROM messages WHERE sync_run_id={r}").fetchone()[0]
    print(f"Run {r}: min {min_id}, max {max_id}")
conn.close()
