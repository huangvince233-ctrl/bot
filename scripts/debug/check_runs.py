
import sqlite3
conn = sqlite3.connect('data/copilot.db')
print('=== runs boundary ===')
for r in conn.execute('SELECT run_id, formal_number, test_number, is_test, start_msg_id, end_msg_id FROM sync_runs ORDER BY run_id ASC').fetchall():
    print(r)
conn.close()
