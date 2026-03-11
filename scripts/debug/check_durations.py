import sqlite3
conn = sqlite3.connect('data/copilot.db')
print('=== runs duration ===')
for r in conn.execute('SELECT run_id, duration, start_time, end_time FROM sync_runs ORDER BY run_id ASC').fetchall():
    print(r)
conn.close()
