import os
import sqlite3

DB_PATH = os.path.join('data', 'copilot.db')
BOT_NAMES = ('my_bdsm_private_bot', 'my_porn_private_bot')

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
rows = cur.execute('''
SELECT run_id, is_test, formal_number, start_time, backup_mode, is_incremental, total_messages, new_messages, bot_name
FROM backup_runs
WHERE bot_name IN (?, ?) AND formal_number = 1
ORDER BY run_id DESC
''', BOT_NAMES).fetchall()
for r in rows:
    print(dict(r))
conn.close()
