import os
import sqlite3
import json

conn = sqlite3.connect(os.path.join('data', 'copilot.db'))
conn.row_factory = sqlite3.Row
cur = conn.cursor()
rows = cur.execute('''
SELECT run_id, formal_number, start_time, end_time, backup_mode, is_incremental, total_messages, new_messages, channels_detail, bot_name
FROM backup_runs
ORDER BY run_id DESC
LIMIT 5
''').fetchall()
for r in rows:
    print({k: r[k] for k in r.keys() if k != 'channels_detail'})
    try:
        channels = json.loads(r['channels_detail']) if r['channels_detail'] else []
        print('channels_count=', len(channels))
        if channels:
            print('first=', channels[0])
            total = sum((c.get('count', 0) or 0) for c in channels)
            new_total = sum((c.get('new_count', 0) or 0) for c in channels)
            print('sum_count=', total, 'sum_new_count=', new_total)
    except Exception as e:
        print('parse error', e)
    print('-' * 60)
conn.close()
