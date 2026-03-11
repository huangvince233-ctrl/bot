from pathlib import Path
import sqlite3
import json

DB = Path(__file__).resolve().parents[1] / 'data' / 'copilot.db'
CHAT_IDS = [1002919642039, 1002829404994]
BOT = 'tgporncopilot'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('== backup_offsets ==')
rows = cur.execute(
    'SELECT chat_id, last_msg_id, updated_at, is_test FROM backup_offsets WHERE chat_id IN (?, ?) ORDER BY chat_id',
    CHAT_IDS,
).fetchall()
for r in rows:
    print(dict(r))
if not rows:
    print('(empty)')

print('\n== backup_runs latest B2-like ==')
rows = cur.execute(
    '''
    SELECT run_id, formal_number, start_time, end_time, total_channels, total_messages, new_messages, is_incremental, bot_name, channels_detail
    FROM backup_runs
    WHERE bot_name = ?
    ORDER BY run_id DESC
    LIMIT 5
    ''',
    (BOT,),
).fetchall()
for r in rows:
    d = dict(r)
    detail = d.get('channels_detail')
    if detail and len(detail) > 500:
        d['channels_detail'] = detail[:500] + '...'
    print(json.dumps(d, ensure_ascii=False, indent=2))

conn.close()
