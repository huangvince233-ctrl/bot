import os
import sys
import sqlite3
import json
from pprint import pprint

DB_PATH = os.path.join('data', 'copilot.db')
BOT_NAMES = ('my_bdsm_private_bot', 'my_porn_private_bot')

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('=== recent backup_runs ===')
rows = cur.execute(
    '''
    SELECT run_id, is_test, formal_number, start_time, end_time, backup_mode, is_incremental, total_messages, new_messages, bot_name, channels_detail
    FROM backup_runs
    WHERE bot_name IN (?, ?)
    ORDER BY run_id DESC
    LIMIT 10
    ''', BOT_NAMES
).fetchall()
for r in rows:
    label = f"#B{r['formal_number']}" if not r['is_test'] and r['formal_number'] is not None else f"TEST?/{r['run_id']}"
    print(f"run_id={r['run_id']} label={label} bot={r['bot_name']} inc={r['is_incremental']} total={r['total_messages']} new={r['new_messages']} start={r['start_time']}")
    try:
        channels = json.loads(r['channels_detail']) if r['channels_detail'] else []
        print(f"  channels={len(channels)}")
        for ch in channels[:3]:
            print(f"    - {ch.get('name')} id={ch.get('id') or ch.get('chat_id')} count={ch.get('count')} new={ch.get('new_count')} json={ch.get('json_file')}")
    except Exception as e:
        print(f"  channels parse error: {e}")

print('\n=== sample backup_offsets (top 20) ===')
rows = cur.execute(
    '''
    SELECT chat_id, last_msg_id, updated_at, is_test
    FROM backup_offsets
    ORDER BY updated_at DESC
    LIMIT 20
    '''
).fetchall()
for r in rows:
    print(dict(r))

conn.close()
