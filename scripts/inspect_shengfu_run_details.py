from pathlib import Path
import sqlite3
import json

DB = Path(__file__).resolve().parents[1] / 'data' / 'copilot.db'
TARGETS = {1002919642039, 1002829404994}

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

rows = cur.execute(
    '''
    SELECT run_id, formal_number, start_time, end_time, channels_detail
    FROM backup_runs
    WHERE bot_name = ?
    ORDER BY run_id DESC
    LIMIT 10
    ''',
    ('tgporncopilot',)
).fetchall()

for row in rows:
    print(f"\n=== run_id={row['run_id']} formal={row['formal_number']} end_time={row['end_time']} ===")
    detail = row['channels_detail']
    if not detail:
        print('(no channels_detail)')
        continue
    try:
        channels = json.loads(detail)
    except Exception as e:
        print('channels_detail parse error:', e)
        continue
    matched = []
    for ch in channels:
        cid = ch.get('id') or ch.get('chat_id')
        try:
            norm = abs(int(cid))
        except Exception:
            continue
        if norm in TARGETS:
            matched.append({
                'id': cid,
                'name': ch.get('name'),
                'status': ch.get('status'),
                'count': ch.get('count'),
                'new_count': ch.get('new_count'),
                'json_file': ch.get('json_file'),
                'md_file': ch.get('md_file'),
            })
    if not matched:
        print('(no shengfu channels in this run)')
    else:
        print(json.dumps(matched, ensure_ascii=False, indent=2))

conn.close()
