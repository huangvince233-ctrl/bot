from pathlib import Path
import sqlite3
import json

DB = Path(__file__).resolve().parents[1] / 'data' / 'copilot.db'
TARGETS = {1002919642039, 1002829404994}
BOT = 'tgporncopilot'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

updated_runs = []
for row in cur.execute(
    '''
    SELECT run_id, channels_detail
    FROM backup_runs
    WHERE bot_name = ? AND channels_detail IS NOT NULL
    ORDER BY run_id DESC
    ''',
    (BOT,),
).fetchall():
    detail = row['channels_detail']
    try:
        channels = json.loads(detail)
    except Exception:
        continue
    if not isinstance(channels, list):
        continue

    changed = False
    for ch in channels:
        cid = ch.get('id') or ch.get('chat_id')
        try:
            norm = abs(int(cid))
        except Exception:
            continue
        if norm not in TARGETS:
            continue

        json_file = ch.get('json_file')
        md_file = ch.get('md_file')
        if json_file and '绳赋(BDSM Lifestye)' in str(json_file):
            ch['json_file'] = None
            changed = True
        if md_file and '绳赋(BDSM Lifestye)' in str(md_file):
            ch['md_file'] = None
            changed = True

        if ch.get('status') == 'completed' and (ch.get('count', 0) or ch.get('new_count', 0)):
            # 保留计数信息，但标记为 historical_missing，表示历史记录存在、实体文件已手动删除
            ch['status'] = 'historical_missing'
            changed = True

    if changed:
        cur.execute(
            'UPDATE backup_runs SET channels_detail = ? WHERE run_id = ?',
            (json.dumps(channels, ensure_ascii=False), row['run_id'])
        )
        updated_runs.append(row['run_id'])

# 删除未完成的备份运行，避免污染最新状态判断
unfinished = cur.execute(
    '''
    SELECT run_id FROM backup_runs
    WHERE bot_name = ? AND end_time IS NULL
    ORDER BY run_id DESC
    ''',
    (BOT,),
).fetchall()
unfinished_ids = [r['run_id'] for r in unfinished]
for run_id in unfinished_ids:
    cur.execute('DELETE FROM backup_runs WHERE run_id = ?', (run_id,))

conn.commit()
print('updated_runs=', updated_runs)
print('deleted_unfinished_runs=', unfinished_ids)
conn.close()
