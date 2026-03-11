from pathlib import Path
import json
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / 'data' / 'copilot.db'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
row = cur.execute(
    '''
    SELECT channels_detail FROM backup_runs
    WHERE bot_name = ? AND formal_number = 4 AND is_test = 0
    ORDER BY run_id DESC LIMIT 1
    ''',
    ('tgporncopilot',)
).fetchone()
conn.close()

print('== channels_detail from backup_runs ==')
if not row:
    print('(missing run)')
    raise SystemExit(0)

channels = json.loads(row['channels_detail']) if row['channels_detail'] else []
for ch in channels:
    info = {
        'id': ch.get('id'),
        'name': ch.get('name'),
        'count': ch.get('count'),
        'new_count': ch.get('new_count'),
        'scanned_group_count': ch.get('scanned_group_count'),
        'raw_count': ch.get('raw_count'),
        'json_file': ch.get('json_file'),
        'md_file': ch.get('md_file'),
    }
    print(json.dumps(info, ensure_ascii=False))
    jf = ch.get('json_file')
    if jf:
        p = ROOT / jf
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding='utf-8'))
                print('  file_records=', len(data) if isinstance(data, list) else 'non-list')
            except Exception as e:
                print('  file_read_error=', e)
        else:
            print('  file_missing')
