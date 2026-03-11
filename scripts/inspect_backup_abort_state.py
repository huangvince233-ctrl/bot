import os
import sqlite3
from pathlib import Path

base_dirs = [
    Path('data/archived/backups'),
    Path('docs/archived/backups'),
    Path('data/temp'),
]

print('=== filesystem scan ===')
for base in base_dirs:
    print(f'-- {base} --')
    if not base.exists():
        print('  (missing)')
        continue
    hits = []
    for path in base.rglob('*'):
        if not path.is_file():
            continue
        name = path.name
        if ('#B' in name) or ('PARTIAL' in name) or ('backup_progress' in name) or ('stop_backup' in name):
            try:
                mtime = path.stat().st_mtime
            except Exception:
                mtime = 0
            hits.append((mtime, str(path)))
    hits.sort(reverse=True)
    for _, p in hits[:80]:
        print(' ', p)

print('\n=== database scan ===')
conn = sqlite3.connect(os.path.join('data', 'copilot.db'))
conn.row_factory = sqlite3.Row
cur = conn.cursor()
rows = cur.execute('''
SELECT run_id, is_test, formal_number, start_time, end_time, backup_mode, is_incremental, total_messages, new_messages, bot_name
FROM backup_runs
ORDER BY run_id DESC
LIMIT 20
''').fetchall()
for r in rows:
    print(dict(r))

count_offsets = cur.execute('SELECT COUNT(*) FROM backup_offsets').fetchone()[0]
print('backup_offsets count =', count_offsets)
for r in cur.execute('SELECT chat_id, last_msg_id, updated_at, is_test FROM backup_offsets ORDER BY updated_at DESC LIMIT 20').fetchall():
    print(dict(r))
conn.close()
