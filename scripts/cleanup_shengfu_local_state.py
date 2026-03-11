from pathlib import Path
import sqlite3
import json
import shutil

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / 'data' / 'copilot.db'
TARGETS = {1002919642039, 1002829404994}
TARGET_NAME = '绳赋(BDSM Lifestye)'

paths_to_remove = [
    ROOT / 'data' / 'metadata' / '精品捆绑' / f'{TARGET_NAME}.json',
    ROOT / 'docs' / 'metadata' / '精品捆绑' / f'{TARGET_NAME}.md',
    ROOT / 'docs' / 'tags' / 'tgporncopilot' / '精品捆绑' / '绳赋_BDSM_Lifestye_.md',
    ROOT / 'data' / 'temp' / '_debug_result_final.txt',
]

removed = []
missing = []
for path in paths_to_remove:
    if path.exists():
        path.unlink()
        removed.append(str(path.relative_to(ROOT)))
    else:
        missing.append(str(path.relative_to(ROOT)))

# 兜底删除任何旧的共享目录（如果还残留）
for rel in [
    Path('data/archived/backups/精品捆绑/绳赋(BDSM Lifestye)'),
    Path('docs/archived/backups/精品捆绑/绳赋(BDSM Lifestye)'),
]:
    p = ROOT / rel
    if p.exists() and p.is_dir():
        shutil.rmtree(p)
        removed.append(str(rel))

# 再确认两个目标没有任何 backup_offsets
conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute('DELETE FROM backup_offsets WHERE chat_id IN (?, ?)', tuple(TARGETS))
conn.commit()
left = cur.execute('SELECT chat_id, last_msg_id FROM backup_offsets WHERE chat_id IN (?, ?)', tuple(TARGETS)).fetchall()
conn.close()

force_full_path = ROOT / 'data' / 'temp' / 'force_full_backup_channels_tgporncopilot.json'
force_full_path.parent.mkdir(parents=True, exist_ok=True)
force_full_path.write_text(
    json.dumps({
        'chat_ids': sorted(TARGETS),
        'reason': 'shengfu-reset-for-next-b2',
    }, ensure_ascii=False, indent=2),
    encoding='utf-8'
)

print('removed=', json.dumps(removed, ensure_ascii=False, indent=2))
print('missing=', json.dumps(missing, ensure_ascii=False, indent=2))
print('remaining_offsets=', left)
print('force_full_file=', str(force_full_path.relative_to(ROOT)))
