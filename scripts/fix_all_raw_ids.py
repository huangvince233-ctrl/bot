#!/usr/bin/env python3
"""
直接对 backup_offsets 和 backup_runs 中所有正整数 chat_id 做修正
使用 Telethon 标准公式：
  - channel (raw_id > 1e9): signed = -(1000000000000 + raw_id)  即 -100{raw_id}
  - chat/group: signed = -raw_id
  - user/其他小 ID 跳过（不是频道/群组，备份不会覆盖这类）
"""
import sqlite3, json

conn = sqlite3.connect('data/copilot.db')
conn.row_factory = sqlite3.Row

def to_signed(cid):
    if cid <= 0:
        return cid  # already signed
    if cid > 1_000_000_000:
        return -(1_000_000_000_000 + cid)  # channel -> -100{cid}
    else:
        return -cid  # legacy chat

# === 1. Fix backup_offsets ===
print('=== Fixing backup_offsets ===')
rows = conn.execute('SELECT chat_id FROM backup_offsets').fetchall()
updated = 0
for (cid,) in rows:
    if cid > 0:
        new_id = to_signed(cid)
        conn.execute('UPDATE backup_offsets SET chat_id = ? WHERE chat_id = ?', (new_id, cid))
        print(f'  {cid} -> {new_id}')
        updated += 1
print(f'  Updated {updated} rows')

# === 2. Fix backup_runs channels_detail ===
print('=== Fixing backup_runs channels_detail ===')
runs_updated = 0
for r in conn.execute('SELECT run_id, channels_detail FROM backup_runs WHERE channels_detail IS NOT NULL').fetchall():
    try:
        details = json.loads(r['channels_detail'])
        changed = False
        for ch in details:
            old_id = ch.get('id')
            if isinstance(old_id, int) and old_id > 0:
                ch['id'] = to_signed(old_id)
                changed = True
        if changed:
            conn.execute('UPDATE backup_runs SET channels_detail = ? WHERE run_id = ?',
                         (json.dumps(details, ensure_ascii=False), r['run_id']))
            runs_updated += 1
    except: pass
print(f'  Updated {runs_updated} backup runs')

conn.commit()
conn.close()
print('Done!')
