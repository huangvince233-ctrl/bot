import sqlite3, json
conn = sqlite3.connect('data/copilot.db')
conn.row_factory = sqlite3.Row

cid = -1002829404994
raw_cid = 2829404994

print('=== backup_offsets ===')
rows = conn.execute('SELECT * FROM backup_offsets WHERE chat_id IN (?, ?)', (cid, raw_cid)).fetchall()
for r in rows:
    print(dict(r))
if not rows:
    print('(empty)')

print('\n=== backup_runs channels_detail ===')
found = False
for r in conn.execute('SELECT run_id, channels_detail FROM backup_runs WHERE channels_detail IS NOT NULL').fetchall():
    try:
        details = json.loads(r['channels_detail'])
        for ch in details:
            if ch.get('id') in (cid, raw_cid):
                print(f"RunID {r['run_id']}: {ch}")
                found = True
    except: pass
if not found:
    print('(empty)')

conn.close()
