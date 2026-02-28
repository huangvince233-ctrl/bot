import sqlite3, json, os

conn = sqlite3.connect('data/copilot.db')
conn.row_factory = sqlite3.Row

print('=== 数据库中所有含"绳赋"的记录 ===')
for r in conn.execute("SELECT chat_id, last_msg_id, updated_at FROM backup_offsets"):
    pass  # need to join with name somehow

# From backup_runs channels_detail, look for 绳赋
print('=== backup_runs 中含绳赋的频道 ===')
for r in conn.execute('SELECT run_id, channels_detail FROM backup_runs WHERE channels_detail IS NOT NULL').fetchall():
    try:
        details = json.loads(r['channels_detail'])
        for ch in details:
            if '绳赋' in str(ch.get('name', '')):
                print(f"  RunID {r['run_id']}: id={ch.get('id')}, name={ch.get('name')}")
    except: pass

conn.close()

print('\n=== metadata 中含绳赋的 JSON ===')
for root, dirs, files in os.walk('data/metadata'):
    for f in files:
        if '绳赋' in f and f.endswith('.json'):
            path = os.path.join(root, f)
            try:
                d = json.load(open(path, encoding='utf-8'))
                print(f"  {path}")
                print(f"  id={d.get('id')}, folder={d.get('folder')}, name={d.get('canonical_name')}")
            except: pass
