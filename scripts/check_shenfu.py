import sqlite3, json

conn = sqlite3.connect('data/copilot.db')
conn.row_factory = sqlite3.Row

# Search backup_runs channels_detail for "绳赋"
print('=== backup_runs 中所有含绳赋的频道 ===')
for r in conn.execute('SELECT run_id, channels_detail FROM backup_runs WHERE channels_detail IS NOT NULL').fetchall():
    try:
        details = json.loads(r['channels_detail'])
        for ch in details:
            if '绳赋' in str(ch.get('name', '')):
                print(f"  RunID {r['run_id']}: id={ch.get('id')} name={ch.get('name')}")
    except: pass

# All backup_offsets
print('\n=== backup_offsets 中含绳赋的行 ===')
# We don't store names in backup_offsets, just show all IDs
for r in conn.execute('SELECT chat_id, last_msg_id, updated_at FROM backup_offsets').fetchall():
    print(f"  chat_id={r['chat_id']} last_msg_id={r['last_msg_id']} updated={r['updated_at']}")

conn.close()
