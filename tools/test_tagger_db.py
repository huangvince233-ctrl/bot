import sqlite3
import json

db_path = 'data/copilot.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT chat_id, msg_id, media_group_id, text_content, creator FROM global_messages WHERE chat_name = '窒物者' AND text_content LIKE '%88%'")
rows = c.fetchall()

result = []
for row in rows:
    result.append({
        'chat_id': row[0],
        'msg_id': row[1],
        'mg_id': row[2],
        'text': row[3][:30],
        'creator': row[4]
    })

with open('test_output_search.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

conn.close()
