import sqlite3
import re
import json

db_path = 'data/copilot.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()

c.execute("SELECT msg_id, chat_name, text_content, creator, actor, keywords FROM global_messages WHERE msg_id = 6000 AND chat_name = '窒物者'")
row = c.fetchone()
if row:
    print("DB Row:")
    print("msg_id:", row[0])
    print("chat_name:", row[1])
    print("text_content:", repr(row[2]))
    print("creator:", repr(row[3]))
    print("actor:", repr(row[4]))
    print("keywords:", repr(row[5]))
    text = row[2]
else:
    print("Msg 6000 not found for 窒物者")
    text = ""
conn.close()

# Load entities
with open('data/entities/tgporncopilot/currententities/entities.json', 'r', encoding='utf-8') as f:
    entities = json.load(f)

print("\n--- Entities check ---")
for creator in entities.get('creators', []):
    if creator['name'] == '窒物者':
        print("Found 窒物者 in creators:")
        print(creator)
        main_name = creator['name']
        aliases = creator.get('aliases', [])
        all_names = [main_name] + aliases
        escaped_names = [re.escape(n) for n in all_names if n]
        pattern_str = '|'.join(escaped_names)
        print("Regex pattern:", pattern_str)
        regex = re.compile(pattern_str, re.IGNORECASE)
        match = regex.search(text)
        print("Match result on text:", match)

