import sqlite3
import json

conn = sqlite3.connect('f:/funny_project/tgporncopilot/data/copilot.db')
cursor = conn.cursor()
query = "SELECT * FROM channel_names WHERE canonical_name LIKE '%重口%' OR latest_name LIKE '%重口%'"
rows = cursor.execute(query).fetchall()
print(json.dumps(rows, ensure_ascii=False, indent=2))
conn.close()
