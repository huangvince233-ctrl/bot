import sys
import os
import json
from pathlib import Path

# 添加 src 到路径以加载 Database
sys.path.append(str(Path('src').absolute()))
from db import Database

db_path = 'data/copilot.db'
db = Database(db_path)

results = {}

# 1. 检查各表计数
tables = ['sync_runs', 'backup_runs', 'global_messages', 'entities', 'target_groups', 'messages']
for t in tables:
    try:
        db.cursor.execute(f"SELECT COUNT(*) FROM {t}")
        results[t] = db.cursor.fetchone()[0]
    except Exception as e:
        results[t] = f"Error: {str(e)}"

# 2. 检查激活的目标群组
try:
    results['active_tg'] = db.get_active_target_group('tgporncopilot')
except Exception as e:
    results['active_tg_err'] = str(e)

# 3. 检查最近的同步记录
try:
    db.cursor.execute("SELECT run_id, formal_number, is_test, bot_name FROM sync_runs ORDER BY run_id DESC LIMIT 5")
    results['last_sync_details'] = db.cursor.fetchall()
except:
    pass

print(json.dumps(results, indent=2))
db.close()
