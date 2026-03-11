import sqlite3
import json
import os

DB_PATH = 'data/copilot.db'
ENTITIES_PATH = 'data/entities/tgporncopilot_entities.json'

def check_results():
    print("--- 1. 实体字典 (entities.json) 检查 ---")
    if os.path.exists(ENTITIES_PATH):
        try:
            with open(ENTITIES_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            creators = data.get('creators', [])
            actors = data.get('actors', [])
            keywords = data.get('keywords', [])
            noise = data.get('noise', [])
            
            print(f"Creators 数量: {len(creators)}")
            print(f"Actors 数量: {len(actors)}")
            print(f"Keywords 数量: {len(keywords)}")
            print(f"Noise (拉黑) 数量: {len(noise)}")
            
            # 打印最近加入的几个 (假设在列表末尾)
            def get_names(items):
                return [i['name'] if isinstance(i, dict) else i for i in items[-5:]]
                
            print(f"最近 Creators: {get_names(creators)}")
            print(f"最近 Actors: {get_names(actors)}")
            print(f"最近 Keywords: {get_names(keywords)}")
            print(f"最近 Noise: {noise[-10:]}")
        except Exception as e:
            print(f"读取 JSON 失败: {e}")
    else:
        print("未找到 entities.json")

    print("\n--- 2. 数据库打标 (global_messages) 检查 ---")
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # 统计已打标的消息
            cursor.execute("SELECT COUNT(*) FROM global_messages WHERE creator IS NOT NULL OR actor IS NOT NULL OR keywords IS NOT NULL")
            tagged_count = cursor.fetchone()[0]
            print(f"总计已打标消息数: {tagged_count}")
            
            # 查看最近打标的消息示例
            cursor.execute("""
                SELECT chat_name, creator, actor, keywords 
                FROM global_messages 
                WHERE creator IS NOT NULL OR actor IS NOT NULL OR keywords IS NOT NULL
                ORDER BY original_time DESC LIMIT 10
            """)
            rows = cursor.fetchall()
            for r in rows:
                print(f"频道: {r[0]} | Creator: {r[1]} | Actor: {r[2]} | Keywords: {r[3]}")
                
            conn.close()
        except Exception as e:
            print(f"查询数据库失败: {e}")
    else:
        print("未找到 copilot.db")

if __name__ == "__main__":
    check_results()
