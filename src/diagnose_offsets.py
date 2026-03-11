import os
import sys
import asyncio
import sqlite3
from telethon import TelegramClient, utils
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db import Database

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'

db = Database('data/copilot.db')

async def diagnose():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        print("=== 数据库断点记录 (前20条) ===")
        rows = db.cursor.execute('SELECT chat_id, last_msg_id, is_test FROM backup_offsets ORDER BY updated_at DESC LIMIT 20').fetchall()
        for r in rows:
            print(f"ChatID: {r[0]}, LastMsgID: {r[1]}, IsTest: {r[2]}")
            
        print("\n=== 采样频道核对 ===")
        # 尝试获取几个常见频道的 entity ID
        targets = [-1002781849423, -1002975736992] # 下载链接, 极品捆绑
        for tid in targets:
            try:
                ent = await client.get_entity(tid)
                signed_id = utils.get_peer_id(ent)
                db_offset = db.get_backup_offset(signed_id, is_test=0)
                print(f"Target: {tid}")
                print(f"  Entity Title: {getattr(ent, 'title', 'N/A')}")
                print(f"  SignedID (get_peer_id): {signed_id}")
                print(f"  DB Offset Found: {db_offset}")
            except Exception as e:
                print(f"  Error for {tid}: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose())
