import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'

async def test_telethon():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        try:
            # 找一个已知频道，比如 -1002975736992 (极品捆绑)
            entity = await client.get_entity(-1002975736992)
            
            # 1. 全量
            res_full = await client.get_messages(entity, limit=0)
            print(f"Full Total: {res_full.total}")
            
            # 2. 增量 (找一个较大的 min_id，比如最新的 ID 减去 10)
            latest_msg = await client.get_messages(entity, limit=1)
            if latest_msg:
                mid = latest_msg[0].id
                test_min_id = mid - 10
                res_inc = await client.get_messages(entity, limit=0, min_id=test_min_id)
                print(f"Inc (min_id={test_min_id}) Total: {res_inc.total}")
                
                if res_inc.total == res_full.total:
                    print("⚠️ ALERT: Telethon .total property DOES NOT respect min_id filter!")
                else:
                    print("✅ Telethon .total property respects min_id filter.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_telethon())
