import os
import asyncio
from telethon import TelegramClient, functions
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'

async def test_telethon():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        try:
            target = -1002975736992 # 极品捆绑
            entity = await client.get_entity(target)
            
            latest = await client.get_messages(entity, limit=1)
            if not latest: return
            mid = latest[0].id
            test_min_id = mid - 50
            
            print(f"Latest ID: {mid}, Test Min ID: {test_min_id}")
            
            # Method 1: get_messages(min_id=X, limit=0)
            res1 = await client.get_messages(entity, limit=0, min_id=test_min_id)
            print(f"M1 (min_id): {res1.total}")
            
            # Method 2: get_messages(offset_id=X, limit=0)
            res2 = await client.get_messages(entity, limit=0, offset_id=test_min_id)
            print(f"M2 (offset_id): {res2.total}")

            # Method 3: get_messages(offset_id=X, add_offset=-1000, limit=0)
            # This is a trick sometimes used to get relative counts
            
            # Method 4: iterate a bit? No, too slow.
            
            # Method 5: GetHistoryRequest directly
            res5 = await client(functions.messages.GetHistoryRequest(
                peer=entity, offset_id=0, offset_date=None,
                add_offset=0, limit=0, max_id=0, min_id=test_min_id, hash=0
            ))
            print(f"M5 (GetHistory+min_id): {res5.count}")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_telethon())
