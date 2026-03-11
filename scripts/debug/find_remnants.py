
import os
import sys
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
target_id = int(os.getenv('TARGET_GROUP_ID'))

async def main():
    client = TelegramClient('data/sessions/copilot_bot', api_id, api_hash)
    await client.connect()
    
    print(f"Iterating through history of {target_id}...")
    async for m in client.iter_messages(target_id, limit=300):
        t = (m.text or "").replace('\n', ' ')[:50]
        print(f"[{m.id}] {t}")
        if 'TEST-1' in t:
            print(f"  >>> HIT: TEST-1 in {m.id}")
        if 'TEST-2' in t:
            print(f"  >>> HIT: TEST-2 in {m.id}")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
