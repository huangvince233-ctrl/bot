
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
    
    print(f"Searching for TEST-1 in {target_id}...")
    found = []
    async for m in client.iter_messages(target_id, search='TEST-1', limit=100):
        print(f"Found: {m.id} - {m.text[:30]}")
        found.append(m.id)
    
    print(f"Searching for TEST-2 in {target_id}...")
    async for m in client.iter_messages(target_id, search='TEST-2', limit=100):
        print(f"Found: {m.id} - {m.text[:30]}")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
