
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
    # Use a different session name to avoid lock
    client = TelegramClient('data/sessions/debug_session', api_id, api_hash)
    await client.connect()
    
    print(f"Checking last 200 messages in {target_id}...")
    async for m in client.iter_messages(target_id, limit=200):
        t = (m.text or "").replace('\n', ' ')[:60]
        if 'TEST-1' in t.upper() or 'TEST-2' in t.upper():
            print(f"  >>> FOUND REMNANT: [{m.id}] {t}")
        else:
            # Also check if it's a media group message (maybe no text?)
            pass
            
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
