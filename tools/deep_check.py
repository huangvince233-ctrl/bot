import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def check():
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        try:
            ent = await client.get_entity(-1003323249740)
            print(f"Name: {ent.title}")
            print(f"Type: {type(ent).__name__}")
            print(f"Restricted: {getattr(ent, 'restricted', 'N/A')}")
            if hasattr(ent, 'restriction_reason'):
                print(f"Restriction Reason: {ent.restriction_reason}")
            
            # Try to get messages
            msgs = await client.get_messages(ent, limit=1)
            print(f"Messages count: {len(msgs)}")
            if len(msgs) > 0:
                print(f"Latest msg ID: {msgs[0].id}")
            
        except Exception as e:
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {e}")

if __name__ == "__main__":
    asyncio.run(check())
