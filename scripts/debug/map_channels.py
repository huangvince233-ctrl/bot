
import asyncio
from telethon import TelegramClient, functions
from telethon import utils
import os
from dotenv import load_dotenv

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    
    client = TelegramClient('data/sessions/copilot_bot', api_id, api_hash)
    await client.connect()
    
    print("--- Channel Mapping ---")
    async for d in client.iter_dialogs():
        print(f"Title: {d.title}, ID: {d.id}, Type: {type(d.entity).__name__}")
        if '测试2群' in d.title:
            print(f"  >>> FOUND: {d.title} -> {d.id}")
            
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
