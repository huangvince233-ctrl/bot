import os
import asyncio
from telethon import TelegramClient, utils
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def main():
    target_id = 1003077564843 # The -100 prefix version
    target_raw = 3077564843
    
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        print(f"🔍 Searching for ID: {target_id} / {target_raw} in dialogs...")
        found = False
        async for dialog in client.iter_dialogs():
            if dialog.id == target_id or dialog.id == -target_id or abs(dialog.id) == target_raw:
                print(f"✅ Found in ACTIVE dialogs: {dialog.name} (ID: {dialog.id})")
                found = True
        
        async for dialog in client.iter_dialogs(archived=True):
            if dialog.id == target_id or dialog.id == -target_id or abs(dialog.id) == target_raw:
                print(f"✅ Found in ARCHIVED dialogs: {dialog.name} (ID: {dialog.id})")
                found = True
        
        if not found:
            print("❌ Not found in any dialogs.")
            try:
                ent = await client.get_entity(target_raw)
                print(f"ℹ️ But get_entity worked: {ent.title} (ID: {ent.id})")
                print(f"   Restricted: {getattr(ent, 'restricted', False)}")
            except Exception as e:
                print(f"ℹ️ And get_entity failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
