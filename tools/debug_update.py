import os
import asyncio
from telethon import TelegramClient, utils
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def main():
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        my_id = (await client.get_me()).id
        all_dialogs = {}
        async for dialog in client.iter_dialogs():
            ent = dialog.entity
            dtype = type(ent).__name__
            
            if "猎奇" in dialog.name:
                print(f"DEBUG: Found {dialog.name} (ID: {dialog.id}, Type: {dtype})")
                if dtype not in ('Channel', 'Chat', 'User'):
                    print(f"  -> REJECTED by type: {dtype}")
                    continue
                
                # Check get_dialog_info logic
                is_channel = (dtype == 'Channel' and getattr(ent, 'broadcast', False))
                is_group = (dtype == 'Channel' and getattr(ent, 'megagroup', False)) or dtype == 'Chat'
                print(f"  -> is_channel: {is_channel}, is_group: {is_group}")
                
        print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
