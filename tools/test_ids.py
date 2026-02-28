import os
import asyncio
from telethon import TelegramClient, utils
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def main():
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        # Get a channel from dialogs
        async for dialog in client.iter_dialogs(limit=10):
            if type(dialog.entity).__name__ == 'Channel':
                ent = dialog.entity
                print(f"Channel: {dialog.name}")
                print(f"  dialog.id: {dialog.id}")
                print(f"  ent.id: {ent.id}")
                print(f"  utils.get_peer_id(ent): {utils.get_peer_id(ent)}")
                print(f"  utils.get_peer_id(dialog.id): {utils.get_peer_id(dialog.id)}")
                break

if __name__ == "__main__":
    asyncio.run(main())
