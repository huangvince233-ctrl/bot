import os
import sys
import asyncio
from telethon import TelegramClient, functions
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.explore_channels import API_ID, API_HASH
from backup_mode.backup import backup_channel

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

async def backup_folder(folder_name):
    async with TelegramClient('data/copilot_user', API_ID, API_HASH) as client:
        filters = await client(functions.messages.GetDialogFiltersRequest())
        if not isinstance(filters, list):
            filters = getattr(filters, 'filters', [filters])
        
        target_ids = []
        for f in filters:
            title = getattr(f, 'title', None)
            if not title or not hasattr(f, 'include_peers'): continue
            t_str = title.text if hasattr(title, 'text') else str(title)
            
            if t_str == folder_name:
                for peer in f.include_peers:
                    if hasattr(peer, 'channel_id'):
                        target_ids.append(f'-100{peer.channel_id}')
                    elif hasattr(peer, 'chat_id'):
                        target_ids.append(f'-{peer.chat_id}')
                    elif hasattr(peer, 'user_id'):
                        target_ids.append(f'{peer.user_id}')
                break
                
    if not target_ids:
        print(f"❌ Folder '{folder_name}' not found or empty.")
        return

    print(f"📦 Found {len(target_ids)} channels in folder '{folder_name}'. Starting backup...")
    for cid in target_ids:
        try:
            await backup_channel(str(cid))
        except Exception as e:
            print(f"⚠️ Failed to backup {cid}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/backup_mode/backup_folder.py <FolderName>")
        sys.exit(1)
    asyncio.run(backup_folder(sys.argv[1]))
