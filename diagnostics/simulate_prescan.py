
import os
import asyncio
import json
from telethon import TelegramClient, functions, types, utils as telethon_utils
from dotenv import load_dotenv

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    session_name = 'data/sessions/copilot_user_temp'
    
    MANAGED_FOLDERS = os.getenv('MANAGED_FOLDERS', '').split(',')
    MANAGED_FOLDERS = [f.strip() for f in MANAGED_FOLDERS if f.strip()]
    
    report = {
        "managed_folders_config": MANAGED_FOLDERS,
        "folders": []
    }
    
    async with TelegramClient(session_name, api_id, api_hash) as client:
        dialogs = await client.get_dialogs()
        active_dialog_ids = {d.id for d in dialogs}
        
        filters = await client(functions.messages.GetDialogFiltersRequest())
        seen = set()
        
        for f in getattr(filters, 'filters', []):
            if not hasattr(f, 'include_peers'): continue
            title = getattr(f, 'title', None)
            f_name = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            
            is_managed = False
            if "*" in MANAGED_FOLDERS or "ALL" in [m.upper() for m in MANAGED_FOLDERS]:
                is_managed = True
            elif f_name in MANAGED_FOLDERS:
                is_managed = True
            
            folder_info = {
                "name": f_name,
                "is_managed": is_managed,
                "peers": []
            }
            
            all_peers = list(getattr(f, 'include_peers', [])) + list(getattr(f, 'pinned_peers', []))
            for peer in all_peers:
                try:
                    signed_id = telethon_utils.get_peer_id(peer)
                except:
                    pid = getattr(peer, 'channel_id', getattr(peer, 'chat_id', getattr(peer, 'user_id', None)))
                    signed_id = pid
                
                in_active = signed_id in active_dialog_ids
                in_seen = signed_id in seen
                added = False
                
                if is_managed and signed_id and not in_seen and in_active:
                    seen.add(signed_id)
                    added = True
                
                folder_info["peers"].append({
                    "id": signed_id,
                    "active": in_active,
                    "seen_before": in_seen,
                    "added": added
                })
            
            report["folders"].append(folder_info)

    with open('prescan_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("✅ Prescan report saved to prescan_report.json")

if __name__ == "__main__":
    asyncio.run(main())
