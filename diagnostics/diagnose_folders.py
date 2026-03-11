
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
    
    results = {}
    
    async with TelegramClient(session_name, api_id, api_hash) as client:
        filters = await client(functions.messages.GetDialogFiltersRequest())
        
        for f in filters.filters:
            if not hasattr(f, 'title'): continue
            title = f.title.text if hasattr(f.title, 'text') else str(f.title)
            
            peers_list = []
            all_peers = list(getattr(f, 'include_peers', [])) + list(getattr(f, 'pinned_peers', []))
            
            for p in all_peers:
                try:
                    pid = telethon_utils.get_peer_id(p)
                    # Resolve name if possible
                    try:
                        ent = await client.get_entity(pid)
                        name = getattr(ent, 'title', 'Unknown')
                    except:
                        name = "Could not resolve"
                    peers_list.append({"id": pid, "name": name})
                except Exception as e:
                    peers_list.append({"error": str(e)})
            
            results[title] = peers_list

    with open('folders_peers.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("✅ Results saved to folders_peers.json")

if __name__ == "__main__":
    asyncio.run(main())
