
import os
import asyncio
import json
from telethon import TelegramClient, functions, types
from dotenv import load_dotenv

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    session_name = 'data/sessions/copilot_user_temp'
    
    target_ids = [
        -1001395222731, # 紀錄庫-紀錄用頻道
        -1003077564843, # 猎奇 I SM I 重口
        -1002974973326  # SM/捆绑/绳艺/调教 字母圈资源汇
    ]
    
    async with TelegramClient(session_name, api_id, api_hash) as client:
        # Get folder 0 (Main)
        dialogs_main = await client.get_dialogs(folder=0)
        main_ids = {d.id for d in dialogs_main}
        
        # Get folder 1 (Archived)
        dialogs_archived = await client.get_dialogs(folder=1)
        archived_ids = {d.id for d in dialogs_archived}
        
        print(f"📊 Dialog Stats:")
        print(f"  - Main List: {len(main_ids)}")
        print(f"  - Archived: {len(archived_ids)}")
        
        for tid in target_ids:
            in_main = tid in main_ids
            in_archived = tid in archived_ids
            print(f"\n🔍 Target: {tid}")
            print(f"  - In Main: {in_main}")
            print(f"  - In Archived: {in_archived}")
            if not in_main and not in_archived:
                print(f"  ⚠️ Warning: Not found in either list!")

if __name__ == "__main__":
    asyncio.run(main())
