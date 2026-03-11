
import os
import asyncio
import sys
import traceback
from telethon import TelegramClient
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))
from backup_mode.backup import backup_channel

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    session_name = 'data/sessions/copilot_user_temp'
    
    target_id = -1001395222731 # 紀錄庫-紀錄用頻道
    
    async with TelegramClient(session_name, api_id, api_hash) as client:
        print(f"🚀 Testing backup for: {target_id}")
        try:
            # Replicating call from backup.py main()
            res = await backup_channel(
                client, 
                target_id, 
                is_test=False, 
                run_label="#B3", 
                folder_name="极品捆绑"
            )
            print(f"\n✅ Result: {res}")
            if res is None:
                print("⚠️ Result is None (indicates exception caught in backup_channel)")
            elif isinstance(res, dict) and res.get('skipped'):
                print(f"⚠️ Result is skipped: {res.get('reason')}")
        except Exception as e:
            print(f"❌ CRASHED: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
