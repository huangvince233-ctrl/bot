import os
import asyncio
import sys
from telethon import TelegramClient, functions
from dotenv import load_dotenv

# 加载环境变量
_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_base_dir, '.env'))

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = os.path.join(_base_dir, 'data', 'sessions', 'copilot_user')

async def list_folders():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("❌ User Client 未授权")
        return

    print("🔍 正在获取所有文件夹...")
    filters_resp = await client(functions.messages.GetDialogFiltersRequest())
    all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
    
    print("\n--- Telegram 文件夹列表 ---")
    for f in all_filters:
        title = getattr(f, 'title', None)
        t_str = ""
        if title:
            if hasattr(title, 'text'):
                t_str = title.text
            else:
                t_str = str(title)
        
        if t_str:
            print(f"Name: [{t_str}] | Type: {type(f).__name__}")
        else:
            print(f"Non-titled Filter | Type: {type(f).__name__}")
            
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(list_folders())
