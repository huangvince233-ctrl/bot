import asyncio
import os
from telethon import TelegramClient, functions, types
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'

async def test_count():
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        # 找一个有消息的频道
        dialogs = await client.get_dialogs(limit=10)
        channels = [d for d in dialogs if d.is_channel]
        if not channels:
            print("No channels found")
            return
        
        ent = channels[0].entity
        print(f"Testing channel: {channels[0].name}")
        
        # 1. 直接获取总数
        res_full = await client(functions.messages.GetHistoryRequest(
            peer=ent, offset_id=0, offset_date=None, 
            add_offset=0, limit=0, max_id=0, min_id=0, hash=0
        ))
        print(f"Full Capacity Count: {res_full.count}")
        
        # 2. 获取最近 10 条消息作为偏移参考
        msgs = await client.get_messages(ent, limit=10)
        if len(msgs) < 10:
            print("Not enough messages for test")
            return
        
        mid_id = msgs[5].id
        print(f"Setting min_id to {mid_id} (last 5 messages)")
        
        # 3. 尝试带 min_id 获取计数
        res_filtered = await client(functions.messages.GetHistoryRequest(
            peer=ent, offset_id=0, offset_date=None, 
            add_offset=0, limit=0, max_id=0, min_id=mid_id, hash=0
        ))
        print(f"Filtered Count (Expect ~5): {res_filtered.count}")
        
        # 4. 使用 get_messages(limit=0) 看看是否有差异
        msgs_filtered = await client.get_messages(ent, limit=0, min_id=mid_id)
        print(f"get_messages(limit=0, min_id={mid_id}) count: {msgs_filtered.total}")

if __name__ == "__main__":
    asyncio.run(test_count())
