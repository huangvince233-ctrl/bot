
import os
import sys
import asyncio
from telethon import TelegramClient, events, Button
from dotenv import load_dotenv

load_dotenv()
api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
bot_token = os.getenv('BOT_TOKEN')
target_id = int(os.getenv('TARGET_GROUP_ID'))

async def main():
    # 使用 Bot Token 登录，避免与 User Client 冲突
    client = TelegramClient('data/sessions/cleanup_temp', api_id, api_hash)
    await client.start(bot_token=bot_token)
    
    print(f"🔍 正在目标群 {target_id} 中搜索残留的同步标签...")
    
    labels = ['TEST-1', 'TEST-2']
    all_found = []
    
    for lbl in labels:
        print(f"  • 搜索: {lbl}")
        # Bot 只能在群组中搜索自己发送的消息或全局搜索（取决于权限）
        async for m in client.iter_messages(target_id, search=lbl, limit=500):
            if m and getattr(m, 'id', None):
                all_found.append(m.id)
                print(f"    [FOUND] ID:{m.id} - {m.text[:30] if m.text else 'Media'}")

    if not all_found:
        print("❌ 未发现任何匹配的残留消息。")
    else:
        unique_ids = sorted(list(set(all_found)))
        print(f"\n🚀 准备物理删除 {len(unique_ids)} 条残留消息...")
        
        # 分批删除
        for i in range(0, len(unique_ids), 100):
            chunk = unique_ids[i:i + 100]
            try:
                await client.delete_messages(target_id, chunk, revoke=True)
                print(f"  ✅ 已清除批次: {chunk[0]} ~ {chunk[-1]}")
            except Exception as e:
                print(f"  ⚠️ 删除批次失败: {e}")
            await asyncio.sleep(1)
            
        print("\n🏁 物理清理完成。")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
