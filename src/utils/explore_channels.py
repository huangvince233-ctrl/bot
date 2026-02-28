import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()

# Get credentials from .env
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')

async def main():
    if not API_ID or not API_HASH:
        print("❌ 错误：请先在 .env 文件中填写 API_ID 和 API_HASH。")
        return

    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        print("🔍 正在全力扫描你加入的所有频道和群组（包括存档频道）...\n")
        
        channels = []
        # 使用 limit=None 确保抓取所有对话
        async for dialog in client.iter_dialogs(limit=None):
            # 只要是频道或者超级群组都记录
            if dialog.is_channel:
                channels.append({
                    'name': dialog.name,
                    'id': dialog.id,
                    'username': dialog.entity.username if hasattr(dialog.entity, 'username') and dialog.entity.username else '私有频道'
                })
        
        print(f"{'名称':<30} | {'ID':<15} | {'用户名'}")
        print("-" * 75)
        for c in channels:
            # 修复中文长度导致的对齐问题
            display_name = c['name'][:30]
            print(f"{display_name:<30} | {c['id']:<15} | {c['username']}")
            
        print("\n💡 提示：")
        print("1. 复制你想存档的频道 ID（带负号的数字）。")
        print("2. 将它们填入 .env 文件的 SOURCE_CHANNELS 中，用逗号隔开。")
        print("3. 如果你想分析里面的内容是否包含特定主题（如 BDSM），可以使用该 ID 进一步扫描。")

if __name__ == "__main__":
    asyncio.run(main())
