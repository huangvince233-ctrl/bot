import asyncio
import os
import sys
from telethon import TelegramClient

# 确保能导入 src 下的代码
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from db import Database
from utils.config import CONFIG

async def cleanup():
    db = Database()
    
    # 1. 获取所有相关的目标群组 (包括双 Bot 配置的所有活跃/非活跃群组)
    # 我们从 target_groups 表中提取 unique 的 chat_id
    rows = db.cursor.execute('SELECT DISTINCT chat_id, title FROM target_groups').fetchall()
    targets = {r[0]: r[1] for r in rows}
    
    # 也加入 .env 中的默认配置，兼容初次运行且未入库的情况
    if CONFIG.get('target_group_id'):
        targets[CONFIG['target_group_id']] = targets.get(CONFIG['target_group_id'], "Default Target (Copilot)")
    
    if not targets:
        print("❌ 未在数据库或配置中找到任何目标群组。")
        return

    print(f"🧹 [Multi-Group Orphan Cleanup] Found {len(targets)} potential target groups.")
    
    session_file = 'data/sessions/copilot_user' 
    
    # 2. 连接 Telegram
    async with TelegramClient(session_file, CONFIG['api_id'], CONFIG['api_hash']) as client:
        me = await client.get_me()
        print(f"👤 已登录账户: {me.first_name} (ID: {me.id})")
        
        for chat_id, title in targets.items():
            print(f"\n📡 正在处理群组: {title} ({chat_id})")
            
            # 为该群组寻找数据库中目前记录的最先进消息 ID
            # 我们直接查找当前数据库中记录的最大已转发消息 ID
            res = db.cursor.execute('''
                SELECT MAX(forwarded_msg_id) FROM messages WHERE forwarded_chat_id = ?
            ''', (chat_id,)).fetchone()
            
            max_safe_id = res[0] if res and res[0] else 0
            print(f"  🛡️  该群当前数据库安全水位线 (MAX ID): {max_safe_id}")

            try:
                target_entity = await client.get_entity(chat_id)
            except Exception as e:
                print(f"  ⚠️ 无法连接到群组 {chat_id}: {e}")
                continue

            orphans = []
            print(f"  🔍 正在扫描后续残留消息...")
            async for message in client.iter_messages(target_entity, min_id=max_safe_id):
                # 仅撤回自己发送的消息
                if message.sender_id == me.id:
                    orphans.append(message.id)
            
            if not orphans:
                print("  ✅ 该群组未发现残留消息。")
                continue
                
            print(f"  🚨 发现 {len(orphans)} 条残留消息，准备开始撤销...")
            chunk_size = 100
            for i in range(0, len(orphans), chunk_size):
                chunk = orphans[i:i + chunk_size]
                try:
                    await client.delete_messages(target_entity, chunk, revoke=True)
                    print(f"    ➡️ 已物理撤回批次: {chunk[0]} ~ {chunk[-1]}")
                except Exception as e:
                    print(f"    ⚠️ 撤回批次失败: {e}")
                await asyncio.sleep(0.5)
            
    print("\n✨ 扫尾工作全部完成！您的所有私密群组现在应该都已经回到回滚状态了。")

if __name__ == "__main__":
    asyncio.run(cleanup())
