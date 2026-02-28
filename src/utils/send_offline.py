"""
send_offline.py - 手动发送机器人下线通知
用途：在 Agent 执行 taskkill 前手动调用，确保 Telegram 私聊中有下线通知。
用法：python src/utils/send_offline.py [--run-id XXXXXX]
"""
import os
import sys
import asyncio
import argparse
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID')

if ADMIN_USER_ID:
    try:
        ADMIN_USER_ID = int(ADMIN_USER_ID)
    except:
        print("⚠️ ADMIN_USER_ID 格式错误")
        ADMIN_USER_ID = None

async def send_offline_notification(run_id: str):
    from telethon import TelegramClient
    
    if not ADMIN_USER_ID:
        print("⚠️ 未配置 ADMIN_USER_ID，无法发送通知。")
        return
    
    bot = TelegramClient('data/copilot_bot', API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    
    msg = (
        f"🛑 **机器人正在下线**\n"
        f"━━━━━━━━━━━━━━\n"
        f"🆔 运行标识: `{run_id}`\n"
        f"⚠️ 由 Agent 主动发起关机，该实例已停止服务。"
    )
    
    await bot.send_message(ADMIN_USER_ID, msg)
    print(f"✅ 下线通知已发送 (RunID: {run_id})")
    await bot.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='发送机器人下线通知')
    parser.add_argument('--run-id', default=None, help='当前 RunID')
    args = parser.parse_args()
    
    run_id = args.run_id
    if not run_id:
        # 尝试自动从文件读取
        try:
            run_id_path = os.path.join('data', 'run_id.txt')
            if os.path.exists(run_id_path):
                with open(run_id_path, 'r', encoding='utf-8') as f:
                    run_id = f.read().strip()
        except:
            pass
            
    if not run_id:
        run_id = 'UNKNOWN'
        
    asyncio.run(send_offline_notification(run_id))
