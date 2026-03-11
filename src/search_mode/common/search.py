import sys
import os
import argparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from db import Database

def main():
    parser = argparse.ArgumentParser(description="Search local Telegram metadata archive.")
    parser.add_argument('query', help='Keyword, username, or channel name to search for')
    args = parser.parse_args()
    
    db = Database('data/copilot.db')
    results = db.search_global(args.query)
    
    if not results:
        print(f"❌ 没有找到与 '{args.query}' 相关的记录。")
        return
        
    print(f"🔍 查找到 {len(results)} 条与 '{args.query}' 相关的近期记录：\n" + "="*50)
    for i, r in enumerate(results, 1):
        chat_name, msg_type, sender_name, original_time, text_content, forwarded_msg_id = r
        text = text_content[:100].replace('\n', ' ') + ('...' if len(text_content) > 100 else '') if text_content else ''
        fwd_info = f" | 🔗 群组转发 ID: {forwarded_msg_id}" if forwarded_msg_id else " | 💾 仅基础归档"
        print(f"[{i}] {original_time} | 频道: {chat_name} | 发送者: {sender_name}")
        print(f"    类型: {msg_type}{fwd_info}")
        if text:
            print(f"    内容: {text}")
        print("-" * 50)

if __name__ == "__main__":
    main()
