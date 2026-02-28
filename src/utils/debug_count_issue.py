import os
import asyncio
import re
from telethon import TelegramClient, types
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'

def count_urls(message):
    """统计消息中携带的链接数（含实体链接和纯文本链接）- 使用与 sync.py 相同的逻辑"""
    urls = set()
    # 1. 统计实体链接
    if message.entities:
        print(f"Entities found: {len(message.entities)}")
        for i, e in enumerate(message.entities):
            if isinstance(e, types.MessageEntityUrl):
                offset = e.offset
                length = e.length
                url_text = message.text[offset:offset+length]
                urls.add(url_text.strip())
                print(f"  Entity {i} (Url): '{url_text.strip()}'")
            elif isinstance(e, types.MessageEntityTextUrl):
                urls.add(e.url.strip())
                print(f"  Entity {i} (TextUrl): '{e.url.strip()}'")
    
    # 2. 补漏：正则扫描
    text = message.text or ""
    plain_urls = re.findall(r'https?://[^\s，。；、]+', text)
    print(f"Regex found: {len(plain_urls)}")
    for u in plain_urls:
        urls.add(u.strip())
        print(f"  Regex match: '{u.strip()}'")
    
    print(f"\nRaw set ({len(urls)} items): {urls}")
        
    # 3. 归一化：以 :// 为锚点剥离协议头
    normalized_urls = set()
    for u in urls:
        raw = u.lower().strip()
        if '://' in raw:
            raw = raw.split('://', 1)[1]
        if raw.startswith('www.'):
            raw = raw[4:]
        raw = raw.rstrip('/')
        if raw:
            normalized_urls.add(raw)
            print(f"  Normalized: '{u}' -> '{raw}'")
        
    print(f"\nFinal Count: {len(normalized_urls)}")
    print(f"Unique URLs: {normalized_urls}")
    return len(normalized_urls)

async def check_msg(chat_id, msg_id):
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        msg = await client.get_messages(chat_id, ids=msg_id)
        if not msg:
            print("Message not found")
            return
        
        print(f"--- Message {msg_id} ---")
        print(f"Text: {repr(msg.text)}")
        print()
        count_urls(msg)

if __name__ == "__main__":
    asyncio.run(check_msg('test', 14817))
