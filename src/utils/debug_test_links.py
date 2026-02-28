import re
from db import Database

class MockEntity:
    def __init__(self, offset, length, url=None):
        self.offset = offset
        self.length = length
        self.url = url

class MockMessage:
    def __init__(self, text, entities=None):
        self.text = text
        self.entities = entities

def count_urls_local(message):
    urls = set()
    # 1. 统计实体链接
    if message.entities:
        for e in message.entities:
            # 简化模拟: MessageEntityUrl / MessageEntityTextUrl
            if e.url: # TextUrl
                urls.add(e.url.strip())
            else: # Plain Url
                url_text = message.text[e.offset:e.offset+e.length]
                urls.add(url_text.strip())
    
    # 2. 正则捕获
    text = message.text or ""
    plain_urls = re.findall(r'https?://[^\s，。；、]+', text)
    for u in plain_urls:
        urls.add(u.strip())
    return len(urls)

# 测试数据
msg_text = "Check this out: https://google.com and https://github.com/vince. Also http://bad.news/app"
# 模拟 TG 识别了前两个
entities = [
    MockEntity(16, 18), # google.com
    MockEntity(39, 26)  # github.com/vince
]
msg = MockMessage(msg_text, entities)

url_count = count_urls_local(msg)
print(f"Detected URLs: {url_count}")

db = Database('data/copilot.db')
# 测试分配
ids = db.assign_resource_ids('test_chat', 999999, 'link', is_test=True, url_count=url_count, is_new_msg=True)
print(f"Assigned IDs: {ids}")

link_ids = ids['link']
if len(link_ids) > 1:
    print(f"Header Display: 🔗 链接号: #{min(link_ids)}-#{max(link_ids)}")
else:
    print(f"Header Display: 🔗 链接号: #{link_ids[0]}")
