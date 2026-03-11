
import os
import asyncio
import json
from telethon import TelegramClient, functions, types
from dotenv import load_dotenv

async def check_channel(client, channel_id):
    try:
        entity = await client.get_entity(channel_id)
        name = getattr(entity, 'title', str(channel_id))
        
        reasons = getattr(entity, 'restriction_reason', []) or []
        restriction_list = []
        is_globally_banned = False
        
        for r in reasons:
            plat = getattr(r, 'platform', '')
            reas = getattr(r, 'reason', '')
            text = getattr(r, 'text', '')
            matched = (plat == 'all' and reas == 'terms')
            if matched: is_globally_banned = True
            restriction_list.append({
                "platform": plat,
                "reason": reas,
                "text": text,
                "matched_all_terms": matched
            })
            
        return {
            "id": channel_id,
            "name": name,
            "restricted": getattr(entity, 'restricted', False),
            "is_globally_banned": is_globally_banned,
            "reasons": restriction_list
        }
            
    except Exception as e:
        return {"id": channel_id, "error": str(e)}

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    session_name = 'data/sessions/copilot_user_temp'
    
    # Check a few more from "精品捆绑" that were skipped
    channels = [
        -1003080296008, # SM调教️🅢🅜💋
        -1003074549125, # SM/绳艺/捆绑/私房调教 字母圈资源汇
        -1003059133638, # 绳艺搬运随选
        -1003023798330  # 厕奴喝尿舔阴
    ]
    
    results = []
    async with TelegramClient(session_name, api_id, api_hash) as client:
        for cid in channels:
            results.append(await check_channel(client, cid))
            
    with open('restrictions_report_2.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("✅ Results saved to restrictions_report_2.json")

if __name__ == "__main__":
    asyncio.run(main())
