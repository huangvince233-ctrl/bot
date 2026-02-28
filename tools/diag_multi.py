import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def check_multi():
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        targets = [-1003323249740, -1001395222731, -1003077564843]
        for tid in targets:
            try:
                ent = await client.get_entity(tid)
                title = getattr(ent, 'title', 'N/A')
                print(f"--- {title} ({tid}) ---")
                print(f"Restricted: {getattr(ent, 'restricted', False)}")
                if hasattr(ent, 'restriction_reason') and ent.restriction_reason:
                    for r in ent.restriction_reason:
                        print(f"  Reason: scope={r.platform}, reason={r.reason}, text={r.text}")
                
                # Check message access
                try:
                    msgs = await client.get_messages(ent, limit=1)
                    print(f"Bot Access: OK (Found {len(msgs)} msgs)")
                    if len(msgs) > 0:
                        print(f"  Latest msg: {msgs[0].id}")
                except Exception as ex:
                    print(f"Bot Access: FAILED ({type(ex).__name__}): {ex}")
                print("\n")
            except Exception as e:
                print(f"--- ID: {tid} (GET ENTITY FAILED) ---")
                print(f"Error: {type(e).__name__}: {e}\n")

if __name__ == "__main__":
    asyncio.run(check_multi())
