import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')

async def check():
    async with TelegramClient('data/sessions/copilot_user', API_ID, API_HASH) as client:
        # The one user says is banned
        banned_id = -1003323249740
        # The one user says is fine
        fine_id = -1001395222731
        
        for tid in [banned_id, fine_id]:
            try:
                ent = await client.get_entity(tid)
                print(f"--- {ent.title} ({tid}) ---")
                print(f"Restricted: {getattr(ent, 'restricted', False)}")
                reasons = getattr(ent, 'restriction_reason', [])
                if reasons:
                    for r in reasons:
                        print(f"  Reason: platform={r.platform}, reason={r.reason}, text={r.text}")
                else:
                    print("  No restriction reasons found.")
                print("\n")
            except Exception as e:
                print(f"FAILED to get {tid}: {e}")

if __name__ == "__main__":
    asyncio.run(check())
