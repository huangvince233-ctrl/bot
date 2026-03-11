
import sqlite3
import os
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'
TARGET_GROUP_ID = int(os.getenv('TARGET_GROUP_ID')) if os.getenv('TARGET_GROUP_ID') else None

async def diagnose():
    print("--- Database Check ---")
    conn = sqlite3.connect('data/copilot.db')
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    
    runs = cursor.execute('SELECT * FROM sync_runs ORDER BY run_id DESC').fetchall()
    print("Recent Sync Runs:")
    for r in runs:
        print(dict(r))
        
    msg_count = cursor.execute('SELECT sync_run_id, COUNT(*) as cnt FROM messages GROUP BY sync_run_id').fetchall()
    print("\nMessage counts by run_id:")
    for m in msg_count:
        print(dict(m))
        
    # Check if run_id 3 (if it exists) has messages
    ids = cursor.execute("SELECT forwarded_msg_id FROM messages WHERE sync_run_id = 3").fetchall()
    fwd_ids = [r[0] for r in ids if r[0] > 0]
    print(f"\nForwarded IDs for run_id 3: {fwd_ids[:10]} ... (total {len(fwd_ids)})")
    
    conn.close()
    
    if not TARGET_GROUP_ID:
        print("\nTarget group ID not found in .env, skipping Telegram check")
        return

    print("\n--- Telegram Check ---")
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        try:
            entity = await client.get_entity(TARGET_GROUP_ID)
            print(f"Checking messages in group: {entity.title}")
            
            # Check a few specific IDs if we found them in DB
            if fwd_ids:
                msgs = await client.get_messages(entity, ids=fwd_ids[:5])
                for m in msgs:
                    if m:
                        print(f"  Found message {m.id}: {str(m.text)[:50]}")
                    else:
                        print(f"  Message from DB NOT found in Telegram (deleted?)")
            
            # Search for label "#1"
            print("\nSearching for label '#1' in group...")
            async for message in client.iter_messages(entity, search='#1', limit=5):
                print(f"  Search Result: Message {message.id} contains '#1': {message.date}")

        except Exception as e:
            print(f"Error checking Telegram: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose())
