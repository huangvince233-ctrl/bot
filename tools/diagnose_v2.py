
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
    print("--- Database Detailed Check ---")
    conn = sqlite3.connect('data/copilot.db')
    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    
    print("\n[sync_runs] latest 5:")
    runs = cursor.execute('SELECT run_id, formal_number, is_test, bot_name, start_msg_id, end_msg_id, start_time FROM sync_runs ORDER BY run_id DESC LIMIT 5').fetchall()
    for r in runs:
        print(dict(r))
        
    print("\n[messages] counts by sync_run_id:")
    msg_counts = cursor.execute('SELECT sync_run_id, COUNT(*) as cnt FROM messages GROUP BY sync_run_id').fetchall()
    for m in msg_counts:
        print(dict(m))
        
    print("\n[sync_offsets] state:")
    offsets = cursor.execute('SELECT * FROM sync_offsets').fetchall()
    for o in offsets:
        print(dict(o))

    # Check for messages of run 3 (Formal #1)
    run3_msgs = cursor.execute('SELECT forwarded_msg_id FROM messages WHERE sync_run_id = 3').fetchall()
    ids = [r[0] for r in run3_msgs if r[0] > 0]
    print(f"\nRun ID 3 message IDs (total {len(ids)}): {ids[:20]}...")
    
    conn.close()
    
    if not TARGET_GROUP_ID:
        print("\nTarget group ID not found in .env")
    else:
        print(f"\nTarget Group ID: {TARGET_GROUP_ID}")

    if ids:
        print("\n--- Telegram Connectivity Check ---")
        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
            try:
                entity = await client.get_entity(TARGET_GROUP_ID)
                print(f"Connected to group: {entity.title}")
                
                # Verify a few IDs
                verified = await client.get_messages(entity, ids=ids[:10])
                for m in verified:
                    if m:
                        print(f"  ID {m.id} exists. Date: {m.date}")
                    else:
                        print(f"  ID in list MISSING in Telegram")
                
            except Exception as e:
                print(f"Telegram Error: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose())
