
import asyncio
from telethon import TelegramClient
import sqlite3
import os
from dotenv import load_dotenv
from utils.config import CONFIG

async def clean_test3_messages():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    
    # We use user_client to delete properly like the bot does
    session_name = 'data/sessions/copilot_user'
    client = TelegramClient(session_name, api_id, api_hash)
    await client.connect()
    print("Logged in as User.")

    db_path = 'data/copilot.db'
    conn = sqlite3.connect(db_path)
    
    # Run 4 is TEST-3
    bounds = conn.execute("SELECT target_group_id, start_msg_id, end_msg_id FROM sync_runs WHERE run_id = 4").fetchone()
    if not bounds:
        print("Run 4 (TEST-3) not found in db.")
        return
        
    target_group_id, start_msg_id, end_msg_id = bounds
    print(f"TEST-3 boundaries: {start_msg_id} to {end_msg_id}")
    
    if not start_msg_id or not end_msg_id:
        print("No valid boundaries found.")
        return
        
    entity = await client.get_entity(target_group_id)
    print(f"Target Group: {getattr(entity, 'title', target_group_id)}")
    
    ids_to_del = list(range(start_msg_id, end_msg_id + 1))
    print(f"Will delete {len(ids_to_del)} messages in range [{start_msg_id}, {end_msg_id}]")
    
    chunk_size = 100
    for i in range(0, len(ids_to_del), chunk_size):
        chunk = ids_to_del[i:i + chunk_size]
        try:
            await client.delete_messages(entity, chunk, revoke=True)
            print(f"Deleted chunk {chunk[0]} - {chunk[-1]}")
        except Exception as e:
            print(f"Failed to delete chunk: {e}")
        await asyncio.sleep(1)
        
    print("Done cleaning TEST-3 physical messages.")
    conn.close()
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(clean_test3_messages())
