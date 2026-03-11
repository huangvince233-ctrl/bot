
import os
import asyncio
import sqlite3
from telethon import TelegramClient, utils as telethon_utils
from dotenv import load_dotenv
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))
from sync_mode.sync import classify_message

async def main():
    load_dotenv()
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    session_name = 'data/sessions/copilot_user'
    
    # Target channel: 纪录库-纪录用频道 (-1001395222731)
    target_id = -1001395222731
    
    db_path = 'data/copilot_temp.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Normalize ID
        norm_id = abs(int(target_id)) % 1000000000000
        print(f"🔍 Diagnostic for Channel ID: {target_id} (Normalized: {norm_id})")
        
        # 1. Check backup_offsets
        cursor.execute('SELECT last_msg_id FROM backup_offsets WHERE chat_id = ?', (norm_id,))
        row = cursor.fetchone()
        backup_offset = row[0] if row else 0
        print(f"📊 DB backup_offsets.last_msg_id: {backup_offset}")
        
        # 2. Check sync_offsets
        cursor.execute('SELECT last_msg_id FROM sync_offsets WHERE chat_id = ?', (norm_id,))
        row = cursor.fetchone()
        sync_offset = row[0] if row else 0
        print(f"📊 DB sync_offsets.last_msg_id: {sync_offset}")
        
        # 4. Check Epoch Start ID
        cursor.execute('''
            SELECT MIN(original_msg_id), COUNT(*)
            FROM messages 
            WHERE original_chat_id = ? 
            AND sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 0)
        ''', (target_id,))
        row = cursor.fetchone()
        epoch_min = row[0] if row else 0
        epoch_count = row[1] if row else 0
        print(f"📊 DB Epoch Min ID: {epoch_min} (Sync count in this epoch: {epoch_count})")
        
        epoch_start_calc = max(0, (epoch_min - 1)) if (epoch_min and epoch_min > 0) else 0
        print(f"📊 Computed epoch_start_msg_id: {epoch_start_calc}")

        async with TelegramClient(session_name, api_id, api_hash) as client:
            entity = await client.get_entity(target_id)
            print(f"✅ Found entity: {getattr(entity, 'title', 'Unknown')}")
            
            print("\n📜 Latest 5 messages in Telegram:")
            latest_id = 0
            async for msg in client.iter_messages(entity, limit=5):
                if latest_id == 0: latest_id = msg.id
                m_type = classify_message(msg)
                print(f"  - ID: {msg.id} | Type: {m_type} | Text: {(msg.text or '')[:30]!r}")
            
            print(f"\n📈 Telegram Latest ID: {latest_id}")
            
            if latest_id > backup_offset:
                print(f"  👉 There ARE {latest_id - backup_offset} potential new messages (by ID gap).")
                print(f"🔍 Analyzing messages from {backup_offset + 1} to {latest_id}...")
                
                count = 0
                skipped = 0
                types_found = {}
                async for msg in client.iter_messages(entity, min_id=backup_offset, reverse=True):
                    count += 1
                    m_type = classify_message(msg)
                    types_found[m_type] = types_found.get(m_type, 0) + 1
                    if m_type == 'skip':
                        skipped += 1
                    if count <= 10 or count > (latest_id - backup_offset - 5):
                        print(f"    - Msg #{msg.id}: Type={m_type}")
                
                print(f"\n📊 Gap Analysis Results:")
                print(f"    - Total messages fetched: {count}")
                print(f"    - Skipped: {skipped}")
                print(f"    - Type distribution: {types_found}")
            else:
                print(f"  👉 Backup offset ({backup_offset}) is caught up with latest ID ({latest_id}).")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
