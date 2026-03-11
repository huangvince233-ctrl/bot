import sys
import os
import json
from pathlib import Path

# Mock self and dependencies
PROJECT_ROOT = Path('.').resolve()
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / 'src'))

from src.db import Database

class BackupFixTester:
    def __init__(self, db):
        self.db = db

    def build_test(self, chat_id, records):
        """Minimal version of the fixed build_full_historical_snapshot"""
        for item in records:
            msg_id = item.get('msg_id')
            res_ids = item.get('res_ids')
            is_empty = not res_ids or all(v is None or v == [] for v in res_ids.values())
            if is_empty:
                print(f"Detecting missing IDs for msg_id {msg_id}, fetching from DB...")
                db_res = self.db.get_message_res_ids(chat_id, msg_id)
                if db_res:
                    print(f"  Successfully backfilled IDs for {msg_id}")
                    item['res_ids'] = db_res
                else:
                    print(f"  DB still has no IDs for {msg_id}")
        return records

db = Database('data/copilot.db')
chat_id = -1002784674222

# Fake some records that we know are in DB but missing IDs in JSON
# In step 1320, we saw msg_id 1706, 1705, 1661 etc. but they have None in DB.
# Wait! If they have None in DB, backfill won't help unless someone assigns them!

# Let's check if ANY message has res_id in DB for this channel.
print("Checking for ANY indexed message in DB...")
db.cursor.execute("SELECT msg_id, res_id FROM global_messages WHERE chat_id = ? AND res_id IS NOT NULL LIMIT 1", (chat_id,))
row = db.cursor.fetchone()
if row:
    print(f"Found sample indexed message in DB: {row}")
    test_msg_id = row[0]
    test_records = [
        {"msg_id": test_msg_id, "res_ids": {"total": None}}
    ]
    tester = BackupFixTester(db)
    result = tester.build_test(chat_id, test_records)
    print(f"Result for {test_msg_id}: {result[0]['res_ids']}")
else:
    print("NO indexed messages in DB for this channel. Backfill will have no effect yet.")
    print("This means the user needs to run a P2/P3 sync OR a Global Backup to assign IDs.")
    
db.close()
