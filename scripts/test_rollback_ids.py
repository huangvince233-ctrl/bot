
import os
import sys
from datetime import datetime

# Add src to path
sys.path.append(os.path.abspath('src'))
from db import Database

def test_rollback_collection():
    test_db = 'data/test_rollback.db'
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db = Database(test_db)
    bot_name = 'test_bot'
    
    # 1. Create a target sync run (the one we want to keep)
    target_id = db.start_sync_run(is_test=False, bot_name=bot_name, target_group_id=123)
    db.set_sync_run_boundaries(target_id, start_msg_id=500, end_msg_id=510)
    target_label = db.get_run_label(target_id)
    print(f"Target Run (Keep) Label: {target_label}")

    # 2. Create a subsequent run (the one we want to roll back)
    run_id = db.start_sync_run(is_test=False, bot_name=bot_name, target_group_id=123)
    db.set_sync_run_boundaries(run_id, start_msg_id=1000, end_msg_id=1010)
    
    # Add some messages to the run we'll rollback
    db.save_message(run_id, 'video', 1, 100, forwarded_msg_id=2000, res_id=1, forwarded_chat_id=123, header_msg_id=1500)
    db.save_message(run_id, 'text', 2, 100, forwarded_msg_id=2001, res_id=0, forwarded_chat_id=123, header_msg_id=1501)
    
    # 3. Perform rollback to target_label
    deleted_labels, info = db.rollback_to(target_label, bot_name=bot_name, commit=False)
    
    print(f"Deleted Labels: {deleted_labels}")
    print(f"Rollback Info: {info}")
    
    ids_to_delete = info.get('msg_ids_to_delete', [])
    print(f"IDs to delete: {ids_to_delete}")
    
    # Expected IDs: 
    # Boundary Range: 1000 to 1010 (11 IDs)
    # Message 1 Header: 1500
    # Message 1 Content: 2000
    # Message 2 Header: 1501
    # Message 2 Content: 2001
    
    expected_range = set((123, mid) for mid in range(1000, 1011))
    expected_extras = {(123, 1500), (123, 2000), (123, 1501), (123, 2001)}
    expected = expected_range.union(expected_extras)
    actual = set(ids_to_delete)
    
    if expected.issubset(actual) and len(actual) >= len(expected):
        print("✅ SUCCESS: All expected IDs (including range) identified.")
    else:
        print(f"❌ FAILURE: ID mismatch.")
        print(f"Missing: {expected - actual}")

    db.close()
    if os.path.exists(test_db):
        os.remove(test_db)

if __name__ == "__main__":
    test_rollback_collection()
