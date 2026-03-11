
import sqlite3
import os
import sys

# Ensure we can import Database
sys.path.append('src')
from db import Database

db = Database('data/copilot.db')

with open('diag_ui_results.log', 'w', encoding='utf-8') as f:
    def log(msg):
        print(msg)
        f.write(str(msg) + '\n')

    log("=== Sync Runs ===")
    runs = db.cursor.execute("SELECT run_id, formal_number, is_test, start_time FROM sync_runs ORDER BY run_id DESC LIMIT 10").fetchall()
    for r in runs:
        label = db.get_run_label(r[0])
        log(f"  ID: {r[0]}, Label: {label}, Time: {r[3]}, Test: {r[2]}")

    log("\n=== Recent Sync Offsets ===")
    cursor = db.cursor
    cursor.execute("SELECT chat_id, last_msg_id, updated_at FROM sync_offsets ORDER BY updated_at DESC LIMIT 20")
    offsets = cursor.fetchall()

    for o in offsets:
        log(f"  ChatID: {o[0]}, LastMsgID: {o[1]}, UpdatedAt: {o[2]}")
        # Check latest info for this channel
        info = db.get_latest_sync_info(chat_id=o[0], is_test=True)
        log(f"    Latest Info (is_test=True): {info}")
        info_f = db.get_latest_sync_info(chat_id=o[0], is_test=False)
        log(f"    Latest Info (is_test=False): {info_f}")

db.close()
