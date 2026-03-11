import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath('src'))
from db import Database

BOT_NAME = 'my_bdsm_private_bot'
DB_PATH = 'data/copilot.db'


def main():
    db = Database(DB_PATH)
    before = db.cursor.execute(
        'SELECT COUNT(*) FROM backup_offsets WHERE is_test = 0'
    ).fetchone()[0]
    print(f'before offsets count = {before}')

    # 当前真实库里 backup_runs 已被删空，recalc 需要 clear_missing=True 来清理残留断点。
    recalculated = db.recalc_backup_offsets(bot_name=BOT_NAME, is_test=False, clear_missing=True)

    after = db.cursor.execute(
        'SELECT COUNT(*) FROM backup_offsets WHERE is_test = 0'
    ).fetchone()[0]
    print(f'recalculated entries = {len(recalculated)}')
    print(f'after offsets count = {after}')

    sample = db.cursor.execute(
        'SELECT chat_id, last_msg_id, updated_at, is_test FROM backup_offsets ORDER BY updated_at DESC LIMIT 10'
    ).fetchall()
    print('sample remaining offsets:')
    for row in sample:
        print(row)

    db.close()


if __name__ == '__main__':
    main()
