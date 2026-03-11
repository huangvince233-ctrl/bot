import os
import sqlite3
from pathlib import Path

DB_PATH = Path('data/copilot.db')

DATA_FILES = [
    Path(r'data/archived/backups/极品捆绑/猎奇 I SM I 重口/backup_#B1_猎奇 I SM I 重口_20260308_132547.json'),
    Path(r'data/archived/backups/极品捆绑/SM_捆绑_绳艺_调教 字母圈资源汇/backup_#B1_SM_捆绑_绳艺_调教 字母圈资源汇_20260308_132652.json'),
    Path(r'data/archived/backups/极品捆绑/sm萝莉人妻少妇捆绑绳艺播放厅/backup_#B1_sm萝莉人妻少妇捆绑绳艺播放厅_20260308_132728.json'),
]
DOC_FILES = [
    Path(r'docs/archived/backups/极品捆绑/猎奇 I SM I 重口/#B1_猎奇 I SM I 重口_20260308_132547.md'),
    Path(r'docs/archived/backups/极品捆绑/SM_捆绑_绳艺_调教 字母圈资源汇/#B1_SM_捆绑_绳艺_调教 字母圈资源汇_20260308_132652.md'),
    Path(r'docs/archived/backups/极品捆绑/sm萝莉人妻少妇捆绑绳艺播放厅/#B1_sm萝莉人妻少妇捆绑绳艺播放厅_20260308_132728.md'),
]
TEMP_FILES = [
    Path('data/temp/backup_progress.json'),
    Path('data/temp/backup_progress_my_porn_private_bot.json'),
    Path('data/temp/backup_progress_tgporncopilot.json'),
    Path('data/temp/stop_backup.flag'),
    Path('data/temp/stop_backup_my_porn_private_bot.flag'),
    Path('data/temp/stop_backup_tgporncopilot.flag'),
]
TARGET_OFFSET_CHAT_IDS = {2784674222, 2974973326, 3077564843}
TARGET_RUN_ID = 122


def main():
    print('=== deleting files ===')
    for path in DATA_FILES + DOC_FILES + TEMP_FILES:
        try:
            if path.exists():
                path.unlink()
                print('deleted', path)
        except Exception as e:
            print('failed ', path, e)

    print('\n=== updating database ===')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute('DELETE FROM backup_runs WHERE run_id = ?', (TARGET_RUN_ID,))
    print('deleted backup_run', TARGET_RUN_ID, 'rows=', cur.rowcount)

    for chat_id in sorted(TARGET_OFFSET_CHAT_IDS):
        cur.execute('DELETE FROM backup_offsets WHERE chat_id = ? AND is_test = 0', (chat_id,))
        print('deleted backup_offset for', chat_id, 'rows=', cur.rowcount)

    conn.commit()

    remaining_runs = cur.execute('SELECT run_id, formal_number, end_time FROM backup_runs ORDER BY run_id DESC LIMIT 5').fetchall()
    remaining_offsets = cur.execute('SELECT chat_id, last_msg_id FROM backup_offsets ORDER BY updated_at DESC LIMIT 10').fetchall()
    print('\nremaining recent runs =', remaining_runs)
    print('remaining offsets =', remaining_offsets)

    conn.close()


if __name__ == '__main__':
    main()
