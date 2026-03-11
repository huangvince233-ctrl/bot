import os
import sys
import json
from pathlib import Path

sys.path.append(os.path.abspath('src'))
from db import Database


def _write_backup_json(path, msg_ids):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [{"msg_id": mid, "type": "text", "text": f"msg-{mid}", "res_ids": {}} for mid in msg_ids]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def test_recalc_offsets_to_b1_tail():
    bot_name = 'my_bdsm_private_bot'
    test_db = Path('data/test_backup_offsets.db')
    if test_db.exists():
        test_db.unlink()

    db = Database(str(test_db))
    chat_id = -1001234567890
    norm_chat_id = db._normalize_id(chat_id)

    backup_dir = Path('data/archived/backups/TestFolder/TestChannel')
    docs_dir = Path('docs/archived/backups/TestFolder/TestChannel')
    b1_json = backup_dir / 'backup_#B1_TestChannel_20260308_120000.json'
    b2_json = backup_dir / 'backup_#B2_TestChannel_20260308_130000.json'
    b1_md = docs_dir / '#B1_TestChannel_20260308_120000.md'
    b2_md = docs_dir / '#B2_TestChannel_20260308_130000.md'

    _write_backup_json(b1_json, [1, 2, 3, 4, 5])
    _write_backup_json(b2_json, [6, 7, 8])
    b1_md.parent.mkdir(parents=True, exist_ok=True)
    b1_md.write_text('# B1', encoding='utf-8')
    b2_md.write_text('# B2', encoding='utf-8')

    run1 = db.start_backup_run(mode='2', is_incremental=False, is_test=False, bot_name=bot_name)
    db.finish_backup_run(run1, {
        'duration': '00:01:00',
        'total_channels': 1,
        'total_messages': 5,
        'new_messages': 5,
        'channels': [{
            'id': chat_id,
            'name': 'TestChannel',
            'count': 5,
            'new_count': 5,
            'json_file': str(b1_json),
            'md_file': str(b1_md),
        }]
    })

    run2 = db.start_backup_run(mode='2', is_incremental=True, is_test=False, bot_name=bot_name)
    db.finish_backup_run(run2, {
        'duration': '00:00:20',
        'total_channels': 1,
        'total_messages': 8,
        'new_messages': 3,
        'channels': [{
            'id': chat_id,
            'name': 'TestChannel',
            'count': 8,
            'new_count': 3,
            'json_file': str(b2_json),
            'md_file': str(b2_md),
        }]
    })

    db.update_backup_offset(chat_id, 8, is_test=False)
    before = db.get_backup_offset(chat_id, is_test=False)
    print(f'before recalc offset = {before}')

    db.delete_backup_run(run2)
    b2_json.unlink(missing_ok=True)
    b2_md.unlink(missing_ok=True)

    recalculated = db.recalc_backup_offsets(bot_name=bot_name, affected_chat_ids={chat_id}, clear_missing=True)
    after = db.get_backup_offset(chat_id, is_test=False)
    print(f'recalculated entries = {recalculated}')
    print(f'after recalc offset = {after}')

    assert before == 8, f'expected old offset=8, got {before}'
    assert after == 5, f'expected offset rollback to B1 tail=5, got {after}'
    assert (norm_chat_id, 0) in recalculated, 'expected recalculated entry for target chat'

    db.close()
    if test_db.exists():
        test_db.unlink()
    b1_json.unlink(missing_ok=True)
    b1_md.unlink(missing_ok=True)
    try:
        backup_dir.rmdir()
        docs_dir.rmdir()
        backup_dir.parent.rmdir()
        docs_dir.parent.rmdir()
    except Exception:
        pass


if __name__ == '__main__':
    test_recalc_offsets_to_b1_tail()
    print('✅ test_recalc_offsets_to_b1_tail passed')
