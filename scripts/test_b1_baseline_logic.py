import os
import sys

sys.path.append(os.path.abspath('src'))
from db import Database


def main():
    test_db = 'data/test_b1_logic.db'
    if os.path.exists(test_db):
        os.remove(test_db)

    db = Database(test_db)
    run_id = db.start_backup_run(mode='2', is_incremental=True, is_test=False, bot_name='tgporncopilot')
    db.finish_backup_run(run_id, {
        'total_messages': 148213,
        'new_messages': 3,
        'total_channels': 39,
        'channels': [{'name': '示例频道', 'count': 100, 'new_count': 3}],
        'duration': '1.0 min'
    })

    runs = db.get_manageable_backup_runs(limit=5, bot_name='tgporncopilot')
    item = next(r for r in runs if r['run_id'] == run_id)
    print(item)

    assert item['incremental'] is False, 'B1 should be displayed as full baseline'
    assert item['new_messages'] == 148213, 'B1 new_messages should be normalized to total_messages for display'
    assert item['is_first_formal_baseline'] is True

    db.close()
    os.remove(test_db)
    print('✅ test_b1_baseline_logic passed')


if __name__ == '__main__':
    main()
