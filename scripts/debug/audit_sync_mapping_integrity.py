import json
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from db import Database


def main() -> int:
    db_path = os.path.join(PROJECT_ROOT, 'data', 'copilot.db')
    out_dir = os.path.join(PROJECT_ROOT, 'data', 'temp')
    os.makedirs(out_dir, exist_ok=True)

    db = Database(db_path)
    result = db.repair_and_audit_sync_mappings()

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(out_dir, f'sync_mapping_audit_{ts}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f'审计报告已输出: {out_path}')
    print('修复数量:', result['normalized_message_chat_ids'])
    print('修复前摘要:', json.dumps(result['before']['summary'], ensure_ascii=False))
    print('修复后摘要:', json.dumps(result['after']['summary'], ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
