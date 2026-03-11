from pathlib import Path
import sqlite3
import json
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / 'data' / 'copilot.db'


def safe_abs_int(value):
    return abs(int(str(value).strip()))


def normalize_title(title: str) -> str:
    return title.strip() if title else ''


def main(argv):
    if len(argv) < 2:
        print('usage: python scripts/reset_channel_backup_state.py <chat_id> [<chat_id> ...]')
        return 1

    target_ids = {safe_abs_int(arg) for arg in argv[1:]}

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    removed = []
    missing = []
    updated_runs = []

    # 1) 清 backup_offsets
    placeholders = ','.join(['?'] * len(target_ids))
    cur.execute(f'DELETE FROM backup_offsets WHERE chat_id IN ({placeholders})', tuple(sorted(target_ids)))

    # 2) 清 backup_runs 中这些频道的历史文件引用，避免树状图继续误判为有本地快照
    run_rows = cur.execute(
        '''
        SELECT run_id, channels_detail
        FROM backup_runs
        WHERE channels_detail IS NOT NULL
        ORDER BY run_id DESC
        '''
    ).fetchall()

    title_candidates = set()
    for row in run_rows:
        detail = row['channels_detail']
        try:
            channels = json.loads(detail)
        except Exception:
            continue
        if not isinstance(channels, list):
            continue

        changed = False
        for ch in channels:
            cid = ch.get('id') or ch.get('chat_id')
            try:
                norm = safe_abs_int(cid)
            except Exception:
                continue
            if norm not in target_ids:
                continue

            title = normalize_title(ch.get('name'))
            if title:
                title_candidates.add(title)

            if ch.get('json_file') is not None:
                ch['json_file'] = None
                changed = True
            if ch.get('md_file') is not None:
                ch['md_file'] = None
                changed = True
            if ch.get('status') == 'completed':
                ch['status'] = 'historical_missing'
                changed = True

        if changed:
            cur.execute(
                'UPDATE backup_runs SET channels_detail = ? WHERE run_id = ?',
                (json.dumps(channels, ensure_ascii=False), row['run_id'])
            )
            updated_runs.append(row['run_id'])

    conn.commit()
    conn.close()

    # 3) 清理由频道名碰撞导致的 metadata / 旧共享目录残留
    for title in sorted(title_candidates):
        paths_to_remove = [
            ROOT / 'data' / 'metadata' / '精品捆绑' / f'{title}.json',
            ROOT / 'docs' / 'metadata' / '精品捆绑' / f'{title}.md',
            ROOT / 'docs' / 'tags' / 'tgporncopilot' / '精品捆绑' / '绳赋_BDSM_Lifestye_.md' if title == '绳赋(BDSM Lifestye)' else None,
        ]
        for path in paths_to_remove:
            if path is None:
                continue
            if path.exists():
                path.unlink()
                removed.append(str(path.relative_to(ROOT)))
            else:
                missing.append(str(path.relative_to(ROOT)))

        for rel in [
            Path('data/archived/backups/精品捆绑') / title,
            Path('docs/archived/backups/精品捆绑') / title,
        ]:
            p = ROOT / rel
            if p.exists() and p.is_dir():
                shutil.rmtree(p)
                removed.append(str(rel))

    print('target_ids=', sorted(target_ids))
    print('updated_runs=', updated_runs)
    print('removed=', json.dumps(removed, ensure_ascii=False, indent=2))
    print('missing=', json.dumps(missing, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))