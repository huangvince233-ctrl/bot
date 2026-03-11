from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'data' / 'copilot.db'
DATA_BACKUPS = ROOT / 'data' / 'archived' / 'backups'
DOCS_BACKUPS = ROOT / 'docs' / 'archived' / 'backups'


def safe_dirname(name: str) -> str:
    if not name:
        return '未命名'
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()


def find_relocated_file(root: Path, folder_name: str, channel_name: str, chat_id: int | str | None, old_path: str | None, want_suffix: str) -> str | None:
    if chat_id is None:
        return None
    safe_folder = safe_dirname(folder_name)
    safe_title = safe_dirname(channel_name)
    norm_id = abs(int(chat_id))
    chan_dir = root / safe_folder / f'{safe_title}_{norm_id}'
    if not chan_dir.is_dir():
        return None

    old_name = Path(old_path).name if old_path else None
    if old_name:
        candidate = chan_dir / old_name
        if candidate.exists():
            return str(candidate)

    files = sorted([p for p in chan_dir.iterdir() if p.is_file() and p.suffix.lower() == want_suffix.lower()])
    if not files:
        return None
    return str(files[-1])


def main(argv: list[str]) -> int:
    dry_run = '--apply' not in argv
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        '''
        SELECT run_id, channels_detail
        FROM backup_runs
        WHERE channels_detail IS NOT NULL
        ORDER BY run_id DESC
        '''
    ).fetchall()

    logs: list[str] = [f'dry_run={dry_run}']
    updated_runs = 0
    updated_paths = 0

    for row in rows:
        run_id = row['run_id']
        detail = row['channels_detail']
        try:
            channels = json.loads(detail)
        except Exception:
            logs.append(f'SKIP_BAD_JSON run_id={run_id}')
            continue
        if not isinstance(channels, list):
            continue

        changed = False
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            cid = ch.get('id') or ch.get('chat_id')
            name = ch.get('name') or ''
            folder = ch.get('folder') or '未分类'

            jf = ch.get('json_file')
            if jf and not os.path.exists(jf):
                new_jf = find_relocated_file(DATA_BACKUPS, folder, name, cid, jf, '.json')
                if new_jf and new_jf != jf:
                    logs.append(f'UPDATE run={run_id} json_file: {jf} -> {new_jf}')
                    ch['json_file'] = new_jf
                    changed = True
                    updated_paths += 1

            mf = ch.get('md_file')
            if mf and not os.path.exists(mf):
                new_mf = find_relocated_file(DOCS_BACKUPS, folder, name, cid, mf, '.md')
                if new_mf and new_mf != mf:
                    logs.append(f'UPDATE run={run_id} md_file: {mf} -> {new_mf}')
                    ch['md_file'] = new_mf
                    changed = True
                    updated_paths += 1

        if changed:
            updated_runs += 1
            if not dry_run:
                cur.execute(
                    'UPDATE backup_runs SET channels_detail = ? WHERE run_id = ?',
                    (json.dumps(channels, ensure_ascii=False), run_id)
                )

    if not dry_run:
        conn.commit()
    conn.close()

    logs.append(f'updated_runs={updated_runs}')
    logs.append(f'updated_paths={updated_paths}')
    out_path = ROOT / 'data' / 'temp' / 'fix_backup_run_paths.log'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text('\n'.join(logs), encoding='utf-8')
    print(f'log written to: {out_path}')
    print(f'dry_run={dry_run}; updated_runs={updated_runs}; updated_paths={updated_paths}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
