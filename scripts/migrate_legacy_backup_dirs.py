from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_BACKUPS = ROOT / 'data' / 'archived' / 'backups'
DOCS_BACKUPS = ROOT / 'docs' / 'archived' / 'backups'
DATA_METADATA = ROOT / 'data' / 'metadata'


def safe_dirname(name: str) -> str:
    if not name:
        return '未命名'
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()


def build_title_to_id_map() -> dict[str, int]:
    mapping: dict[str, int] = {}
    if not DATA_METADATA.exists():
        return mapping

    for root, _, files in os.walk(DATA_METADATA):
        for file in files:
            if not file.endswith('.json'):
                continue
            path = Path(root) / file
            try:
                payload = json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            raw_id = payload.get('id') or payload.get('chat_id')
            title = payload.get('canonical_name') or payload.get('latest_name') or payload.get('title')
            if raw_id is None or not title:
                continue
            try:
                mapping[safe_dirname(title)] = abs(int(raw_id))
            except Exception:
                continue
    return mapping


def merge_dir(src: Path, dst: Path, dry_run: bool, logs: list[str]) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            merge_dir(item, target, dry_run, logs)
            continue
        if target.exists():
            logs.append(f'  SKIP  {item} -> {target} (exists)')
            continue
        logs.append(f'  MOVE  {item} -> {target}')
        if not dry_run:
            shutil.move(str(item), str(target))
    # 如果目录空了，删除它
    try:
        if not any(src.iterdir()):
            logs.append(f'  RMDIR {src}')
            if not dry_run:
                src.rmdir()
    except Exception:
        pass


def migrate_tree(root: Path, title_to_id: dict[str, int], dry_run: bool) -> list[str]:
    logs: list[str] = []
    if not root.exists():
        return logs

    for folder in root.iterdir():
        if not folder.is_dir():
            continue
        # 仅处理真正的备份频道目录，跳过 bot/好友/杂项根目录
        if folder.name in {'bot_官频_私群_好友'}:
            logs.append(f'SKIP_FOLDER {folder}')
            continue
        for chdir in list(folder.iterdir()):
            if not chdir.is_dir():
                continue

            # 已经是新格式：末尾带 _数字
            if re.search(r'_(\d+)$', chdir.name):
                continue

            safe_name = safe_dirname(chdir.name)
            chat_id = title_to_id.get(safe_name)
            if not chat_id:
                logs.append(f'SKIP_UNMAPPED {chdir}')
                continue

            target = chdir.parent / f'{safe_name}_{chat_id}'
            if target == chdir:
                continue

            if target.exists():
                logs.append(f'MERGE {chdir} -> {target}')
                if not dry_run:
                    merge_dir(chdir, target, dry_run, logs)
                    try:
                        if chdir.exists() and not any(chdir.iterdir()):
                            chdir.rmdir()
                    except Exception:
                        pass
            else:
                logs.append(f'RENAME {chdir} -> {target}')
                if not dry_run:
                    chdir.rename(target)
    return logs


def main(argv: list[str]) -> int:
    dry_run = '--apply' not in argv
    title_to_id = build_title_to_id_map()

    logs: list[str] = []
    logs.append(f'dry_run={dry_run}')
    logs.append(f'mapped_titles={len(title_to_id)}')
    logs.append('--- DATA ---')
    logs.extend(migrate_tree(DATA_BACKUPS, title_to_id, dry_run))
    logs.append('--- DOCS ---')
    logs.extend(migrate_tree(DOCS_BACKUPS, title_to_id, dry_run))

    out_path = ROOT / 'data' / 'temp' / 'migrate_legacy_backup_dirs.log'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text('\n'.join(logs), encoding='utf-8')
    print(f'log written to: {out_path}')
    rename_count = sum(1 for line in logs if line.startswith('RENAME '))
    merge_count = sum(1 for line in logs if line.startswith('MERGE '))
    skip_unmapped_count = sum(1 for line in logs if line.startswith('SKIP_UNMAPPED '))
    print(f'dry_run={dry_run}; rename={rename_count}; merge={merge_count}; skip_unmapped={skip_unmapped_count}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
