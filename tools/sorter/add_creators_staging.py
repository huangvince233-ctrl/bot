#!/usr/bin/env python3
"""
将一组创作者写入 sorter 的 staging_decisions.json（标记为暂存 creator）。

用法:
  python tools/sorter/add_creators_staging.py --bot tgporncopilot --file ./names.txt
  python tools/sorter/add_creators_staging.py --bot tgporncopilot --names "名1;名2;名3"
  cat names.txt | python tools/sorter/add_creators_staging.py --bot tgporncopilot

脚本会去重并保留已有的暂存条目。
"""
import sys
import json
from pathlib import Path
import argparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def get_staging_file(bot='tgporncopilot'):
    try:
        from src.utils.config import get_bot_config
        cfg = get_bot_config(bot)
        candidates_dir = PROJECT_ROOT / cfg.get('candidates_dir', 'docs/entities/tgporncopilot_candidates')
    except Exception:
        candidates_dir = PROJECT_ROOT / 'docs/entities/tgporncopilot_candidates'
    return candidates_dir / 'staging_decisions.json'

def load_staging(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8')) or []
        except Exception:
            return []
    return []

def save_staging(path: Path, entries):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--bot', default='tgporncopilot')
    p.add_argument('--file', help='包含一行一个名字的文件')
    p.add_argument('--names', help='分号或逗号分隔的名字列表')
    p.add_argument('--category', default='未分类')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    names = []
    if args.file:
        fp = Path(args.file)
        if not fp.exists():
            print(f'文件不存在: {fp}')
            return
        txt = fp.read_text(encoding='utf-8')
        for line in txt.splitlines():
            line = line.strip()
            if line: names.append(line)
    elif args.names:
        for part in args.names.replace(';', ',').split(','):
            if part.strip(): names.append(part.strip())
    else:
        # read from stdin
        if not sys.stdin.isatty():
            txt = sys.stdin.read()
            for line in txt.splitlines():
                line = line.strip()
                if line: names.append(line)

    if not names:
        print('未接收到任何名字，请通过 --file, --names 或 stdin 提供一行/多行名字。')
        return

    path = get_staging_file(args.bot)
    entries = load_staging(path)
    exist_set = {str(e.get('name', '')).strip().lower() for e in entries}

    added = 0
    for n in names:
        nn = n.strip()
        if not nn: continue
        if nn.lower() in exist_set:
            continue
        entries.append({'name': nn, 'category': args.category, 'is_staging': True, 'type': 'creator'})
        exist_set.add(nn.lower())
        added += 1

    if args.dry_run:
        print(f'将要添加 {added} 个条目，目标文件: {path}\n样例: {entries[-3:]}')
        return

    save_staging(path, entries)
    print(f'已写入 {added} 个暂存创作者到: {path}')

if __name__ == '__main__':
    main()
