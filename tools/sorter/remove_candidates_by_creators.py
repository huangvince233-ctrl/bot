#!/usr/bin/env python3
"""
从 candidate_pool_part_*.md 中移除与 entities.json 中 creators 列表匹配的词条，并重排编号。
"""
import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
entities_path = PROJECT_ROOT / 'data' / 'entities' / 'tgporncopilot_entities.json'
candidates_dir = PROJECT_ROOT / 'docs' / 'entities' / 'tgporncopilot_candidates'

if not entities_path.exists():
    print('entities file not found:', entities_path)
    exit(1)

creators = []
with entities_path.open(encoding='utf-8') as f:
    data = json.load(f)
    for c in data.get('creators', []):
        name = c.get('name') if isinstance(c, dict) else c
        if name:
            creators.append(name.strip())

if not creators:
    print('no creators found in entities.json')
    exit(0)

def normalize(text: str) -> str:
    s = str(text or '').strip().lower()
    s = re.sub(r'\s+', '', s)
    return s

creator_norms = {normalize(n) for n in creators if normalize(n)}

def should_remove(word: str) -> bool:
    return normalize(word) in creator_norms

pattern = re.compile(r'^(\d+)\.\s*`\s*(.*?)\s*`\s*——\s*(.*)$')

current_rank = 1
for md_file in sorted(candidates_dir.glob('candidate_pool_part_*.md')):
    txt = md_file.read_text(encoding='utf-8')
    lines = txt.splitlines()
    header = []
    word_lines = []
    for line in lines:
        m = pattern.match(line)
        if m:
            word = m.group(2).strip()
            if should_remove(word):
                # skip (remove)
                continue
            # keep, renumber
            rest = m.group(3)
            new_line = f"{current_rank}. ` {word} ` —— {rest}"
            word_lines.append(new_line)
            current_rank += 1
        else:
            if not word_lines:
                header.append(line)
            else:
                # trailing non-word lines; keep as is
                header.append(line)

    if word_lines:
        md_file.write_text('\n'.join(header + word_lines) + '\n', encoding='utf-8')
    else:
        try:
            md_file.unlink()
        except: pass

print('cleanup done, remaining candidate count approx:', current_rank - 1)
