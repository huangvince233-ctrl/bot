#!/usr/bin/env python3
"""
将 staging_decisions.json 中 type=='creator' 的名字合并到 data/entities/..._entities.json 的 creators 列表（去重）。
"""
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
staging_path = PROJECT_ROOT / 'docs' / 'entities' / 'tgporncopilot_candidates' / 'staging_decisions.json'
entities_path = PROJECT_ROOT / 'data' / 'entities' / 'tgporncopilot_entities.json'

staged = []
if staging_path.exists():
    try:
        arr = json.loads(staging_path.read_text(encoding='utf-8'))
        for e in arr:
            if isinstance(e, dict) and e.get('type') == 'creator':
                name = e.get('name')
                if name:
                    staged.append(name)
    except Exception as ex:
        print('failed reading staging:', ex)

if not staged:
    print('no staged creators found')
    exit(0)

if not entities_path.exists():
    print('entities json not found:', entities_path)
    exit(1)

data = json.loads(entities_path.read_text(encoding='utf-8'))
creators = data.get('creators', []) or []
exist = { (c.get('name') if isinstance(c, dict) else c).strip().lower() for c in creators }
added = 0
for name in staged:
    if name.strip().lower() in exist:
        continue
    creators.append({'name': name, 'aliases': []})
    exist.add(name.strip().lower())
    added += 1

if added > 0:
    data['creators'] = creators
    entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

print('added', added, 'creators to', entities_path)

# Do not remove staging entries; they remain for UI/reference
