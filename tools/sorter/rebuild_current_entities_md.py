#!/usr/bin/env python3
"""
仅根据 entities.json 重建 current_entities.md。
不读取 candidate pool，不改 entities.json。
"""
import os
import sys
import json

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(project_root)
from src.utils.config import get_bot_config
from src.search_mode.program1_discovery.sync_entities import EntitySyncer


def main():
    bot = 'tgporncopilot'
    if len(sys.argv) > 1:
        bot = sys.argv[1]
    cfg = get_bot_config(bot)
    entities_path = os.path.join(project_root, cfg['entities_json'])
    current_md = os.path.join(project_root, cfg['current_entities_md'])
    pool_dir = os.path.join(project_root, cfg['candidates_dir'])

    syncer = EntitySyncer(pool_dir=pool_dir, entities_path=entities_path, current_md=current_md)
    entities = syncer.load_entities()
    syncer.export_markdown(entities)
    print(f'rebuilt: {current_md}')


if __name__ == '__main__':
    main()
