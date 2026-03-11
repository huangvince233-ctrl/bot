import json
import os
from pathlib import Path

# Config
BOT = 'tgporncopilot'
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
candidates_dir = PROJECT_ROOT / f'docs/entities/{BOT}_candidates'
samples_file = candidates_dir / 'candidate_samples.json'
metadata_file = candidates_dir / 'candidate_metadata.json'

def recover():
    if not samples_file.exists():
        print(f"Error: Samples file not found at {samples_file}")
        return

    print(f"Reading {samples_file}...")
    with open(samples_file, 'r', encoding='utf-8') as f:
        samples = json.load(f)

    words = sorted(samples.keys())
    total = len(words)
    print(f"Found {total} words in samples.json. Rebuilding pool...")

    chunk_size = 500
    for i in range(0, total, chunk_size):
        chunk = words[i:i+chunk_size]
        part_num = (i // chunk_size) + 1
        md_file = candidates_dir / f"candidate_pool_part_{part_num:03d}.md"
        
        header = f"# 🔍 候选词分拣池 (恢复版) - 分卷 {part_num:03d}\n\n"
        lines = []
        for j, word in enumerate(chunk):
            rank = i + j + 1
            freq = len(samples.get(word, []))
            # 统一格式，确保能被 server.py 的正则识别
            line = f"{rank}. ` {word} ` —— [ ] CREATOR | [ ] ACTOR | [ ] TAG | [ ] NOISE (频次: {freq}, 来源: restored)\n"
            lines.append(line)
            
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(header + "".join(lines))
        print(f"Created {md_file.name}")
            
    # Update metadata
    if metadata_file.exists():
        with open(metadata_file, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        meta['candidate_count'] = total
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)
    print("✅ Recovery complete!")

if __name__ == "__main__":
    recover()
