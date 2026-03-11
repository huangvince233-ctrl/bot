import os
import re
import json
from pathlib import Path

# Mocking the environment
PROJECT_ROOT = Path("f:/funny_project/tgporncopilot")
config = {"candidates_dir": "docs/entities/tgporncopilot_candidates"}

def test_load():
    candidates_dir = PROJECT_ROOT / config['candidates_dir']
    print(f"Checking dir: {candidates_dir}")
    if not candidates_dir.exists():
        print("Dir not found!")
        return

    pattern_v2 = re.compile(
        r'\d+\.\s*`\s*(.*?)\s*`\s*——\s*\[(.)\]\s*CREATOR\s*\|\s*\[(.)\]\s*ACTOR\s*\|\s*\[(.)\]\s*TAG(?:\((.*?)\))?\s*\|\s*\[(.)\]\s*NOISE'
        r'.*?频次:\s*(\d+),\s*来源:\s*(.*?)\)'
    )
    
    results = []
    seen = set()
    
    files = sorted(candidates_dir.glob("candidate_pool_part_*.md"))
    print(f"Found {len(files)} files.")
    
    for md_file in files:
        content = md_file.read_text(encoding='utf-8')
        matches = list(pattern_v2.finditer(content))
        print(f"File {md_file.name}: {len(matches)} matches.")
        for m in matches:
            word = m.group(1).strip()
            if word not in seen:
                seen.add(word)
                results.append(word)
    
    print(f"Total candidates loaded: {len(results)}")

test_load()
