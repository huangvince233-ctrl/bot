"""修复 PP#B0 → P#B0 的双前缀问题"""
import os
import re

FIX_RE = re.compile(r'PP#B0')

def fix_dir(base_dir):
    count = 0
    for root, dirs, files in os.walk(base_dir):
        for fname in files:
            if 'PP#B0' in fname:
                new_fname = FIX_RE.sub('P#B0', fname)
                old_full = os.path.join(root, fname)
                new_full = os.path.join(root, new_fname)
                os.rename(old_full, new_full)
                print(f"  ✅ {fname} → {new_fname}")
                count += 1
    return count

total = 0
for base in ['data/archived/backups', 'docs/archived/backups']:
    if os.path.exists(base):
        print(f"\n📂 修复: {base}")
        total += fix_dir(base)

print(f"\n✅ 共修复 {total} 个文件")
