from pathlib import Path

roots = [
    Path('data/archived/backups/精品捆绑/绳赋(BDSM Lifestye)_1002919642039'),
    Path('data/archived/backups/精品捆绑/绳赋(BDSM Lifestye)_1002829404994'),
    Path('docs/archived/backups/精品捆绑/绳赋(BDSM Lifestye)_1002919642039'),
    Path('docs/archived/backups/精品捆绑/绳赋(BDSM Lifestye)_1002829404994'),
    Path('data/archived/backups/精品捆绑/绳赋(BDSM Lifestye)'),
    Path('docs/archived/backups/精品捆绑/绳赋(BDSM Lifestye)'),
]

for root in roots:
    if not root.exists():
        continue
    for p in root.rglob('*'):
        if p.is_file():
            print('delete', p)
            p.unlink(missing_ok=True)
