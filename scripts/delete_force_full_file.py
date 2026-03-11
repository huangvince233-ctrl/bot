from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / 'data' / 'temp' / 'force_full_backup_channels_tgporncopilot.json'
if path.exists():
    path.unlink()
    print(f'deleted: {path.relative_to(ROOT)}')
else:
    print('already missing')
