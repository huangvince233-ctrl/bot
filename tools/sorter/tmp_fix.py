import pathlib

p = pathlib.Path('tools/sorter/server.py')
content = p.read_text('utf-8')

# Replace entities_json references
content = content.replace("CONFIG['entities_json']", "CONFIG['currententities_dir_data'] + '/entities.json'")
content = content.replace(
    "CONFIG.get('entities_json', 'data/entities/tgporncopilot_entities.json')",
    "CONFIG.get('currententities_dir_data', 'data/entities/tgporncopilot/currententities') + '/entities.json'"
)

# Replace candidates_dir references
content = content.replace("CONFIG['candidates_dir']", "CONFIG['candidates_dir_docs']")
content = content.replace(
    "CONFIG.get('candidates_dir', 'docs/entities/tgporncopilot_candidates')",
    "CONFIG.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates')"
)
content = content.replace("config['candidates_dir']", "config['candidates_dir_docs']")
content = content.replace("cfg.get('candidates_dir'", "cfg.get('candidates_dir_docs'")

p.write_text(content, 'utf-8')

# Verify
remaining = []
for kw in ['entities_json', 'candidates_dir\']', 'staging_json', 'staging_dir', 'current_entities_md']:
    if kw in content:
        remaining.append(kw)

if remaining:
    print("Still has old keys:", remaining)
else:
    print("All old keys replaced successfully!")
