import sqlite3
import json
import os

db_path = 'data/copilot.db'
metadata_dir = 'data/metadata'

def migrate():
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Build mapping from metadata
    id_map = {} # {raw_id: signed_id}
    
    print("🔍 Scanning metadata for ID mapping...")
    for root, dirs, files in os.walk(metadata_dir):
        for f in files:
            if f.endswith('.json'):
                path = os.path.join(root, f)
                try:
                    with open(path, 'r', encoding='utf-8') as jf:
                        data = json.load(jf)
                        # Metadata uses 'id' instead of 'chat_id'
                        signed_id = data.get('id') or data.get('chat_id')
                        if signed_id is None: continue
                        
                        signed_id = int(signed_id)
                        s_str = str(signed_id)
                        
                        raw_id = None
                        if s_str.startswith('-100'):
                            raw_id = int(s_str[4:])
                        elif s_str.startswith('-'):
                            raw_id = abs(signed_id)
                        else:
                            # Already positive?
                            continue
                        
                        id_map[raw_id] = signed_id
                        # Also map signed_id to itself just in case
                        id_map[signed_id] = signed_id
                        
                except Exception as e:
                    print(f"  ⚠️ Error reading {f}: {e}")

    print(f"✅ Found {len(id_map)} mappings.")

    # 2. Update backup_offsets
    print("🔄 Updating backup_offsets...")
    cursor.execute("SELECT chat_id FROM backup_offsets")
    offsets = cursor.fetchall()
    updated_offsets = 0
    for row in offsets:
        old_id = row['chat_id']
        if old_id in id_map:
            new_id = id_map[old_id]
            if old_id != new_id:
                cursor.execute("UPDATE backup_offsets SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
                updated_offsets += 1
    print(f"  ✨ Updated {updated_offsets} offsets.")

    # 3. Update backup_runs
    print("🔄 Updating backup_runs (channels_detail)...")
    cursor.execute("SELECT run_id, channels_detail FROM backup_runs")
    runs = cursor.fetchall()
    updated_runs = 0
    for row in runs:
        run_id = row['run_id']
        detail_str = row['channels_detail']
        if not detail_str: continue
        
        try:
            details = json.loads(detail_str)
            changed = False
            # details can be a list of channels or a dict
            if isinstance(details, list):
                for chan in details:
                    old_id = chan.get('id')
                    if old_id in id_map:
                        new_id = id_map[old_id]
                        if old_id != new_id:
                            chan['id'] = new_id
                            changed = True
            elif isinstance(details, dict):
                # Handle dictionary format if it exists
                for old_id_str, info in details.items():
                    try:
                        old_id = int(old_id_str)
                        if old_id in id_map:
                            # Note: Dict keys can't be updated in place easily
                            pass
                    except: pass

            if changed:
                cursor.execute("UPDATE backup_runs SET channels_detail = ? WHERE run_id = ?", (json.dumps(details, ensure_ascii=False), run_id))
                updated_runs += 1
        except Exception as e:
            print(f"  ⚠️ Error processing run {run_id}: {e}")
            
    print(f"  ✨ Updated {updated_runs} backup runs.")

    conn.commit()
    conn.close()
    print("🎉 Migration completed!")

if __name__ == "__main__":
    migrate()
