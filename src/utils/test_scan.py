import os
run_to_channels = {}
def scan_root(root_dir, ext):
    print(f"Scanning {root_dir}...")
    if not os.path.exists(root_dir): 
        print(f"Not exists: {root_dir}")
        return
    for folder_name in os.listdir(root_dir):
        f_path = os.path.join(root_dir, folder_name)
        if not os.path.isdir(f_path): continue
        for ch_name in os.listdir(f_path):
            ch_path = os.path.join(f_path, ch_name)
            if not os.path.isdir(ch_path): continue
            for fname in os.listdir(ch_path):
                print(f"Found file: {fname}")
                if fname.startswith('sync_') and fname.endswith(ext):
                    if ext == '.json':
                        lbl = fname.replace('sync_', '').replace('.json', '')
                    else:
                        part = fname[5:-3]
                        lbl = part.rsplit('_', 2)[0] if '_' in part else part
                    print(f"Targeting lbl: {lbl}")
                    if lbl not in run_to_channels: run_to_channels[lbl] = set()
                    run_to_channels[lbl].add((ch_name, folder_name))

scan_root(os.path.join('data', 'archived', 'logs'), '.json')
print(run_to_channels)
