#!/usr/bin/env python3
# 优化 search_bot.py: 在缓存时顺带记录 is_deleted，避免历史频道 O(N^2) 遍历

f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()

old = '''        # [优化] 提前建立本地元数据 ID 缓存，避免 O(N*M) 的文件扫描
        metadata_id_map = {} # {id_str: (name, folder)}
        meta_root = os.path.join('data', 'metadata')
        if os.path.exists(meta_root):
            for root, dirs, files in os.walk(meta_root):
                for f in files:
                    if f.endswith('.json'):
                        try:
                            with open(os.path.join(root, f), 'r', encoding='utf-8') as f_meta:
                                mj = json.load(f_meta)
                                mid = mj.get('id') or mj.get('chat_id')
                                if mid:
                                    metadata_id_map[str(mid)] = (mj.get('canonical_name', f[:-5]), os.path.basename(root))
                        except: pass'''

new = '''        # [优化] 提前建立本地元数据 ID 缓存，避免 O(N*M) 的文件扫描
        # metadata_id_map: {id_str: (name, folder)}
        # deleted_channels_map: {id_str: {name, folder, deleted_at}} -- is_deleted=true 的历史频道
        metadata_id_map = {}
        deleted_channels_map = {}
        meta_root = os.path.join('data', 'metadata')
        if os.path.exists(meta_root):
            for root, dirs, files in os.walk(meta_root):
                for f in files:
                    if f.endswith('.json'):
                        try:
                            with open(os.path.join(root, f), 'r', encoding='utf-8') as f_meta:
                                mj = json.load(f_meta)
                                mid = mj.get('id') or mj.get('chat_id')
                                if mid:
                                    mid_str = str(mid)
                                    name = mj.get('canonical_name', f[:-5])
                                    folder = os.path.basename(root)
                                    metadata_id_map[mid_str] = (name, folder)
                                    if mj.get('is_deleted'):
                                        deleted_channels_map[mid_str] = {
                                            'name': name,
                                            'folder': folder,
                                            'deleted_at': mj.get('deleted_at', '')[:10]
                                        }
                        except: pass'''

if old in content:
    content = content.replace(old, new, 1)
    open('src/search_bot.py', 'w', encoding='utf-8').write(content)
    print('OK - cache optimization done')
else:
    print('NOT FOUND')
    idx = content.find('提前建立本地元数据 ID 缓存')
    print(repr(content[idx-5:idx+400]))
