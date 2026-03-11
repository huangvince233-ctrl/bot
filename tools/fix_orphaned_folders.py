import os
import shutil
import re

# 1. 读入最新的频道分组映射
dialogs_file = "docs/metadata/关注列表/all_dialogs.txt"
if not os.path.exists(dialogs_file):
    print(f"Error: {dialogs_file} not found.")
    exit(1)

# channel_id -> folder_name
channel_to_folder = {}
with open(dialogs_file, "r", encoding="utf-8") as f:
    for line in f:
        # 例: [极品捆绑] 📢 25号宇宙 (-1001842629760)
        match = re.search(r"\[(.*?)\] (?:📢|👥|🤖|🛡️|💬|📍|👤) .*?\(([-]?\d+)\)", line)
        if match:
            folder_name = match.group(1).strip()
            raw_id = match.group(2).strip()
            abs_id_str = str(abs(int(raw_id)))             # 例如 1001927546930
            mod_id_str = str(abs(int(raw_id)) % 1000000000000) # 例如 1927546930
            
            # 同时注册两种格式，因为残留的文件夹名字这两种后缀都可能存在
            channel_to_folder[abs_id_str] = folder_name
            channel_to_folder[mod_id_str] = folder_name
            channel_to_folder[raw_id.replace('-', '')] = folder_name

def safe_name(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

dir_targets = [
    os.path.join('docs', 'archived', 'logs'),
    os.path.join('data', 'archived', 'logs'),
    os.path.join('docs', 'archived', 'backups'),
    os.path.join('data', 'archived', 'backups')
]

moved_count = 0
for base_dir in dir_targets:
    if not os.path.exists(base_dir): continue
    
    # 遍历所有分组文件夹 (如 极品捆绑, 较少捆绑AV_...)
    for current_folder in os.listdir(base_dir):
        current_folder_path = os.path.join(base_dir, current_folder)
        if not os.path.isdir(current_folder_path): continue
        
        # 遍历该分组下的所有频道文件夹 (如 sm.重口味_1001927546930)
        for channel_folder in os.listdir(current_folder_path):
            channel_folder_path = os.path.join(current_folder_path, channel_folder)
            if not os.path.isdir(channel_folder_path): continue
            
            # 提取名字中的 ID
            id_match = re.search(r'_(\d+)$', channel_folder)
            folder_has_id_suffix = False
            short_id = None
            
            if id_match:
                short_id = id_match.group(1)
                folder_has_id_suffix = True
                
            # [DEBUG] 
            # print(f"DEBUG Check - Folder: {channel_folder}, Found Short ID: {short_id}, Exact Math in map: {short_id in channel_to_folder}")
            
            # 判定目标分组：如果在列表中，去对应的目标；如果不在列表中，说明退群了，去「已归档」
            if folder_has_id_suffix:
                if short_id in channel_to_folder:
                    target_folder = channel_to_folder[short_id]
                else:
                    target_folder = "已归档"
                    
                safe_target = safe_name(target_folder)
                
                # 为了统一名称，新目录一律去掉旧的 ID 尾巴
                new_name = channel_folder[:channel_folder.rfind('_')]
                new_parent_dir = os.path.join(base_dir, safe_target)
                new_dir = os.path.join(new_parent_dir, new_name)
                
                # 如果所在的文件夹不对，或者名字还没被切掉尾巴，就执行移动或重命名
                if current_folder != safe_target or channel_folder != new_name:
                    os.makedirs(new_parent_dir, exist_ok=True)
                    
                    try:
                        if os.path.exists(new_dir) and new_dir != channel_folder_path:
                            print(f"🔄 Merging: {channel_folder_path} -> {new_dir}")
                            for item in os.listdir(channel_folder_path):
                                src_item = os.path.join(channel_folder_path, item)
                                dst_item = os.path.join(new_dir, item)
                                if os.path.exists(dst_item):
                                    if os.path.isdir(src_item):
                                        shutil.rmtree(src_item)
                                    else:
                                        os.remove(src_item)
                                else:
                                    shutil.move(src_item, dst_item)
                            os.rmdir(channel_folder_path)
                        else:
                            print(f"🚚 Moving/Renaming: {channel_folder_path} -> {new_dir}")
                            shutil.move(channel_folder_path, new_dir)
                        moved_count += 1
                    except Exception as e:
                        print(f"⚠️ Error moving {channel_folder_path} to {new_dir}: {e}")

print(f"✅ Cleanup finished. Total {moved_count} directories repaired.")
