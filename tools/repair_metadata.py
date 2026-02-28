import os
import json
import sqlite3
import shutil
import re
from datetime import datetime

# 配置路径
DB_PATH = 'data/copilot.db'
DATA_META_ROOT = 'data/metadata'
DOCS_META_ROOT = 'docs/metadata'

def safe_name(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

def repair():
    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库不存在: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. 获取所有已知的频道映射关系
    # {id: canonical_name}
    channel_map = {}
    rows = cursor.execute('SELECT chat_id, canonical_name FROM channel_names').fetchall()
    for cid, cname in rows:
        channel_map[cid] = cname

    print(f"📊 数据库中共有 {len(channel_map)} 个频道记录")

    # 2. 扫描 data/metadata 和 docs/metadata 下的所有文件
    meta_roots = [DATA_META_ROOT, DOCS_META_ROOT]
    
    for root in meta_roots:
        if not os.path.exists(root): continue
        print(f"🔍 正在扫描: {root}")
        
        for folder_name in os.listdir(root):
            if folder_name == "关注列表": continue
            folder_path = os.path.join(root, folder_name)
            if not os.path.isdir(folder_path): continue
            
            for fname in os.listdir(folder_path):
                ext = None
                if fname.lower().endswith('.json'): ext = '.json'
                elif fname.lower().endswith('.md'): ext = '.md'
                
                if not ext: continue
                
                file_path = os.path.join(folder_path, fname)
                
                # 尝试从 JSON 中提取真实 ID (如果是 MD，尝试找同名 JSON)
                chat_id = None
                try:
                    target_json = file_path
                    if ext == '.md':
                        # 尝试找同名的 .json
                        base = fname.rsplit('.', 1)[0]
                        target_json = os.path.join(DATA_META_ROOT, folder_name, f"{base}.json")
                    
                    if os.path.exists(target_json):
                        with open(target_json, 'r', encoding='utf-8') as f:
                            chat_id = json.load(f).get('id')
                except: pass

                if not chat_id: continue
                
                # 检查 ID 是否在数据库中
                if chat_id in channel_map:
                    canonical_name = channel_map[chat_id]
                    expected_fname = f"{safe_name(canonical_name)}{ext}"
                    
                    # 检查文件名是否需要修复
                    if fname != expected_fname:
                        new_path = os.path.join(folder_path, expected_fname)
                        if os.path.exists(new_path):
                            print(f"  ⚠️ [冲突] 目标已存在，跳过重命名: {fname} -> {expected_fname}")
                        else:
                            try:
                                os.rename(file_path, new_path)
                                print(f"  ✅ [修复文件名] {fname} -> {expected_fname}")
                                file_path = new_path # 更新路径点以便后续迁移
                                fname = expected_fname
                            except Exception as e:
                                print(f"  ❌ [重命名失败] {file_path}: {e}")

                else:
                    print(f"  ℹ️ [未匹配 ID] {fname} (ID: {chat_id}) 不在数据库中")

    conn.close()
    print("\n✨ 修复完成！建议运行一次 python src/sync_mode/update_docs.py 以同步文件夹分类。")

if __name__ == "__main__":
    repair()
