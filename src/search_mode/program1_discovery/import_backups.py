import os
import sys
import json
import sqlite3
import argparse
from datetime import datetime

# 工作流 Program 0 (Discovery Prep): 备份文件增量大面积入库
# 将本地 JSON 备份文件同步到 global_messages 数据库表，为后续打标与搜索提供底层池。

def classify_message(msg):
    """简易消息分类，与 sync.py 保持逻辑一致"""
    text = f"{msg.get('text', '')} {msg.get('caption', '')}".strip()
    media = msg.get('media')
    
    if media:
        if 'document' in media:
            mime = media.get('mime_type', '')
            if 'video' in mime: return 'video'
            if 'image' in mime: return 'photo'
            return 'file'
        if 'video' in media: return 'video'
        if 'photo' in media: return 'photo'
        if 'gif' in media: return 'gif'
        if 'web_preview' in media: return 'link_preview'
    
    # 检测文本中的链接
    urls = [w for w in text.split() if w.startswith('http')]
    if urls: return 'link'
    
    return 'text'

def import_backups(bot_name='tgporncopilot', db_path='data/copilot.db', backup_base='data/archived/backups'):
    from src.utils.config import get_bot_config
    config = get_bot_config(bot_name)
    managed_folders = config.get('managed_folders', [])
    
    print(f"🚀 开始增量入库模式 (Bot: {bot_name})")
    print(f"🎯 管辖范围: {managed_folders}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    total_added = 0
    total_skipped = 0
    
    for folder in managed_folders:
        folder_path = os.path.join(backup_base, folder)
        if not os.path.exists(folder_path):
            print(f"⚠️ 文件夹不存在，跳过: {folder_path}")
            continue
        
        print(f"📂 正在扫描文件夹: {folder}")
        for channel_dir in os.listdir(folder_path):
            channel_path = os.path.join(folder_path, channel_dir)
            if not os.path.isdir(channel_path): continue
            
            # 探测 Chat ID (支持 [id] 格式和 _id 格式)
            import re
            match = re.search(r'\[(-?\d+)\]', channel_dir)
            chat_id = None
            chat_name = channel_dir
            
            if match:
                chat_id = int(match.group(1))
                chat_name = channel_dir.split(' [')[0]
            elif '_' in channel_dir:
                # 尝试 _id 格式 (与 index_exporter 和 global_tagger 逻辑一致)
                parts = channel_dir.split('_')
                last_part = parts[-1]
                if last_part.isdigit():
                    if last_part.startswith("100") and len(last_part) > 10:
                        chat_id = int(f"-{last_part}")
                    else:
                        chat_id = int(f"-100{last_part}")
                    chat_name = "_".join(parts[:-1])
            
            # 如果以上都没匹配到，尝试反查数据库
            if not chat_id:
                row = cursor.execute(
                    'SELECT chat_id FROM channel_names WHERE canonical_name = ? OR latest_name = ?',
                    (channel_dir, channel_dir)
                ).fetchone()
                if row:
                    chat_id = row[0]
                    chat_name = channel_dir
            
            if not chat_id:
                continue

            # 查找 JSON 备份文件
            json_files = [f for f in os.listdir(channel_path) if f.endswith('.json') and not f.startswith('metadata')]
            if not json_files: continue
            
            for jf in json_files:
                jf_path = os.path.join(channel_path, jf)
                try:
                    with open(jf_path, 'r', encoding='utf-8') as f:
                        messages = json.load(f)
                        if not isinstance(messages, list): continue
                        
                        for msg in messages:
                            msg_id = msg.get('msg_id')
                            if not msg_id: continue
                            
                            m_type = msg.get('type') or classify_message(msg)
                            sender = msg.get('sender', 'Unknown')
                            otime = msg.get('original_time')
                            text = (msg.get('text', '') or '') + (msg.get('caption', '') or '')
                            fname = msg.get('file_name')
                            mg_id = msg.get('media_group_id')
                            
                            # 执行写入 (使用 INSERT OR IGNORE 以保护已有的打标数据)
                            cursor.execute('''
                                INSERT OR IGNORE INTO global_messages (
                                    chat_id, chat_name, msg_id, msg_type, 
                                    sender_name, original_time, text_content, file_name, media_group_id
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (chat_id, chat_name, msg_id, m_type, sender, otime, text, fname, mg_id))
                            
                            if cursor.rowcount > 0:
                                total_added += 1
                            else:
                                total_skipped += 1
                except Exception as e:
                    print(f"❌ 读取文件失败 {jf}: {e}")
                    
            print(f"  ✅ {chat_name}: 新增 {total_added} 条, 跳过 {total_skipped} 条 (已存在)")
            conn.commit()
            total_added = 0
            total_skipped = 0

    conn.close()
    print("🏁 全部入库操作完成。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bot', type=str, default='tgporncopilot')
    args = parser.parse_args()
    
    # 修正路径引用 (脚本位于 src/search_mode/program1_discovery/ 深度为 3)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    os.chdir(project_root)
    sys.path.append(project_root)
    
    import_backups(bot_name=args.bot)
