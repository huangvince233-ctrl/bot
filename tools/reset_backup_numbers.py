"""
v9.1 一键重置备份编号脚本
将旧版 B38 等备份全部重命名为 #B0 (主Bot管辖) 或 P#B0 (副Bot管辖)。
同时清理数据库中旧的 backup_runs 记录，让下次增量直接从 #B1 / P#B1 开始。
"""
import os
import re
import sys
import sqlite3

# 管辖范围定义
MAIN_BOT_FOLDERS = ['极品捆绑', '精品捆绑', '整理']
SUB_BOT_FOLDERS  = ['较少捆绑AV_实拍_定制', '约_较少捆绑中精品', '未分组', '已归档', '杂文件']

# 旧编号正则：匹配 #B1 ~ #B999 或 TEST-B1 等
OLD_LABEL_RE = re.compile(r'(#B\d+|TEST-B\d+)')

def rename_files_in_dir(base_dir, folder_group, new_label):
    """遍历 base_dir/folder_group/{频道名}/ 下所有文件，将旧编号替换为 new_label"""
    group_path = os.path.join(base_dir, folder_group)
    if not os.path.isdir(group_path):
        return 0
    
    count = 0
    for channel_name in os.listdir(group_path):
        channel_path = os.path.join(group_path, channel_name)
        if not os.path.isdir(channel_path):
            continue
        for fname in os.listdir(channel_path):
            if OLD_LABEL_RE.search(fname):
                new_fname = OLD_LABEL_RE.sub(new_label, fname)
                if new_fname != fname:
                    old_full = os.path.join(channel_path, fname)
                    new_full = os.path.join(channel_path, new_fname)
                    os.rename(old_full, new_full)
                    print(f"  ✅ {fname}  →  {new_fname}")
                    count += 1
    return count

def reset_database():
    """将 backup_runs 表中的旧记录全部清除，让编号从 0 重新开始"""
    db_path = os.path.join('data', 'copilot.db')
    if not os.path.exists(db_path):
        print("⚠️ 数据库不存在，跳过")
        return
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # 清空历史 backup_runs (编号将从 0 重新计数)
    c.execute("DELETE FROM backup_runs")
    
    # 清空历史 backup_offsets (断点将从头开始，但增量备份会自动检测本地文件)
    # 注意：不清空 backup_offsets，保留断点，这样增量备份能正确续接
    
    conn.commit()
    conn.close()
    print("✅ 数据库 backup_runs 已清空，下次备份将从 #B0 / P#B0 开始")

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_root)
    print(f"📁 工作目录: {os.getcwd()}\n")
    
    total = 0
    
    for base_dir in ['data/archived/backups', 'docs/archived/backups']:
        if not os.path.exists(base_dir):
            continue
        print(f"\n{'='*60}")
        print(f"📂 处理: {base_dir}")
        print(f"{'='*60}")
        
        # 主 Bot 管辖 → #B0
        for folder in MAIN_BOT_FOLDERS:
            print(f"\n🔵 主Bot [{folder}] → #B0")
            total += rename_files_in_dir(base_dir, folder, '#B0')
        
        # 副 Bot 管辖 → P#B0
        for folder in SUB_BOT_FOLDERS:
            print(f"\n🟠 副Bot [{folder}] → P#B0")
            total += rename_files_in_dir(base_dir, folder, 'P#B0')
        
        # bot_官频_私群_好友 特殊分组保留不动
        if os.path.isdir(os.path.join(base_dir, 'bot_官频_私群_好友')):
            print(f"\n⚪ 跳过 [bot_官频_私群_好友] (非管辖，保留原样)")
    
    print(f"\n{'='*60}")
    print(f"📊 总计重命名: {total} 个文件")
    print(f"{'='*60}")
    
    # 重置数据库
    print("\n🗄️ 正在重置数据库编号...")
    reset_database()
    
    print("\n🎉 完成！下次运行备份时：")
    print("   主Bot (tgporncopilot) → 从 #B1 开始")
    print("   副Bot (my_porn_private_bot) → 从 P#B1 开始")

if __name__ == '__main__':
    main()
