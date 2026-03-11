"""
update_docs.py - 统一文档更新脚本

功能：
1. 扫描 Telegram 文件夹结构 → 生成 docs/subscriptions.md
2. 读取数据库中已同步的消息 → 生成 docs/logs/{分组}/{频道名}.md

用法：python src/sync_mode/update_docs.py
"""

import os
import re
import json
import argparse
from dotenv import load_dotenv
import sqlite3
import asyncio
from datetime import datetime
import sys
import glob
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# 强制 Windows 控制台使用 UTF-8 编码，防止 emoji 导致 GBK 报错
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

from telethon import TelegramClient, functions, types
from db import Database

# 这里的 db 仅供脚本独立运行时使用，主程序调用时应传入 db 实例
db = None
load_dotenv()
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
DB_PATH = 'data/copilot.db'

# 全局文件缓存，减少 O(N) 循环中的 IO
_metadata_cache = {}

def get_metadata_cache(root_dir):
    if root_dir in _metadata_cache:
        return _metadata_cache[root_dir]
    
    cache = {}
    if not os.path.exists(root_dir):
        return cache
        
    for folder in os.listdir(root_dir):
        f_path = os.path.join(root_dir, folder)
        if os.path.isdir(f_path):
            cache[folder] = set(os.listdir(f_path))
    
    _metadata_cache[root_dir] = cache
    return cache

def invalidate_metadata_cache():
    global _metadata_cache
    _metadata_cache = {}

def safe_name(name):
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

def format_range(ids):
    if not ids: return ""
    mi, ma = min(ids), max(ids)
    return f"#{mi}" if mi == ma else f"#{mi}-#{ma}"

def enforce_metadata_paths(chat_id, target_folder, canonical_name):
    """
    [NEW/FIX] 强制统一元数据和归档文件/文件夹到正确的 target_folder 下。
    扫描预定义的根目录，如果发现 canonical_name 在错误的分类文件夹中，则移动它。
    由于旧有的逻辑如果崩溃或只移动了部分，会导致残留，因此本函数采用“全局搜捕”式的修补机制。
    返回移动过的原文件夹列表。
    """
    safe_target = safe_name(target_folder)
    safe_c = safe_name(canonical_name)
    moved_from_dirs = set()

    # 1. 处理文件 (JSON / MD)
    file_targets = [
        ('data', 'metadata', '.json'),
        ('docs', 'metadata', '.md')
    ]
    for root, sub, ext in file_targets:
        base_dir = os.path.join(root, sub)
        if not os.path.exists(base_dir): continue
        target_name = f"{safe_c}{ext}"
        
        for folder_name in os.listdir(base_dir):
            if folder_name == safe_target: continue # 已经在目标位置，跳过
            old_dir = os.path.join(base_dir, folder_name)
            if not os.path.isdir(old_dir): continue
            
            old_path = os.path.join(old_dir, target_name)
            if os.path.exists(old_path):
                new_dir = os.path.join(base_dir, safe_target)
                new_path = os.path.join(new_dir, target_name)
                os.makedirs(new_dir, exist_ok=True)
                try:
                    shutil.move(old_path, new_path)
                    print(f"  🚚 [Enforce File] {old_path} -> {new_path}")
                    moved_from_dirs.add(folder_name)
                    
                    if ext == '.json':
                        try:
                            with open(new_path, 'r', encoding='utf-8') as f_upd:
                                data_upd = json.load(f_upd)
                            data_upd['folder'] = target_folder
                            with open(new_path, 'w', encoding='utf-8') as f_upd:
                                json.dump(data_upd, f_upd, ensure_ascii=False, indent=4)
                        except Exception as e:
                            print(f"  ⚠️ [Update JSON Internal FAILED] {new_path}: {e}")
                except Exception as e:
                    print(f"  ⚠️ [Move File FAILED] {old_path}: {e}")

    # 2. 处理归档目录 (logs / backups)
    dir_targets = [
        ('docs', 'archived', 'logs'),
        ('data', 'archived', 'logs'),
        ('docs', 'archived', 'backups'),
        ('data', 'archived', 'backups')
    ]
    
    # 构建所有可能的老目录名匹配项
    norm_id = str(chat_id)
    if norm_id.startswith('-100'):
        short_id = norm_id[4:]
    elif norm_id.startswith('-'):
        short_id = norm_id[1:]
    else:
        short_id = norm_id
        
    possible_dir_names = [
        safe_c,
        f"{safe_c}_{norm_id}",
        f"{safe_c}_{short_id}",
        f"{safe_c}_{abs(int(chat_id)) % 1000000000000}" 
    ]
    
    for root, arch, sub in dir_targets:
        base_dir = os.path.join(root, arch, sub)
        if not os.path.exists(base_dir): continue
        
        for folder_name in os.listdir(base_dir):
            if folder_name == safe_target: continue
            
            # [FIX] 兼容旧格式：只要是这几种名字，都当作此频道的历史文件夹处理
            for cand_name in possible_dir_names:
                old_dir = os.path.join(base_dir, folder_name, cand_name)
                if os.path.exists(old_dir) and os.path.isdir(old_dir):
                    new_parent_dir = os.path.join(base_dir, safe_target)
                    new_dir = os.path.join(new_parent_dir, safe_c)  # 统一合并为标准新名
                    os.makedirs(new_parent_dir, exist_ok=True)
                    try:
                        if os.path.exists(new_dir):
                            print(f"  📂 [Dir Exists] Merging Old Format: {old_dir} -> {new_dir}")
                            for item in os.listdir(old_dir):
                                src_item = os.path.join(old_dir, item)
                                dst_item = os.path.join(new_dir, item)
                                if os.path.exists(dst_item):
                                    if os.path.isdir(src_item):
                                        shutil.rmtree(src_item)
                                    else:
                                        os.remove(src_item)
                                else:
                                    shutil.move(src_item, dst_item)
                            os.rmdir(old_dir)
                        else:
                            shutil.move(old_dir, new_dir)
                            print(f"  🚚 [Enforce Dir (Old Format)] {old_dir} -> {new_dir}")
                        moved_from_dirs.add(folder_name)
                    except Exception as e:
                        print(f"  ⚠️ [Move Dir FAILED] {old_dir}: {e}")
                    
    return list(moved_from_dirs)

def auto_organize_root():
    """
    [User Directed] 自动整理根目录。
    将分散在根目录下的诊断脚本 (*.py) 移至 tools/，
    将临时记录或日志 (*.log, *.txt) 移至 data/temp/。
    排除掉项目核心必需文件。
    """
    print("🧹 [Auto Organize] Scanning root directory for cleanup...")
    root = os.getcwd()
    tools_dir = os.path.join(root, 'tools')
    temp_dir = os.path.join(root, 'data', 'temp')
    os.makedirs(tools_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    # 核心黑名单：绝对不能移动的文件
    essential_files = {
        'search_bot.py', 'db.py', 'requirements.txt', 'README.md', '.env', 
        '.gitignore', 'start_bot.bat', 'LICENSE',
        'start_tgporncopilot.bat', 'start_my_porn_private_bot.bat'
    }

    # 1. 处理 Python 脚本
    for f in glob.glob("*.py"):
        if f in essential_files: continue
        # 排除路径深度 (只理清根目录的文件)
        if os.sep in f: continue
        
        src_path = os.path.join(root, f)
        dst_path = os.path.join(tools_dir, os.path.basename(f))
        try:
            if os.path.exists(dst_path): os.remove(dst_path)
            shutil.move(src_path, dst_path)
            print(f"  📂 [Organized Script] {f} -> tools/")
        except Exception as e:
            print(f"  ⚠️ [Organized Script FAILED] {f}: {e}")

    # 2. 处理 临时文本/日志
    for f in glob.glob("*.txt") + glob.glob("*.log"):
        if f in essential_files or f.lower() == 'requirements.txt': continue
        if os.sep in f: continue
        
        src_path = os.path.join(root, f)
        dst_path = os.path.join(temp_dir, os.path.basename(f))
        try:
            if os.path.exists(dst_path): os.remove(dst_path)
            shutil.move(src_path, dst_path)
            print(f"  📝 [Organized Log] {f} -> data/temp/")
        except Exception as e:
            print(f"  ⚠️ [Organized Log FAILED] {f}: {e}")

async def run_metadata_update(client=None, db_instance=None, only_prepare=False, bot_name=None):
    """
    提供给外部（如 search_bot.py）调用的核心接口
    """
    global db
    if db_instance:
        db = db_instance
    elif db is None:
        db = Database(DB_PATH)

    # [NEW] 整理根目录
    auto_organize_root()
    
    # 如果仅准备环境，则到此为止
    if only_prepare:
        print("✅ 环境准备与整理完成。")
        return

    should_disconnect = False
    if client is None:
        client = TelegramClient('data/sessions/copilot_user', API_ID, API_HASH)
        await client.start()
        should_disconnect = True

    try:
        print("📂 正在扫描 Telegram...")

        # ===== 1. 获取用户信息与对话 =====
        me = await client.get_me()
        my_id = me.id
        all_dialogs = {}
        
        # 加载封禁/黑名单列表
        banned_channels = {}
        banned_file = os.path.join('data', 'banned_channels.json')
        if os.path.exists(banned_file):
            try:
                with open(banned_file, 'r', encoding='utf-8') as f:
                    banned_channels = json.load(f)
            except: pass
        def get_dialog_info(dialog, archived=False):
            ent = dialog.entity
            dtype = type(ent).__name__
            if dtype not in ('Channel', 'Chat', 'User'):
                return None
            
            is_bot = getattr(ent, 'bot', False)
            # [NEW] 如果已退出频道或群组已停用，则不视为活跃对话，让清理逻辑将其转入历史存档
            if getattr(ent, 'left', False) or getattr(ent, 'deactivated', False):
                return None
                
            is_official = (dialog.id == 777000)
            is_self = (dialog.id == my_id)
            is_channel = (dtype == 'Channel' and getattr(ent, 'broadcast', False))
            is_group = (dtype == 'Channel' and getattr(ent, 'megagroup', False)) or dtype == 'Chat'
            is_contact = getattr(ent, 'contact', False) if dtype == 'User' else False
            
            name = dialog.name
            if is_self: name = "收藏夹 (Saved Messages)"
            
            return {
                "name": name, "id": dialog.id,
                "type": dtype, "is_bot": is_bot, "is_official": is_official,
                "is_self": is_self, "is_channel": is_channel, "is_group": is_group,
                "is_contact": is_contact, "archived": archived,
                "restricted": any(r.platform == 'all' and r.reason == 'terms' for r in getattr(ent, 'restriction_reason', [])) if getattr(ent, 'restriction_reason', None) else False,
                "partial_restricted": (
                    any(r.platform != 'all' for r in getattr(ent, 'restriction_reason', [])) or
                    any(r.platform == 'all' and r.reason != 'terms' for r in getattr(ent, 'restriction_reason', []))
                ) if getattr(ent, 'restriction_reason', None) else False,
                "restriction_reason": [
                    {"platform": r.platform, "reason": r.reason, "text": r.text} 
                    for r in getattr(ent, 'restriction_reason', [])
                ] if getattr(ent, 'restriction_reason', None) else []
            }

        async for dialog in client.iter_dialogs(archived=False):
            info = get_dialog_info(dialog, archived=False)
            if info: all_dialogs[dialog.id] = info
            
        async for dialog in client.iter_dialogs(archived=True):
            info = get_dialog_info(dialog, archived=True)
            if info: all_dialogs[dialog.id] = info
        # 不在文档脚本中排除目标仓库，让用户能在列表中看到它
        
        counts = {
            "channel": sum(1 for d in all_dialogs.values() if d["is_channel"]),
            "group": sum(1 for d in all_dialogs.values() if d["is_group"]),
            "bot": sum(1 for d in all_dialogs.values() if d["is_bot"]),
            "user": sum(1 for d in all_dialogs.values() if d["type"] == "User" and not d["is_bot"] and not d["is_official"] and not d["is_self"]),
            "official": sum(1 for d in all_dialogs.values() if d["is_official"]),
            "self": sum(1 for d in all_dialogs.values() if d["is_self"]),
            "contact": sum(1 for d in all_dialogs.values() if d["is_contact"]),
            "archived_only": sum(1 for d in all_dialogs.values() if d["archived"])
        }
        print(f"  扫描到 {len(all_dialogs)} 个项目 (📢:{counts['channel']} 👥:{counts['group']} 🤖:{counts['bot']} 🛡️:{counts['official']} 💾:{counts['self']})")

        # ===== 2. 获取文件夹 =====
        folders = {}  # folder_title -> [dialog_ids]
        folder_assigned = set()
        folder_of_dialog = {}  # dialog_id -> folder_name

        # 构建辅助查找表: entity_id -> dialog_id
        # Telethon dialog.id 已经是完整的负数 ID (如 -1003023798330)
        # 但 folder peer 给的是 channel_id (如 3023798330)
        # 所以需要 -100{channel_id} -> dialog.id 的映射
        entity_to_dialog = {}
        for did, info in all_dialogs.items():
            # dialog.id 本身
            entity_to_dialog[did] = did
            # 尝试提取 entity_id 并建立映射
            # 对于 Channel: dialog.id = -100{channel_id}
            did_str = str(did)
            if did_str.startswith('-100'):
                raw_id = int(did_str[4:])  # 去掉 -100 前缀
                entity_to_dialog[raw_id] = did

        filters = await client(functions.messages.GetDialogFiltersRequest())
        if not isinstance(filters, list):
            filters = filters.filters if hasattr(filters, 'filters') else [filters]

        for f in filters:
            title = None
            if hasattr(f, 'title'):
                t = f.title
                title = t.text if hasattr(t, 'text') else str(t)
            if not title or not hasattr(f, 'include_peers'):
                continue

            peer_ids = []
            for peer in f.include_peers:
                try:
                    # 使用 get_entity 自动处理所有 Peer 类型 (User, Chat, Channel)
                    ent = await client.get_entity(peer)
                    # [FIX] 必须使用 telethon.utils.get_peer_id 以获得带 -100 前缀的 Peer ID，否则无法匹配 all_dialogs
                    from telethon import utils as telethon_utils
                    did = telethon_utils.get_peer_id(ent)
                    
                    # 如果全量列表里没有（例如已经退出频道），即便文件夹里有，也不强行补全
                    # 这样可以解决用户提到的“已删除频道仍然出现在列表”的问题
                    if did in all_dialogs:
                        peer_ids.append(did)
                        folder_assigned.add(did)
                        folder_of_dialog[did] = title
                except Exception as ent_e:
                    pass
            if peer_ids:
                folders[title] = peer_ids


        ungrouped = [d for d, info in all_dialogs.items() if not info["archived"] and d not in folder_assigned and (info["is_channel"] or info["is_group"])]
        archived_list = [d for d, info in all_dialogs.items() if info["archived"] and d not in folder_assigned and (info["is_channel"] or info["is_group"])]

        for did in ungrouped:
            folder_of_dialog[did] = "未分组"
        for did in archived_list:
            folder_of_dialog[did] = "已归档"

        # ===== 3. 生成 docs/subscriptions.md =====
        lines = [
            "# 📺 我的关注列表 (Subscriptions)",
            "",
            f"> 📢 频道: **{counts['channel']}** | 👥 群组: **{counts['group']}** | 🤖 机器人: **{counts['bot']}** | 💾 收藏: **{counts['self']}** | 👤 好友: **{counts['contact']}** | 🛡️ 官号: **{counts['official']}**",
            f"> 更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
        ]
        for folder_name, ids in folders.items():
            lines.append(f"## 📁 {folder_name} ({len(ids)})")
            lines.append("")
            for did in ids:
                info = all_dialogs[did]
                if info.get("is_self"): tag = "💾"
                elif info.get("is_official") or "私密视频库" in info["name"]: tag = "🛡️"
                elif info.get("is_bot"): tag = "🤖"
                elif info.get("is_channel"): tag = "📢"
                elif info.get("is_group"): tag = "👥"
                elif info.get("is_contact"): tag = "👤"
                else: tag = "💬"
                
                a = " `[archived]`" if info["archived"] else ""
                b = ""
                if str(did) in banned_channels:
                    b = " 🚷 `[已封禁]`"
                elif info.get("restricted"):
                    b = " 🛑 `[全平台封禁]`"
                elif info.get("partial_restricted"):
                    b = " ⚠️ `[局部受限]`"
                
                lines.append(f"  - {tag} **{info['name']}** `{info['id']}`{a}{b}")
            lines.append("")

        if ungrouped:
            lines.append(f"## 📋 未分组 ({len(ungrouped)})")
            lines.append("")
            for did in ungrouped:
                info = all_dialogs[did]
                tag = "📢" if info["type"] == "Channel" else "👥"
                b = " 🚷 `[已封禁]`" if str(did) in banned_channels else ""
                lines.append(f"  - {tag} **{info['name']}** `{info['id']}`{b}")
            lines.append("")

        if archived_list:
            lines.append(f"## 🗄️ 已归档 ({len(archived_list)})")
            lines.append("")
            for did in archived_list:
                info = all_dialogs[did]
                tag = "📢" if info["type"] == "Channel" else ("👥" if info["type"] == "Chat" else ("🤖" if info.get("is_bot") else "👤"))
                b = ""
                if str(did) in banned_channels:
                    b = " 🚷 `[已封禁]`"
                elif info.get("restricted"):
                    b = " 🛑 `[全平台封禁]`"
                elif info.get("partial_restricted"):
                    b = " ⚠️ `[局部受限]`"
                lines.append(f"  - {tag} **{info['name']}** `{info['id']}`{b}")
            lines.append("")

        # ===== 3b. 移除了失联/封禁频道列表 (根据用户要求) =====
        banned_count = 0
        # 如果需要统计数量可以保留逻辑，但不再写入 lines

        # ===== 3c. 新增：全量列表 (带有分组标签) =====
        lines.append(f"## 🔗 所有在线对话列表 ({len(all_dialogs)})")
        lines.append("")
        txt_lines = [f"Telegram 对话全量列表 (更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M')})", ""]
        
        sorted_all = sorted(all_dialogs.values(), key=lambda x: x['name'].lower())
        for info in sorted_all:
            did = info['id']
            if info.get("is_official") or "私密视频库" in info["name"]: tag = "🛡️"
            elif info.get("is_bot"): tag = "🤖"
            elif info.get("is_channel"): tag = "📢"
            elif info.get("is_group"): tag = "👥"
            else: tag = "💬"
            
            # 获取所在的文件夹名称
            folder_label = folder_of_dialog.get(did, "未分组")
            if info["archived"] and folder_label == "未分组":
                folder_label = "已归档"
            
            b = ""
            if str(did) in banned_channels:
                b = " 🚷 `[已封禁]`"
            elif info.get("restricted"):
                b = " 🛑 `[全平台封禁]`"
            elif info.get("partial_restricted"):
                b = " ⚠️ `[局部受限]`"
                
            lines.append(f"  - {tag} **{info['name']}** `{did}` 🏷️ `[{folder_label}]`{b}")
            txt_lines.append(f"[{folder_label}] {tag} {info['name']} ({did})")
        lines.append("")

        lines += ["---", "### 图例", "- 📢 频道 | 👥 群组 | 💬 私聊 | 👤 好友 | 🤖 机器人 | 🛡️ 官方 | 💾 收藏夹 | 🗄️ 归档"]

        meta_dir = os.path.join('docs', 'metadata', '关注列表')
        os.makedirs(meta_dir, exist_ok=True)
        
        with open(os.path.join(meta_dir, 'subscriptions.md'), 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
            
        # ===== 3c. 新增：全局元数据建档 (Metadata Archive) =====
        print("💾 正在建立全局频道/群组档案并锁定规范名称...")
        
        # 加载封禁/黑名单列表 (already loaded above)

        # 获取数据库中的所有同步/角色/备份时间戳
        sync_offsets = db.get_all_sync_offsets()
        backup_offsets = db.get_all_backup_offsets()

        # 跟踪本次更新中活跃的文件，用于后续清理
        active_json_files = set()
        active_md_files = set()
        
        # 本次更新的变动汇总
        report_moves = []   # (name, from, to)
        report_new = []     # (name, folder)
        report_status = []  # (name, old_status, new_status)

        for did, info in all_dialogs.items():
            folder_label = folder_of_dialog.get(did, "未分组")
            if info["archived"] and folder_label == "未分组":
                folder_label = "已归档"
                
            current_title = info['name']
            
            # 存入数据库，锁定或更新 Canonical Name，这会自动修复任何 Telegram 上的改名
            old_title, canonical_title = db.check_and_update_channel_name(did, current_title)
            
            # [Smart Move / Self-Healing Enforce]
            # 采用全新自愈逻辑：不再基于单一的 JSON 所在目录判定分组。
            # 直接扫描所有归档库和元数据目录，发现残留和错置的记录，一律暴力合并与迁移到当前目标文件夹 label 之内。
            moved_from_list = enforce_metadata_paths(did, folder_label, canonical_title)
            for old_fold in moved_from_list:
                report_moves.append((canonical_title, old_fold, folder_label))

            safe_f_name = safe_name(folder_label)
            safe_c_name = safe_name(canonical_title)
            
            data_meta_dir = os.path.join('data', 'metadata', safe_f_name)
            docs_meta_dir = os.path.join('docs', 'metadata', safe_f_name)
            os.makedirs(data_meta_dir, exist_ok=True)
            os.makedirs(docs_meta_dir, exist_ok=True)

            # 获取旧状态用于对比
            safe_f_name = safe_name(folder_label)
            safe_c_name = safe_name(canonical_title)
            json_path = os.path.join('data', 'metadata', safe_f_name, f"{safe_c_name}.json")
            
            old_status_text = None
            if os.path.exists(json_path):
                try:
                    with open(json_path, 'r', encoding='utf-8') as f_old:
                        old_js = json.load(f_old)
                        if old_js.get('is_banned'): old_status_text = "🚷 数据库标黑"
                        elif old_js.get('is_restricted'): old_status_text = "🛑 全平台封禁"
                        elif old_js.get('is_partial_restricted'): old_status_text = "⚠️ 局部受限"
                        else: old_status_text = "✅ 正常"
                except: pass
            else:
                report_new.append((canonical_title, folder_label))

            # 获取状态和时间戳
            did_str = str(did)
            is_banned = did_str in banned_channels
            is_global_restricted = info.get('restricted', False)
            is_partial_restricted = info.get('partial_restricted', False)
            
            # [NEW] 优先从 backup_offsets 获取精确的最后备份时间
            last_sync = sync_offsets.get(did) or "-"
            last_backup = backup_offsets.get(did) or "-"
            
            # 如果 offset 里没有，尝试从 backup_runs 的全量汇总里找 (作为兜底)
            if last_backup == "-":
                row_last = db.get_latest_backup_info(did)
                if row_last and row_last.get('time'):
                    last_backup = row_last['time'][:16].replace('T', ' ')
            
            if is_banned:
                status_text = "🚷 数据库标黑"
            elif is_global_restricted:
                status_text = "🛑 全平台封禁"
            elif is_partial_restricted:
                status_text = "⚠️ 局部受限"
            else:
                status_text = "✅ 正常"
            
            # 记录状态变化 (新标记出的违规)
            if old_status_text and old_status_text != status_text:
                if status_text != "✅ 正常": # 只汇报变糟糕的情况
                    report_status.append((canonical_title, old_status_text, status_text))

            meta_json = {
                "id": did,
                "canonical_name": canonical_title,
                "latest_name": current_title,
                "type": info['type'],
                "folder": folder_label,
                "is_bot": info.get('is_bot', False),
                "is_official": info.get('is_official', False),
                "is_archived": info.get('archived', False),
                "is_banned": is_banned,
                "is_restricted": is_global_restricted,
                "is_partial_restricted": is_partial_restricted,
                "restriction_reasons": info.get('restriction_reason', []),
                "last_sync_time": last_sync,
                "last_backup_time": last_backup,
                "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            json_path = os.path.join(data_meta_dir, f"{safe_c_name}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(meta_json, f, ensure_ascii=False, indent=2)
                
            md_path = os.path.join(docs_meta_dir, f"{safe_c_name}.md")
            
            status_full_text = status_text
            if is_global_restricted or is_partial_restricted:
                reasons = info.get('restriction_reason', [])
                reason_detail = ", ".join([f"{r['platform']}:{r['reason']}" for r in reasons]) if reasons else "未知原因"
                status_full_text += f" ({reason_detail})"
            elif not is_banned:
                status_full_text += " (Online)"
            else:
                status_full_text += " (Banned)"
            
            md_content = f"""# {canonical_title} - 频道档案

## 基本信息
- **内部 ID**: `{did}`
- **当前状态**: {status_full_text}
- **规范名称**: {canonical_title}
- **最新名称**: {current_title}
- **实体类型**: {info['type']}
- **所属分组**: {folder_label}
- **归档状态**: {"是" if info.get('archived') else "否"}

## 📅 运行时间线
- **最后同步时间**: `{last_sync}`
- **最后备份时间**: `{last_backup}`

> ⚠️ 自动维护的全局映射档案
> 更新时间: {meta_json['updated_at']}
"""
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            
            active_json_files.add(os.path.abspath(json_path))
            active_md_files.add(os.path.abspath(md_path))
                
        print(f"✅ 全局频道档案已更新至 data/metadata 和 docs/metadata 目录")

        # ===== 3d. 新增：智能清理已删除/退出的频道元数据 (保留有历史的存档) =====
        print("🧹 正在检查并清理失效的元数据文件...")
        
        # 预先整理出所有有历史记录的 ID
        has_history_ids = set()
        for did in sync_offsets.keys(): has_history_ids.add(did)
        for did in backup_offsets.keys(): has_history_ids.add(did)

        def cleanup_orphaned_files(root_dir, active_set, extension):
            if not os.path.exists(root_dir): return [], []
            deleted_names = []
            preserved_names = []
            
            # 使用缓存进行遍历
            meta_cache = get_metadata_cache(root_dir)
            
            for folder_name, files in meta_cache.items():
                if folder_name == "关注列表": continue
                f_path = os.path.join(root_dir, folder_name)
                
                for fname in files:
                    if fname.endswith(extension):
                        full_path = os.path.abspath(os.path.join(f_path, fname))
                        name_without_ext = fname.rsplit('.', 1)[0]
                        if full_path not in active_set:
                            # 尝试获取该文件的 ID
                            file_id = None
                            try:
                                if extension == '.json':
                                    with open(full_path, 'r', encoding='utf-8') as f_tmp:
                                        file_id = json.load(f_tmp).get('id')
                                elif extension == '.md':
                                    # MD 文件通常和 JSON 成对，找对应的 JSON 获取 ID
                                    json_v = os.path.join('data', 'metadata', folder_name, f"{name_without_ext}.json")
                                    if os.path.exists(json_v):
                                        with open(json_v, 'r', encoding='utf-8') as f_tmp:
                                            file_id = json.load(f_tmp).get('id')
                            except: pass

                            if file_id and file_id in has_history_ids:
                                preserved_names.append(name_without_ext)
                                # [NEW] 标记为历史频道 (is_deleted=true)，供 Bot UI 虚拟分组使用
                                if extension == '.json':
                                    try:
                                        with open(full_path, 'r', encoding='utf-8') as f_tmp:
                                            jdata = json.load(f_tmp)
                                        if not jdata.get('is_deleted'):
                                            jdata['is_deleted'] = True
                                            jdata['deleted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                            with open(full_path, 'w', encoding='utf-8') as f_tmp:
                                                json.dump(jdata, f_tmp, ensure_ascii=False, indent=2)
                                    except: pass
                                continue
                            
                            # 确实没用且没历史，删
                            try:
                                os.remove(full_path)
                                deleted_names.append(name_without_ext)
                            except Exception as e:
                                print(f"⚠️ [Cleanup FAILED] {full_path}: {e}")
                
                # 如果文件夹彻底空了，删掉
                try:
                    if not os.listdir(f_path):
                        os.rmdir(f_path)
                except: pass
            return deleted_names, preserved_names

        json_deleted_names, json_preserved_names = cleanup_orphaned_files('data/metadata', active_json_files, '.json')
        md_deleted_names, md_preserved_names = cleanup_orphaned_files('docs/metadata', active_md_files, '.md')
        
        # 取 JSON 的结果作为主计数（因为通常成对）
        final_deleted = sorted(list(set(json_deleted_names)))
        final_preserved = sorted(list(set(json_preserved_names)))

        if final_deleted or final_preserved:
            msg = f"🧹 清理报告：\n   - 删除完全无效档案: {len(final_deleted)} 个\n"
            if final_preserved:
                msg += f"   - 历史记录保护 (不予清理): {len(final_preserved)} 个\n"
            print(msg)
            
        # 同时保存一份纯文本版，方便快速查阅
        with open(os.path.join(meta_dir, 'all_dialogs.txt'), 'w', encoding='utf-8') as f:
            f.write("\n".join(txt_lines))
            
        print(f"✅ {meta_dir}/subscriptions.md 已更新")
        print(f"✅ {meta_dir}/all_dialogs.txt 已更新")

        # 返回结果方便 Bot 汇报
        return {
            "deleted_count": len(final_deleted),
            "deleted_names": final_deleted,
            "preserved_count": len(final_preserved),
            "preserved_names": final_preserved,
            "new_channels": report_new,    # list of (name, folder)
            "moves": report_moves,          # list of (name, from, to)
            "status_changes": report_status # list of (name, old, new)
        }

        # ===== 4. 生成 docs/logs/{分组}/{频道名}.md =====
        # 使用传入的 db 实例的连接
        conn = db.conn
        try:
            rows = conn.execute(
                'SELECT name FROM sqlite_master WHERE type="table" AND name="messages"'
            ).fetchone()
        except Exception:
            rows = None
        
        if not rows:
            print("⚠️ messages 表不存在，跳过存档文档生成。")
            conn.close()
            return

        # 1. 获取所有同步任务
        runs = conn.execute('SELECT * FROM sync_runs ORDER BY run_id ASC').fetchall()
        col_names = [description[0] for description in conn.execute('SELECT * FROM sync_runs LIMIT 1').description]
        
        # 2. 建立目录扫描映射 (run_label -> set((channel_name, folder_name)))
        run_to_channels = {}
        
        def scan_root(root_dir, ext):
            if not os.path.exists(root_dir): return
            for folder_name in os.listdir(root_dir):
                f_path = os.path.join(root_dir, folder_name)
                if not os.path.isdir(f_path): continue
                for ch_name in os.listdir(f_path):
                    ch_path = os.path.join(f_path, ch_name)
                    if not os.path.isdir(ch_path): continue
                    for fname in os.listdir(ch_path):
                        if fname.startswith('sync_') and fname.endswith(ext):
                            if ext == '.json':
                                lbl = fname.replace('sync_', '').replace('.json', '')
                            else:
                                part = fname[5:-3] # remove 'sync_' and '.md'
                                lbl = part.rsplit('_', 2)[0] if '_' in part else part
                            
                            if lbl not in run_to_channels: run_to_channels[lbl] = set()
                            run_to_channels[lbl].add((ch_name, folder_name))
                            
                            # 兼容 safe_name 把 #1 变成 1 的情况
                            if lbl.isdigit():
                                hash_lbl = f"#{lbl}"
                                if hash_lbl not in run_to_channels: run_to_channels[hash_lbl] = set()
                                run_to_channels[hash_lbl].add((ch_name, folder_name))

        scan_root(os.path.join('data', 'archived', 'logs'), '.json')
        scan_root(os.path.join('docs', 'archived', 'logs'), '.md')

        # 3. 循环任务并生成文档
        archive_count = 0
        print(f"DEBUG: Found {len(runs)} runs in DB. channels mapping: {run_to_channels}")
        for run_row in runs:
            run = dict(zip(col_names, run_row))
            run_id = run['run_id']
            is_test = run['is_test']
            start_time_iso = run['start_time'] or datetime.now().isoformat()
            
            # 计算 Run Label
            if is_test:
                count_res = conn.execute('SELECT COUNT(*) FROM sync_runs WHERE is_test = 1 AND run_id <= ?', (run_id,)).fetchone()
                count = count_res[0] if count_res else 1
                run_label = f"TEST-{max(1, count)}"
            else:
                run_label = f"#{run['formal_number']}"
            
            # 获取命中的频道
            targeted = run_to_channels.get(run_label, [])
            print(f"DEBUG: Run {run_id} ({run_label}): Targeted channels: {targeted}")
            if not targeted: continue
            
            for channel_name, folder in targeted:
                db_msgs = conn.execute('''
                    SELECT msg_type, original_msg_id, original_chat_name, sender_name,
                           original_time, forwarded_time, text_content, creator, 
                           group_index, file_name, res_id, res_photo_id, 
                           res_video_id, res_gif_id, res_link_id, res_link_msg_id,
                           res_preview_id, res_other_id, res_text_id, res_msg_id
                    FROM messages 
                    WHERE sync_run_id = ? AND original_chat_name = ?
                    ORDER BY group_index ASC, id ASC
                ''', (run_id, channel_name)).fetchall()
                
                msgs = []
                for m in db_msgs:
                    msgs.append({
                        "type": m[0], "msg_id": m[1], "channel": m[2], "sender": m[3],
                        "original_time": m[4], "forwarded_time": m[5], "text": m[6],
                        "creator": m[7], "group": m[8], "file_name": m[9], "res_id": m[10],
                        "res_photo_id": m[11], "res_video_id": m[12], "res_gif_id": m[13],
                        "res_link_id": m[14], "res_link_msg_id": m[15],
                        "res_preview_id": m[16], "res_other_id": m[17], "res_text_id": m[18],
                        "res_msg_id": m[19]
                    })

                dir_path = os.path.join('docs', 'archived', 'logs', safe_name(folder), safe_name(channel_name))
                os.makedirs(dir_path, exist_ok=True)
                
                # [FIX]: Clean up any existing duplicate .md files for this specific run_label
                # to prevent file piling when start_time/time_label mismatches or changes.
                import glob
                safe_run_label = safe_name(run_label)
                existing_pattern = os.path.join(dir_path, f"sync_{safe_run_label}_*.md")
                for old_f in glob.glob(existing_pattern):
                    try: os.remove(old_f)
                    except: pass
                
                try:
                    time_label = datetime.fromisoformat(start_time_iso).strftime("%Y%m%d_%H%M%S")
                except:
                    time_label = "unknown"
                
                file_path = os.path.join(dir_path, f"sync_{safe_run_label}_{time_label}.md")
                v_cnt = sum(1 for m in msgs if m['type'] == 'video')
                p_cnt = sum(1 for m in msgs if m['type'] == 'photo')
                f_cnt = sum(1 for m in msgs if m['type'] == 'file')
                g_cnt = sum(1 for m in msgs if m['type'] == 'gif')
                l_cnt_total = 0
                for m in msgs:
                    link_id_val = m.get('res_link_id')
                    if link_id_val:
                        if '-' in str(link_id_val):
                            try:
                                start, end = map(int, str(link_id_val).split('-'))
                                l_cnt_total += (end - start + 1)
                            except:
                                l_cnt_total += 1
                        else:
                            l_cnt_total += 1
                
                pv_cnt = sum(1 for m in msgs if m['type'] == 'link_preview')
                t_cnt = sum(1 for m in msgs if m['type'] == 'text')
                l_cnt = sum(1 for m in msgs if m['type'] == 'link')
                lm_cnt = sum(1 for m in msgs if m.get('res_link_msg_id'))

                num_total_groups = len(set(m['group'] for m in msgs if m['group'] is not None))
                num_media_groups = len(set(m['group'] for m in msgs if m['type'] in ['video', 'photo', 'gif', 'file']))
                num_text_msgs = t_cnt + l_cnt

                def get_range(key):
                    ids = []
                    for m in msgs:
                        val = m.get(key)
                        if val:
                            if '-' in str(val):
                                try:
                                    s, e = map(int, str(val).split('-'))
                                    ids.extend(list(range(s, e+1)))
                                except:
                                    pass
                            else:
                                try:
                                    ids.append(int(val))
                                except:
                                    pass
                    if not ids: return "-"
                    if len(ids) == 1: return f"#{ids[0]}"
                    mi, ma = min(ids), max(ids)
                    if len(ids) == (ma - mi + 1):
                        return f"#{mi}-#{ma}"
                    return f"#{mi}-#{ma} (共{len(ids)}项)"

                r_all = get_range('res_id')
                r_vid = get_range('res_video_id')
                r_pho = get_range('res_photo_id')
                r_gif = get_range('res_gif_id')
                r_file = get_range('res_other_id')
                r_prv = get_range('res_preview_id')
                r_txt = get_range('res_text_id')
                r_lnk = get_range('res_link_id')
                r_lmk = get_range('res_link_msg_id')
                r_msg = get_range('res_msg_id')

                ICONS = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️", "link": "🔗", "link_preview": "👁‍🗨️", "text": "✍️"}

                md = [
                    f"# {channel_name} - 同步报告 {run_label}",
                    f"",
                    f"### 📊 频道本轮统计汇总",
                    f"- **总消息数量**: {num_total_groups}",
                    f"- **带资源消息**: {num_media_groups + pv_cnt} ({num_media_groups}组 + {pv_cnt}预览)",
                    f"- **文本消息数量**: {num_text_msgs}",
                    f"- **资源总量**: {v_cnt+p_cnt+g_cnt+pv_cnt+f_cnt} (🎬:{v_cnt} | 🖼️:{p_cnt} | 🎞️:{g_cnt} | 👁‍🗨️:{pv_cnt} | 📄:{f_cnt})",
                    f"- **链接总数**: {l_cnt_total} 🔗",
                    f"- **携带链接消息**: {lm_cnt} 📎",
                    f"",
                    f"### 🔢 编号概览",
                    f"📋 **对话资源号范围 (Resource IDs)**:",
                    f"- **总编号**: `{r_all}`",
                    f"- **📦 带资源消息编号范围**: `{r_msg}`",
                    f"- **🎬 视频号**: `{r_vid}`",
                    f"- **🖼️ 图片号**: `{r_pho}`",
                    f"- **🎞️ GIF号**: `{r_gif}`",
                    f"- **📄 文件号**: `{r_file}`",
                    f"- **👁‍🗨️ 可预览链接号**: `{r_prv}`",
                    f"- **🔗 链接号**: `{r_lnk}`",
                    f"- **📎 带链接消息号**: `{r_lmk}`",
                    f"- **✍️ 文字号**: `{r_txt}`",
                    f"",
                    f"> 📍 来源分组: `{folder}` | 🕒 同步时间: {start_time_iso.split('.')[0].replace('T', ' ')}",
                    "",
                    "---",
                    "",
                ]

                if not msgs:
                    md.append("\n> [!NOTE]\n> 本次同步该频道未发现新消息。")
                else:
                    md.append("### 📜 消息列表")
                    
                    from collections import defaultdict
                    groups = defaultdict(list)
                    for m in msgs:
                        groups[m['group']].append(m)
                    
                    for g_idx, g_msgs in groups.items():
                        def get_g_range(key):
                            ids = []
                            for m in g_msgs:
                                val = m.get(key)
                                if val:
                                    if '-' in str(val):
                                        try:
                                            s, e = map(int, str(val).split('-'))
                                            ids.extend(list(range(s, e+1)))
                                        except: pass
                                    else:
                                        try: ids.append(int(val))
                                        except: pass
                            if not ids: return None
                            unique_ids = sorted(list(set(ids)))
                            if len(unique_ids) == 1: return f"#{unique_ids[0]}"
                            mi, ma = unique_ids[0], unique_ids[-1]
                            if len(unique_ids) == (ma - mi + 1):
                                return f"#{mi}-#{ma}"
                            return f"#{mi}-#{ma}"

                        g_r_msg = get_g_range('res_msg_id')
                        g_r_all = get_g_range('res_id')
                        g_r_vid = get_g_range('res_video_id')
                        g_r_pho = get_g_range('res_photo_id')
                        g_r_gif = get_g_range('res_gif_id')
                        g_r_file = get_g_range('res_other_id')
                        g_r_prv = get_g_range('res_preview_id')
                        g_r_lnk = get_g_range('res_link_id')
                        g_r_lmk = get_g_range('res_link_msg_id')
                        g_r_txt = get_g_range('res_text_id')
                        
                        num_parts = []
                        if g_r_msg: num_parts.append(f"📦 资源: `{g_r_msg}`")
                        if g_r_txt: num_parts.append(f"✍️ 文字: `{g_r_txt}`")
                        if g_r_lmk: num_parts.append(f"📎 带链接消息号: `{g_r_lmk}`")
                        if g_r_all: num_parts.append(f"🔢 总资源号: `{g_r_all}`")
                        if g_r_vid: num_parts.append(f"🎬 视频: `{g_r_vid}`")
                        if g_r_pho: num_parts.append(f"🖼️ 图片: `{g_r_pho}`")
                        if g_r_gif: num_parts.append(f"🎞️ GIF: `{g_r_gif}`")
                        if g_r_file: num_parts.append(f"📄 文件: `{g_r_file}`")
                        if g_r_prv: num_parts.append(f"👁‍🗨️ 可预览链接号: `{g_r_prv}`")
                        if g_r_lnk: num_parts.append(f"🔗 链接号: `{g_r_lnk}`")
                        
                        num_header = " | ".join(num_parts)
                        
                        md.append(f"#### 📦 第 {g_idx} 组消息 | {num_header}\n")
                        
                        # 1. 该组统一的来源信息和文本部分
                        group_creator = next((m['creator'] for m in g_msgs if m['creator'] and m['creator'] != "Unknown"), None)
                        group_text = next((m['text'] for m in g_msgs if m['text']), None)
                        
                        if group_creator:
                            md.append(f"- **发布源**: {group_creator}")
                            
                        if group_text:
                            md.append("")
                            clean_text = group_text[:500] + "..." if len(group_text) > 500 else group_text
                            for line in clean_text.split('\n'):
                                md.append(f"> {line}" if line.strip() else ">")
                            md.append("")
                        
                        for m in g_msgs:
                            time_str = m['original_time'][:16].replace('T', ' ') if m['original_time'] else "N/A"
                            icon = ICONS.get(m['type'], "✍️")
                            sender = m['sender'] or 'System'
                            
                            # 获取子编号
                            t = m['type']
                            key_map = {'video': ('res_video_id', '视频'), 'photo': ('res_photo_id', '图片'), 'gif': ('res_gif_id', 'GIF'), 'file': ('res_other_id', '文件'), 'link': ('res_link_id', '链接'), 'link_preview': ('res_preview_id', '预览链接'), 'text': ('res_text_id', '文本')}
                            sub_id_str = ""
                            if t in key_map:
                                db_key, label_name = key_map[t]
                                val = m.get(db_key)
                                if val:
                                    if '-' in str(val):
                                        sub_id_str = f" {label_name} #{val}"
                                    else:
                                        sub_id_str = f" {label_name} #{val}"
                                        
                            r_id = f"#{m.get('res_id', '')}" if m.get('res_id') else f"ID:{m.get('msg_id', '?')}"
                            
                            # 2. 消息头
                            md.append(f"**{icon} ({time_str}) - {sender}{sub_id_str} | 总: {r_id}**\n")
                            
                            # 3. 文件详情
                            if m['file_name']: 
                                md.append(f"- **文件名**: `{m['file_name']}`\n")
                
                content = "\n".join(md)
                
                # docs 目錄
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                # data 目錄同步
                data_dir_path = os.path.join('data', 'archived', 'logs', safe_name(folder), safe_name(channel_name))
                os.makedirs(data_dir_path, exist_ok=True)
                
                data_existing_pattern = os.path.join(data_dir_path, f"sync_{safe_run_label}_*.md")
                for old_f in glob.glob(data_existing_pattern):
                    try: os.remove(old_f)
                    except: pass
                
                data_file_path = os.path.join(data_dir_path, f"sync_{safe_run_label}_{time_label}.md")
                with open(data_file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                    
                archive_count += 1

        # [REMOVED] conn.close() - Using shared connection from db_instance
        print(f"✅ docs/logs/ 已更新 ({archive_count} 个同步记录)")
    except Exception as e:
        import traceback
        print(f"❌ 元数据更新过程中出现异常: {e}")
        traceback.print_exc()
        raise e
    finally:
        if should_disconnect:
            await client.disconnect()

async def main():
    parser = argparse.ArgumentParser(description='Update Documentation and Metadata')
    parser.add_argument('--prepare', action='store_true', help='仅执行根目录整理与环境准备')
    parser.add_argument('--bot', type=str, help='指定 Bot 身份')
    args = parser.parse_args()
    
    await run_metadata_update(only_prepare=args.prepare, bot_name=args.bot)

if __name__ == "__main__":
    asyncio.run(main())
