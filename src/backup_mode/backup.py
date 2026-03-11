import os
import re
import sys
import json
import asyncio
import argparse
import time
from datetime import datetime

# 强制 UTF-8 编码
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

from telethon import TelegramClient, functions, types
from telethon import utils as telethon_utils
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import Database
from sync_mode.sync import extract_creator, classify_message, count_urls
from utils.config import CONFIG

load_dotenv()

# 通过全局配置管理，支持双机器人身份调用
API_ID = CONFIG['api_id']
API_HASH = CONFIG['api_hash']
SESSION_NAME = 'data/sessions/copilot_user'
MANAGED_FOLDERS = CONFIG['managed_folders']

db = Database('data/copilot.db')

def get_sender_name(message):
    sender = message.sender
    if sender:
        if hasattr(sender, 'first_name'):
            name = (sender.first_name or "") + (" " + sender.last_name if sender.last_name else "")
            return name.strip() or "匿名"
        if hasattr(sender, 'title'):
            return sender.title
    return "匿名"

def safe_dirname(name):
    if not name: return "未命名"
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()

def channel_archive_dirname(source_name, chat_id):
    """为频道生成稳定目录名，避免同名频道互相覆盖。"""
    safe_source = safe_dirname(source_name)
    norm_id = str(abs(int(chat_id))) if chat_id is not None else '0'
    return f"{safe_source}_{norm_id}"

def legacy_channel_archive_dirnames(source_name):
    """兼容旧版 B1 目录：仅使用频道名，不带 chat_id。"""
    safe_source = safe_dirname(source_name)
    candidates = [safe_source]
    # 某些旧文件可能直接使用原始名字经过最小清洗；这里保守保留单一 safe 名称即可
    return [c for c in candidates if c]

def rename_channel_archives(old_name, new_name):
    """自动将硬盘上旧的频道名称目录重命名为新名称"""
    if old_name == new_name: return
    safe_old = safe_dirname(old_name)
    safe_new = safe_dirname(new_name)
    if safe_old == safe_new: return
    
    base_dirs = [
        os.path.join('docs', 'archived', 'logs'),
        os.path.join('data', 'archived', 'logs'),
        os.path.join('docs', 'archived', 'backups'),
        os.path.join('data', 'archived', 'backups')
    ]
    
    for base in base_dirs:
        if not os.path.exists(base): continue
        for folder in os.listdir(base):
            folder_path = os.path.join(base, folder)
            if not os.path.isdir(folder_path): continue
            
            old_path = os.path.join(folder_path, safe_old)
            if os.path.exists(old_path) and os.path.isdir(old_path):
                new_path = os.path.join(folder_path, safe_new)
                try:
                    if not os.path.exists(new_path):
                        os.rename(old_path, new_path)
                        print(f"  🔄 自动重命名归档目录:\n    - 从: {old_path}\n    - 到: {new_path}")
                except Exception as e:
                    print(f"  ⚠️ 重命名目录失败 {old_path}: {e}")


def format_range_ids(ids):
    if not ids: return ""
    ids = [i for i in ids if i is not None]
    if not ids: return ""
    if len(ids) == 1: return f"#{ids[0]}"
    id_min, id_max = min(ids), max(ids)
    if len(ids) == (id_max - id_min + 1):
        return f"#{id_min}-#{id_max}"
    return f"#{id_min}-#{id_max} (共{len(ids)}项)"

async def get_fwd_source_name(client, message):
    fwd = message.fwd_from
    if not fwd: return None
    if fwd.from_name: return fwd.from_name
    if hasattr(fwd, 'from_id') and fwd.from_id:
        try:
            ent = await client.get_entity(fwd.from_id)
            return getattr(ent, 'title', getattr(ent, 'first_name', 'Unknown'))
        except:
            return f"ID: {fwd.from_id}"
    return None

# 进度与控制文件 (基于 Bot 隔离)
PROGRESS_FILE = f'data/temp/backup_progress_{CONFIG["app_name"]}.json'
STOP_FLAG = f'data/temp/stop_backup_{CONFIG["app_name"]}.flag'

def update_progress(data):
    """写入进度到 JSON 文件供机器人读取"""
    try:
        os.makedirs('data', exist_ok=True)
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except: pass

def get_historical_speed():
    try:
        if os.path.exists('data/backup_speed.json'):
            with open('data/backup_speed.json', 'r') as f:
                content = f.read().strip()
                if content:
                    data = json.loads(content)
                    speed = data.get('speed_msgs_per_min', 4000)
                    # [FIX-v6] 启动时校准历史速度，防止被极速（如 50000+）污染
                    if speed > 10000: return 6000
                    return max(100, speed)
    except: pass
    return 4000

def update_historical_speed(speed):
    try:
        # [FIX-v4] 增加速度上限保护，防止因瞬间完成导致的时间预估失真（如 60000 msgs/min）
        # 即使是极速状态，由于 Telegram API 限制，4000-8000 是比较真实的范围
        capped_speed = min(speed, 10000) 
        avg_speed = capped_speed
        if os.path.exists('data/backup_speed.json'):
            with open('data/backup_speed.json', 'r') as f:
                content = f.read().strip()
                if content:
                    old_speed = json.loads(content).get('speed_msgs_per_min', 4000)
                    avg_speed = int((old_speed + capped_speed) / 2)
        with open('data/backup_speed.json', 'w') as f:
            json.dump({'speed_msgs_per_min': max(100, avg_speed)}, f)
    except: pass

def is_stopped():
    """检查是否收到停止信号"""
    return os.path.exists(STOP_FLAG)

async def get_total_message_count(client, entity, min_id=0):
    """估算频道消息总数。返回 (count, is_exact)：
       is_exact=True 精确值，is_exact=False 为 ID 差值估算（需标注 ~）。"""
    try:
        res = await client(functions.messages.GetHistoryRequest(
            peer=entity, offset_id=0, offset_date=None, 
            add_offset=0, limit=0, max_id=0, min_id=0, hash=0
        ))
        total_full = res.count

        if min_id > 0:
            EXACT_LIMIT = 2000
            new_msgs = await client.get_messages(entity, limit=EXACT_LIMIT, min_id=min_id)
            actual_count = len(new_msgs)
            if actual_count < EXACT_LIMIT:
                return (actual_count, True)   # 精确计数
            else:
                latest_id = new_msgs[0].id if new_msgs else min_id
                delta_est = max(0, latest_id - min_id)
                return (min(delta_est, total_full), False)  # 估算值

        return (total_full, True)
    except:
        return (0, True)


def build_id_path_index():
    """扫描 metadata 文件夹，建立 ID -> (Folder, Name) 索引"""
    idx = {}
    meta_root = os.path.join('data', 'metadata')
    if not os.path.exists(meta_root):
        return idx
    for root, dirs, files in os.walk(meta_root):
        for f in files:
            if f.endswith('.json'):
                try:
                    with open(os.path.join(root, f), 'r', encoding='utf-8') as jf:
                        mj = json.load(jf)
                        mid = mj.get('id') or mj.get('chat_id')
                        if mid:
                            idx[str(mid)] = {
                                'folder': os.path.basename(root),
                                'name': mj.get('canonical_name', f[:-5])
                            }
                except:
                    pass
    return idx

def get_latest_backup_data(root_dir, channel_dir_name):
    """从指定频道目录中找到最新的、非残缺的备份 JSON 文件及其内容"""
    if not os.path.exists(root_dir): return None, []
    
    latest_file = None
    latest_mtime = 0
    
    # 兼容带编号前缀的文件，如 backup_#B1_CHANNEL_20260228_150224.json
    # 或原始格式 CHANNEL_20260228_150224.json
    # [IMPORTANT] 严格排除所有标记了 _PARTIAL 的备份
    pattern = re.compile(rf".*{re.escape(channel_dir_name)}_(\d{{8}}_\d{{6}})\.json$")
    
    for f in os.listdir(root_dir):
        if '_PARTIAL' in f: continue
        
        match = pattern.search(f)
        if match:
            full_path = os.path.join(root_dir, f)
            mtime = os.path.getmtime(full_path)
            if mtime > latest_mtime:
                latest_mtime = mtime
                latest_file = full_path

    if latest_file:
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return latest_file, data if isinstance(data, list) else []
        except Exception as e:
            print(f"  ⚠️ 读取历史备份文件失败 {latest_file}: {e}")
            
    return None, []

def load_historical_records_fallback(source_name, chat_id):
    """跨全部备份目录兜底查找该频道最新历史 JSON，避免因 folder 变化/旧路径问题导致历史未并入。"""
    backups_root = os.path.join('data', 'archived', 'backups')
    if not os.path.isdir(backups_root):
        return None, []

    candidates = [channel_archive_dirname(source_name, chat_id)] + legacy_channel_archive_dirnames(source_name)
    best_file = None
    best_data = []
    best_mtime = 0

    for folder in os.listdir(backups_root):
        folder_root = os.path.join(backups_root, folder)
        if not os.path.isdir(folder_root):
            continue
        for cand in candidates:
            hist_root = os.path.join(folder_root, cand)
            if not os.path.isdir(hist_root):
                continue
            latest_path, historical_data = get_latest_backup_data(hist_root, cand)
            if latest_path and os.path.exists(latest_path):
                mtime = os.path.getmtime(latest_path)
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_file = latest_path
                    best_data = historical_data if isinstance(historical_data, list) else []
    return best_file, best_data

def iter_channel_backup_files(backups_root, source_name, chat_id, include_partial=False):
    """遍历该频道在全部备份目录下的所有 JSON 快照文件。"""
    if not os.path.isdir(backups_root):
        return

    candidate_dirs = [channel_archive_dirname(source_name, chat_id)] + legacy_channel_archive_dirnames(source_name)
    seen_files = set()

    for folder in os.listdir(backups_root):
        folder_root = os.path.join(backups_root, folder)
        if not os.path.isdir(folder_root):
            continue
        for cand in candidate_dirs:
            channel_root = os.path.join(folder_root, cand)
            if not os.path.isdir(channel_root):
                continue
            for file_name in os.listdir(channel_root):
                if not file_name.lower().endswith('.json'):
                    continue
                if (not include_partial) and '_PARTIAL' in file_name:
                    continue
                full_path = os.path.join(channel_root, file_name)
                if full_path in seen_files or not os.path.isfile(full_path):
                    continue
                seen_files.add(full_path)
                yield full_path

def build_full_historical_snapshot(source_name, chat_id):
    """聚合该频道全部历史 backup 快照，去重后得到完整时间线。

    设计目标：即使上一份最新快照本身残缺，也要尽可能从更早/其他目录快照中拼回完整基线，
    让新的 Bn 文件始终成为“当前已知最完整快照”。
    """
    backups_root = os.path.join('data', 'archived', 'backups')
    best_by_msg_id = {}
    latest_source_path = None
    latest_source_mtime = 0

    for full_path in iter_channel_backup_files(backups_root, source_name, chat_id, include_partial=False):
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"  ⚠️ 读取历史快照失败 {full_path}: {e}")
            continue

        if not isinstance(data, list):
            continue

        try:
            mtime = os.path.getmtime(full_path)
        except Exception:
            mtime = 0

        if mtime > latest_source_mtime:
            latest_source_mtime = mtime
            latest_source_path = full_path

        for item in data:
            if not isinstance(item, dict):
                continue
            msg_id = int(item.get('msg_id', 0) or 0)
            if msg_id <= 0:
                continue

            # [FIX] 解决历史快照中 res_ids 缺失导致的 MD 统计横杠问题
            # 如果历史 JSON 里没编号，尝试去数据库捞一把
            res_ids = item.get('res_ids')
            is_empty = not res_ids or all(v is None or v == [] for v in res_ids.values())
            if is_empty:
                db_res = db.get_message_res_ids(chat_id, msg_id)
                if db_res:
                    item['res_ids'] = db_res

            prev = best_by_msg_id.get(msg_id)
            if not prev:
                best_by_msg_id[msg_id] = item
                continue

            prev_score = len(json.dumps(prev, ensure_ascii=False, sort_keys=True))
            cur_score = len(json.dumps(item, ensure_ascii=False, sort_keys=True))
            if cur_score >= prev_score:
                best_by_msg_id[msg_id] = item

    ordered = sorted(best_by_msg_id.values(), key=lambda x: int(x.get('msg_id', 0) or 0), reverse=True)
    return latest_source_path, ordered

def merge_backup_records(new_records, historical_records):
    """合并新旧记录并按 msg_id 去重，输出最新消息置顶的完整快照。"""
    merged = {}

    for item in historical_records or []:
        if not isinstance(item, dict):
            continue
        msg_id = int(item.get('msg_id', 0) or 0)
        if msg_id > 0:
            merged[msg_id] = item

    for item in new_records or []:
        if not isinstance(item, dict):
            continue
        msg_id = int(item.get('msg_id', 0) or 0)
        if msg_id > 0:
            merged[msg_id] = item

    return sorted(merged.values(), key=lambda x: int(x.get('msg_id', 0) or 0), reverse=True)

def find_best_history_dir(backups_root, folder_name, source_name, chat_id):
    """优先查找新目录名；若不存在，则兼容旧版不带 chat_id 的目录。"""
    folder_root = os.path.join(backups_root, safe_dirname(folder_name))
    if not os.path.isdir(folder_root):
        return None, None

    new_dir = channel_archive_dirname(source_name, chat_id)
    new_path = os.path.join(folder_root, new_dir)
    if os.path.isdir(new_path):
        return new_path, new_dir

    for legacy_dir in legacy_channel_archive_dirnames(source_name):
        legacy_path = os.path.join(folder_root, legacy_dir)
        if os.path.isdir(legacy_path):
            return legacy_path, legacy_dir

    return None, None

def migrate_legacy_history_dir(backups_root, docs_root, folder_name, source_name, chat_id):
    """若发现旧版不带 chat_id 的目录，则尝试迁移到新目录名，保证后续增量沿用同一目录。"""
    folder_safe = safe_dirname(folder_name)
    target_dir = channel_archive_dirname(source_name, chat_id)

    data_folder_root = os.path.join(backups_root, folder_safe)
    docs_folder_root = os.path.join(docs_root, folder_safe)

    if not os.path.isdir(data_folder_root):
        return

    for legacy_dir in legacy_channel_archive_dirnames(source_name):
        if legacy_dir == target_dir:
            continue

        old_data = os.path.join(data_folder_root, legacy_dir)
        new_data = os.path.join(data_folder_root, target_dir)
        if os.path.isdir(old_data) and not os.path.isdir(new_data):
            try:
                os.rename(old_data, new_data)
                print(f"  🔄 迁移旧备份目录: {old_data} -> {new_data}")
            except Exception as e:
                print(f"  ⚠️ 迁移旧备份目录失败 {old_data}: {e}")

        old_docs = os.path.join(docs_folder_root, legacy_dir)
        new_docs = os.path.join(docs_folder_root, target_dir)
        if os.path.isdir(old_docs) and not os.path.isdir(new_docs):
            try:
                os.rename(old_docs, new_docs)
                print(f"  🔄 迁移旧文档目录: {old_docs} -> {new_docs}")
            except Exception as e:
                print(f"  ⚠️ 迁移旧文档目录失败 {old_docs}: {e}")

def get_last_recorded_id(chat_id, source_name, folder_name, is_test):
    """
    [FIX-v2] 综合探测断点：优先查找数据库，若无则遍历所有备份目录查找。
    不再依赖 folder_name 做文件系统查询，兼容旧版/跨文件夹的备份文件。
    """
    # 1. 查数据库 backup_offsets — 不依赖 folder_name，始终可靠
    last_id = db.get_backup_offset(chat_id, is_test=is_test)
    if last_id > 0:
        return last_id
    
    # 2. 遍历 backups 下所有文件夹查找匹配的频道备份 (兼容旧版程序)
    backups_root = os.path.join('data', 'archived', 'backups')
    if not os.path.exists(backups_root):
        return 0
    
    best_id = 0
    for folder in os.listdir(backups_root):
        hist_root, hist_dir_name = find_best_history_dir(backups_root, folder, source_name, chat_id)
        if not hist_root or not os.path.isdir(hist_root):
            continue
        _, historical_data = get_latest_backup_data(hist_root, hist_dir_name)
        if historical_data and isinstance(historical_data, list):
            max_id = max((m.get('msg_id', 0) for m in historical_data), default=0)
            if max_id > best_id:
                best_id = max_id
    
    return best_id

async def backup_channel(client, source, is_test=True, global_stats=None, run_label=None, folder_name=None, entity=None):
    """备份单个频道，最新消息置顶"""
    # [FIX-v6] 如果外部已解析 entity，则直接使用，节省一次 RPC
    source_name = getattr(entity, 'title', str(source)) if entity else str(source)
    print(f"📡 正在拉取备份目标: {source_name}...")
    
    is_partial = False
    historical_records = []
    
    try:
        if not entity:
            # Resolve entity: 尝试转为 int 提高识别率
            source_id = int(source) if (isinstance(source, str) and (source.isdigit() or source.startswith('-'))) else source
            entity = await client.get_entity(source_id)
        # 检查全平台封禁 vs 局部受限
        restriction_reasons = getattr(entity, 'restriction_reason', []) or []
        is_globally_banned = any(
            getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'
            for r in restriction_reasons
        )
        if is_globally_banned:
            name = getattr(entity, 'title', str(source_id))
            print(f"  🚫 [全平台封禁] {name} 无法访问，已跳过。")
            return {'skipped': True, 'name': name, 'reason': 'globally_banned'}
        if getattr(entity, 'restricted', False):
            print(f"  ⚠️ [警告] 频道被 Telegram 标记为局部受限，仍尝试访问...")
            # 局部受限仍可访问，不跳过
            
        chat_id = telethon_utils.get_peer_id(entity)
        current_title = getattr(entity, 'title', None) or getattr(entity, 'username', '') or str(chat_id)
        
        # [NEW] 检查并执行可能的跨系统改名，保持与历史记录连贯
        old_title, new_title = db.check_and_update_channel_name(chat_id, current_title)
        if old_title != new_title:
            print(f"  📢 检测到频道改名: '{old_title}' -> '{new_title}'")
            rename_channel_archives(old_title, new_title)
            
        source_name = new_title
        if global_stats:
            global_stats['current_channel_name'] = f"{source_name} ({chat_id})"
            update_progress(global_stats)
        print(f"🎯 已锁定: {source_name} (ID: {chat_id})")
        
        # 获取文件夹分组
        folder_name = "未分类"
        try:
            filters = await client(functions.messages.GetDialogFiltersRequest())
            for f in getattr(filters, 'filters', []):
                if not hasattr(f, 'title') or not hasattr(f, 'include_peers'): continue
                t_str = f.title.text if hasattr(f.title, 'text') else str(f.title)
                for peer in f.include_peers:
                    pid = getattr(peer, 'channel_id', getattr(peer, 'chat_id', getattr(peer, 'user_id', None)))
                    if pid and (chat_id == pid or str(chat_id).endswith(str(pid))):
                        folder_name = t_str
                        break
                if folder_name != "未分类": break
        except: pass
        
        # [FIX-v4] 强制进行增量探测与分母校准，无论是否显式传递 --incremental
        # 因为 UI 端显示的进度条逻辑是基于 total_raw_estimate 的全局汇总
        last_recorded_id = get_last_recorded_id(chat_id, source_name, folder_name, is_test)
        
        # 只有在 global_stats 存在时才进行“分母修正”
        if global_stats:
            # 校准逻辑：如果探测到断点且非强制全量扫描
            should_calibrate = not global_stats.get('full_scan')
            if last_recorded_id > 0 and should_calibrate:
                delta, _is_exact = await get_total_message_count(client, entity, min_id=last_recorded_id)
                old_local_estimate = global_stats.get('current_channel_total_raw', 0)
                
                # 纠正全局分母：从总估值中扣除该频道被“节省”掉的部分
                diff = old_local_estimate - delta
                if diff > 0:
                    global_stats['total_raw_estimate'] = max(0, global_stats['total_raw_estimate'] - diff)
                    # print(f"  📉 进度条校准: {source_name} 节省 {diff} 条，全局剩余 {global_stats['total_raw_estimate']}")
                
                global_stats['current_channel_total_raw'] = delta
                update_progress(global_stats)
            elif last_recorded_id > 0:
                print(f"  ℹ️ 已探测断点 #{last_recorded_id}, 但当前为全量任务，不缩小分母。")

        # 决定抓取起点 - 这里的优先级依然是：用户指定的 Epoch > 断点
        fetch_min_id = max(last_recorded_id, db.get_epoch_start_msg_id(chat_id, is_test=is_test))
        if fetch_min_id > last_recorded_id:
            print(f"  📅 纪元锁定: 仅拉取 msg #{fetch_min_id} 之后的新消息。")
        elif fetch_min_id > 0:
            print(f"  📅 增量模式: 继续拉取 msg #{fetch_min_id} 之后的消息。")
        
        # [ADD] 核心修复：加载历史数据，确保最终产出包含之前的全部记录
        if fetch_min_id > 0:
            migrate_legacy_history_dir(
                os.path.join('data', 'archived', 'backups'),
                os.path.join('docs', 'archived', 'backups'),
                folder_name,
                source_name,
                chat_id,
            )
            latest_path, historical_records = build_full_historical_snapshot(source_name, chat_id)
            if (not historical_records) and fetch_min_id > 0:
                # 兜底兼容旧逻辑：至少拿到一份最新快照路径帮助定位问题
                latest_path, historical_records = load_historical_records_fallback(source_name, chat_id)
            if latest_path:
                print(f"  📂 已载入历史记录: {os.path.basename(latest_path)} (共 {len(historical_records)} 条)")
            else:
                historical_records = []

        records = []
        last_grouped_id = None
        max_seen_msg_id = fetch_min_id  # [FIX-v10] track true max msg_id seen by API (incl. skip)
        async for message in client.iter_messages(entity, min_id=fetch_min_id, reverse=True):
            # [FIX-v10] record max ID before classify - skip messages also advance the scan cursor
            if message.id > max_seen_msg_id:
                max_seen_msg_id = message.id
            
            text = (message.text or "").strip()
            
            # 使用与 sync.py 完全一致的分类逻辑
            msg_type = classify_message(message)
            if msg_type == 'skip': continue
            
            file_name = None
            if hasattr(message, 'file') and message.file and getattr(message.file, 'name', None):
                file_name = message.file.name
            
            # 多链接统计方案，使用 sync.py 的统计口径
            u_count = count_urls(message)
            sender = get_sender_name(message)
            
            from datetime import timedelta
            local_msg_date = message.date + timedelta(hours=8) if message.date else None
            orig_time = local_msg_date.strftime("%Y-%m-%d %H:%M:%S") if local_msg_date else ""
            
            # [FIX] 识别消息组开头
            cur_grid = message.grouped_id
            is_new_msg = (cur_grid is None or cur_grid != last_grouped_id)
            last_grouped_id = cur_grid

            fwd_source = await get_fwd_source_name(client, message)
            creator = fwd_source or extract_creator(text)

            # [FIX] 激活“首见即分配”逻辑：如果是 Backup 第一次看到这条消息，立即为其申领正式 ID
            res_ids = db.assign_resource_ids(
                chat_id, message.id, msg_type, 
                is_test=is_test, 
                url_count=u_count, 
                is_new_msg=is_new_msg
            ) or {}
            
            record = {
                "msg_id": message.id, "type": msg_type, "sender": sender,
                "original_time": orig_time, "text": text, "file_name": file_name,
                "media_group_id": str(message.grouped_id) if message.grouped_id else None,
                "res_ids": res_ids, "url_count": u_count, "creator": creator
            }
            records.append(record)
            
            # [NEW] 实体发现 (Entity Discovery)
            if creator and creator != "Unknown":
                db.add_entity_candidate(creator, "creator")
            
            # 从文本中提取潜在演员 (简单正则示范)
            if text:
                potential_actors = re.findall(r'[演员|模特|女M|女m]\s*[:：]?\s*([\w\u4e00-\u9fa5]+)', text)
                for actor in potential_actors:
                    if len(actor) < 10: # 防止抓到长段落
                        db.add_entity_candidate(actor, "actor")
            
            if len(records) % 5 == 0:
                # 检查停止信号
                if is_stopped():
                    print(f"🛑 收到停止信号，正在保存 {source_name} 已扫描的 {len(records)} 条消息...")
                    is_partial = True
                    break # 跳出循环，进入下方的保存逻辑

                # 更新实时进度 (每100条更新一次，减少 IO)
                if global_stats:
                    # [NEW] 实时计算当前的相册组数
                    cur_groups = 0
                    tmp_id = None
                    for rm in records:
                        g_id = rm.get('media_group_id')
                        if g_id:
                            if g_id != tmp_id: cur_groups += 1
                            tmp_id = g_id
                        else:
                            cur_groups += 1
                            tmp_id = None
                            
                    global_stats['current_channel_raw_count'] = len(records)
                    global_stats['current_channel_groups_saved'] = cur_groups
                    
                    global_stats['current_raw_count'] = global_stats.get('base_raw_count', 0) + len(records)
                    global_stats['total_groups_saved'] = global_stats.get('base_count_groups', 0) + cur_groups
                    
                    # 动态估算剩余时间 (结合历史速度和当前运行时标)
                    elapsed_total = time.time() - global_stats['start_time']
                    hist_speed = global_stats.get('hist_speed', 4000)
                    
                    # 如果运行超过 5 秒，糅合当前实际速度
                    if elapsed_total > 5 and global_stats['current_raw_count'] > 0:
                        current_speed = int(global_stats['current_raw_count'] / (elapsed_total / 60.0))
                        blended_speed = (current_speed + hist_speed) / 2.0
                    else:
                        blended_speed = hist_speed
                        
                    # [FIX-v2] 下限保护：确保分母不会小于已扫描数
                    global_stats['total_raw_estimate'] = max(global_stats['total_raw_estimate'], global_stats['current_raw_count'])
                    remaining_raw = max(0, global_stats['total_raw_estimate'] - global_stats['current_raw_count'])
                    global_stats['estimated_total_time_minutes'] = round(remaining_raw / max(1, blended_speed), 1)

                    # 命令行频率控制
                    now = time.time()
                    if now - global_stats.get('last_cli_print', 0) >= 10:
                        print(f"  📦 进度: {source_name} 已扫描 {len(records)}/{global_stats['current_channel_total_raw'] if global_stats else '?'} 原始记录 "
                              f"({(len(records)/(global_stats['current_channel_total_raw'] if global_stats['current_channel_total_raw']>0 else 1)*100):.1f}%) "
                              f"预计还需 {global_stats['estimated_total_time_minutes']} 分钟")
                        global_stats['last_cli_print'] = now
                    
                    global_stats['last_update'] = now
                    update_progress(global_stats)

        if not records and not historical_records:
            print(f"ℹ️ {source_name} 无任何消息记录，跳过。")
            return {"id": chat_id, "name": source_name, "count": 0, "new_count": 0, "scanned_group_count": 0, "raw_count": 0, "raw_new_count": 0, "status": "skipped", "ranges": {"all": "-", "video": "-", "photo": "-", "text": "-"}}

        # [FIX] 即使无新消息 (records为空)，只要有 historical_records，我们依然执行保存逻辑
        # 这样会生成一个新的带时间戳的文件，包含之前的全部历史，符合用户“每执行必生成”的预期
        if not records and max_seen_msg_id <= fetch_min_id:
            print(f"ℹ️ {source_name} 没有新消息追加，仅生成新的历史快照。")
        elif not records:
            # [FIX-v10] 有新消息但全部被 skip，仍需推进断点以免下次重复扫描
            print(f"ℹ️ {source_name} 没有新的有效消息 (扫描到 #{max_seen_msg_id}, 全部为 skip)。")
            if not is_partial:
                db.update_backup_offset(chat_id, max_seen_msg_id, is_test=is_test)
                print(f"  📌 断点已推进: #{max_seen_msg_id} (skip-only)")
        else:
            print(f"✅ {source_name} 抓取到 {len(records)} 条新消息，正在合并写入...")
            # [FIX-v10] 使用 max_seen_msg_id 确保断点覆盖所有已扫描消息
            if not is_partial:
                new_offset = max(max_seen_msg_id, max(m['msg_id'] for m in records))
                db.update_backup_offset(chat_id, new_offset, is_test=is_test)
                print(f"  📌 断点已更新: #{new_offset}")
            else:
                print(f"  ⚠️ 任务被中断，保留上一个数据库断点。")

        # 归并逻辑：新的 Bn 始终输出“完整时间线快照”，而不是仅保存本轮增量补丁
        records.reverse()
        final_records = merge_backup_records(records, historical_records)
        
        # [FIX] 统一给 final_records 里的缺失 res_ids 的条目分配 ID
        # 为了保证 ID 会从旧到新顺延递增，我们对 final_records 进行反向遍历（即按时间正序）
        last_grouped_id_pass = None
        
        # [优化] 计算需要修复的数量
        need_fix_count = sum(1 for item in final_records if not item.get('res_ids') or all(v is None or v == [] for v in item.get('res_ids', {}).values()))
        if need_fix_count > 0:
            print(f"  🛠️ 正在为 {need_fix_count} 个陈旧记录进行编号补全(防阻塞模式)...")
            
        fixed_count = 0
        for item in reversed(final_records):
            res_ids = item.get('res_ids')
            is_empty = not res_ids or all(v is None or v == [] for v in res_ids.values())
            if is_empty:
                cid = chat_id
                mid = int(item.get('msg_id', 0) or 0)
                if mid <= 0: continue
                mtype = item.get('type')
                ucount = item.get('url_count', 0)
                
                # 尝试从库里拿，刚才 db.py 已经修正了，如果拿到空会由于下面这个检查不进入分支
                db_res = db.get_message_res_ids(cid, mid)
                if db_res and any(v is not None and v != [] for v in db_res.values()):
                    item['res_ids'] = db_res
                else:
                    cur_grid = item.get('media_group_id')
                    is_new_msg = (cur_grid is None or cur_grid != last_grouped_id_pass)
                    last_grouped_id_pass = cur_grid
                    
                    new_ids = db.assign_resource_ids(
                        cid, mid, mtype, 
                        is_test=is_test, 
                        url_count=ucount, 
                        is_new_msg=is_new_msg,
                        commit=False  # [不频繁提交防止卡死主线程]
                    ) or {}
                    item['res_ids'] = new_ids
                    
                    fixed_count += 1
                    # 每补配 500 个编号让出一次事件循环，并且打印一次进度
                    if fixed_count % 500 == 0:
                        db.conn.commit()  # 落盘一部分
                        print(f"    ➡️ 已回填补齐 {fixed_count} / {need_fix_count} 条...")
                        await asyncio.sleep(0.01)

        if fixed_count > 0:
            db.conn.commit()
            print(f"  ✅ 历史编号回填完成！共修补 {fixed_count} 条记录。")

        # [REMOVE] Early return was here, preventing file save

        # 归档路径 (data 为 JSON, docs 为 MD)
        channel_dir = channel_archive_dirname(source_name, chat_id)
        root_dir = os.path.join('data', 'archived', 'backups', safe_dirname(folder_name), channel_dir)
        docs_dir = os.path.join('docs', 'archived', 'backups', safe_dirname(folder_name), channel_dir)
        os.makedirs(root_dir, exist_ok=True)
        os.makedirs(docs_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        label_prefix = f"{run_label}_" if run_label else ""
        partial_tag = "_PARTIAL" if is_partial else ""
        json_file = os.path.join(root_dir, f"backup_{label_prefix}{channel_dir}_{timestamp}{partial_tag}.json")
        md_file = os.path.join(docs_dir, f"{label_prefix}{channel_dir}_{timestamp}{partial_tag}.md")
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(final_records, f, ensure_ascii=False, indent=2)
            
        # [NEW] 备份保留策略：清理旧文件，仅保留最近3次
        def prune_backups(directory, prefix, suffix, keep=3):
            files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(suffix)]
            files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
            if len(files) > keep:
                for old_file in files[keep:]:
                    try:
                        os.remove(os.path.join(directory, old_file))
                        print(f"  🗑️ 已清理过期备份: {old_file}")
                    except: pass

        prune_backups(root_dir, "backup_", ".json")
        prune_backups(docs_dir, "", ".md")

        # 统计分析 (基于全量记录)
        v_c = sum(1 for m in final_records if m['type'] == 'video')
        p_c = sum(1 for m in final_records if m['type'] == 'photo')
        f_c = sum(1 for m in final_records if m['type'] == 'file')
        g_c = sum(1 for m in final_records if m['type'] == 'gif')
        pv_c = sum(1 for m in final_records if m['type'] == 'link_preview')
        t_c = sum(1 for m in final_records if m['type'] == 'text')
        l_c = sum(1 for m in final_records if m['type'] == 'link')
        
        # 消息组合并 (基于全量记录，最新置顶)
        groups = []
        temp_g = []
        last_grid = None
        for m in final_records:
            grid = m.get('media_group_id')
            if grid:
                if grid == last_grid: temp_g.append(m)
                else:
                    if temp_g: groups.append(temp_g)
                    temp_g, last_grid = [m], grid
            else:
                if temp_g: groups.append(temp_g)
                groups.append([m])
                temp_g, last_grid = [], None
        if temp_g: groups.append(temp_g)

        # [FIX] 补充之前意外删掉的 groups_new 计算逻辑 (基于纯新记录，用于上报新增/扫描组数)
        groups_new = []
        temp_g_new = []
        last_grid_new = None
        for m in records:
            grid = m.get('media_group_id')
            if grid:
                if grid == last_grid_new: temp_g_new.append(m)
                else:
                    if temp_g_new: groups_new.append(temp_g_new)
                    temp_g_new, last_grid_new = [m], grid
            else:
                if temp_g_new: groups_new.append(temp_g_new)
                groups_new.append([m])
                temp_g_new, last_grid_new = [], None
        if temp_g_new: groups_new.append(temp_g_new)
        scanned_groups_this_run = len(groups_new)
        saved_groups_this_run = len(groups_new)

        # [NEW] 统计编号范围 (基于全量记录)
        id_ranges = {}
        for key in ['total', 'video', 'photo', 'gif', 'other', 'preview', 'text', 'link', 'link_msg', 'res_msg']:
            id_list = []
            for m in final_records:
                val = m['res_ids'].get(key)
                if val:
                    if isinstance(val, list): id_list.extend(val)
                    else: id_list.append(val)
            id_ranges[key] = (format_range_ids(id_list) if id_list else "-", len(id_list))

        r_all, _ = id_ranges['total']
        r_msg, _ = id_ranges['res_msg']
        r_vid, _ = id_ranges['video']
        r_pho, _ = id_ranges['photo']
        r_gif, _ = id_ranges['gif']
        r_file, _ = id_ranges['other']
        r_prv, _ = id_ranges['preview']
        r_txt, _ = id_ranges['text']
        r_lnk, url_actual_total = id_ranges['link']
        r_lmk, _ = id_ranges['link_msg']

        num_total_groups = len(groups)
        num_media_groups = sum(1 for g in groups if any(m['type'] in ['video', 'photo', 'gif', 'file'] for m in g))
        num_text_msgs = t_c + l_c
        lm_c = sum(1 for m in records if m.get('url_count', 0) > 0)

        # 生成 Markdown
        md = [
            f"# {source_name} - 历史备份归档",
            f"",
            f"### 📊 全局统计汇总",
            f"- **消息数量**: {num_total_groups} 条 (相册已合并)",
            f"- **原始消息条数**: {len(final_records)}",
            f"- **带资源消息**: {num_media_groups + pv_c} ({num_media_groups}组 + {pv_c}预览)",
            f"- **文本消息数量**: {num_text_msgs}",
            f"- **资源总量**: {v_c+p_c+g_c+pv_c+f_c} (🎬:{v_c} | 🖼️:{p_c} | 🎞️:{g_c} | 👁‍🗨️:{pv_c} | 📄:{f_c})",
            f"- **链接总数**: {url_actual_total} 🔗",
            f"- **携带链接消息**: {lm_c} 📎",
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
            f"> 📍 来源分组: `{folder_name}` | 🕒 导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "", "---", ""
        ]

        for i, g in enumerate(groups):
            g_idx = len(groups) - i
            
            def get_g_range_b(key):
                ids = []
                for m in g:
                    if m.get('res_ids') and m['res_ids'].get(key):
                        val = m['res_ids'][key]
                        if isinstance(val, list): ids.extend(val)
                        else: ids.append(val)
                if not ids: return None
                unique_ids = sorted(list(set(ids)))
                if len(unique_ids) == 1: return f"#{unique_ids[0]}"
                mi, ma = unique_ids[0], unique_ids[-1]
                if len(unique_ids) == (ma - mi + 1): return f"#{mi}-#{ma}"
                return f"#{mi}-#{ma}"

            g_r_msg = get_g_range_b('res_msg')
            g_r_txt = get_g_range_b('text')
            g_r_lmk = get_g_range_b('link_msg')
            g_r_all = get_g_range_b('total')
            g_r_vid = get_g_range_b('video')
            g_r_pho = get_g_range_b('photo')
            g_r_gif = get_g_range_b('gif')
            g_r_file = get_g_range_b('other')
            g_r_prv = get_g_range_b('preview')
            g_r_lnk = get_g_range_b('link')

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
            group_creator = next((m.get('creator') for m in g if m.get('creator') and m.get('creator') != "Unknown"), None)
            group_text = next((m.get('text') for m in g if m.get('text')), None)
            
            if group_creator:
                md.append(f"- **发布源**: {group_creator}")
                
            if group_text:
                md.append("")
                group_text_clean = group_text[:500] + "..." if len(group_text) > 500 else group_text
                for line in group_text_clean.split('\n'):
                    md.append(f"> {line}" if line.strip() else ">")
                md.append("")

            for m in g:
                time_str = m.get('original_time', "")[:16].replace('T', ' ') if m.get('original_time') else "N/A"
                icon = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️", "link": "🔗", "link_preview": "👁‍🗨️", "text": "✍️"}.get(m.get('type', ''), "✍️")
                sender = m.get('sender') or 'System'
                
                # 获取子编号
                t = m.get('type')
                res_ids = m.get('res_ids', {})
                key_map = {'video': ('video', '视频'), 'photo': ('photo', '图片'), 'gif': ('gif', 'GIF'), 'file': ('other', '文件'), 'link': ('link', '链接'), 'link_preview': ('preview', '预览链接'), 'text': ('text', '文本')}
                sub_id_str = ""
                if t in key_map:
                    db_key, label_name = key_map[t]
                    val = res_ids.get(db_key)
                    if val:
                        if isinstance(val, list) and val:
                            sub_id_str = f" {label_name} #{min(val)}-#{max(val)}" if min(val) != max(val) else f" {label_name} #{val[0]}"
                        elif not isinstance(val, list):
                            sub_id_str = f" {label_name} #{val}"
                            
                r_id = f"#{res_ids.get('total', '')}" if res_ids.get('total') else f"ID:{m.get('msg_id', '?')}"
                
                # 2. 消息头
                md.append(f"**{icon} ({time_str}) - {sender}{sub_id_str} | 总: {r_id}**\n")
                
                # 3. 文件详情
                if m.get('file_name'):
                    md.append(f"- **文件名**: `{m.get('file_name')}`\n")

        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(md))
            
        if is_stopped():
            print(f"⚠️ {source_name} 备份已中断（部分保存），新抓取 {len(records)} 条，总计 {len(final_records)} 条。")
            return {
                "id": chat_id, "name": source_name,
                "count": num_total_groups,
                "new_count": saved_groups_this_run,
                "scanned_group_count": scanned_groups_this_run,
                "saved_group_count": saved_groups_this_run,
                "raw_count": len(final_records),
                "raw_new_count": len(records),
                "status": "interrupted",
                "folder": folder_name or "未分类",
                "json_file": json_file,
                "md_file": md_file,
                "ranges": {
                    "all": r_all, "video": r_vid, "photo": r_pho, "text": r_txt
                }
            }
            
        # [FIX-v10] 移除冗余的第二次断点更新 — 断点仅在上方 records 非空时写入一次
        # 旧逻辑在 records 为空时会重写 last_recorded_id（旧值），无意义且可能掩盖问题

        print(f"✅ {source_name} 备份完成！(新抓取 {len(records)} 条，总计 {len(final_records)} 条)")
        # completed:
        # - saved_group_count / new_count: 最终成功保存的增量组数
        # - scanned_group_count: 本轮扫描命中的增量组数
        # - count/raw_count: 完整快照总量
        return {
            "id": chat_id, "name": source_name,
            "count": num_total_groups,    # 使用用户偏好的组数
            "new_count": saved_groups_this_run,
            "scanned_group_count": scanned_groups_this_run,
            "saved_group_count": saved_groups_this_run,
            "raw_count": len(final_records),
            "raw_new_count": len(records),
            "status": "completed",
            "folder": folder_name or "未分类",
            "json_file": json_file,
            "md_file": md_file,
            "ranges": {
                "all": r_all, "video": r_vid, "photo": r_pho, "text": r_txt
            }
        }
    except Exception as e:
        err_msg = str(e).lower()
        err_type = type(e).__name__.lower()
        if "channelprivate" in err_type or "chatwriteforbidden" in err_type or "chatinaccessible" in err_type or \
           "could not find the input entity" in err_msg:
            print(f"🚷 发现疑似退出或失联频道: {source}")
        else:
            print(f"❌ 备份 {source} 失败: {e}")
            import traceback; traceback.print_exc()
        return None

async def main():
    parser = argparse.ArgumentParser(description="Backup Telegram channels.")
    parser.add_argument('channel', nargs='?', help='Target channel shortcut')
    parser.add_argument('--mode', choices=['1', '2'], help='1: Partial, 2: Global')
    parser.add_argument('--ids', help='Target IDs')
    parser.add_argument('--test', action='store_true', help='Test Mode')
    parser.add_argument('--incremental', action='store_true', help='Incremental Mode')
    parser.add_argument('--run-id', type=int, help='Current run ID')
    parser.add_argument('--channel', type=int, help='Telegram channel ID')
    parser.add_argument('--run-label', help='Current run label (e.g. B1)')
    parser.add_argument('--bot', type=str, default='tgporncopilot', help='指定触发的 Bot 身份')
    args = parser.parse_args()
    
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        # 预热实体缓存并获取活跃对话列表
        dialogs = await client.get_dialogs()
        active_dialog_ids = {d.id for d in dialogs}
        
        targets = []
        if args.ids: targets = [x.strip() for x in args.ids.split(',') if x.strip()]
        elif args.channel: targets = [args.channel]
        elif args.mode == '2':
            filters = await client(functions.messages.GetDialogFiltersRequest())
            seen = set()
            for f in getattr(filters, 'filters', []):
                if not hasattr(f, 'include_peers'): continue
                
                # [FIX-v9.11] 检查文件夹名称是否在当前 Bot 的管辖范围内
                title = getattr(f, 'title', None)
                f_name = (title.text if hasattr(title, 'text') else str(title)) if title else ""
                
                # [NEW] 支持 '*' 通配符，表示管理所有文件夹
                if "*" not in MANAGED_FOLDERS and "ALL" not in [m.upper() for m in MANAGED_FOLDERS]:
                    if f_name not in MANAGED_FOLDERS:
                        continue
                
                # include_peers + pinned_peers 都要覆盖，防止遗漏
                all_peers = list(getattr(f, 'include_peers', [])) + list(getattr(f, 'pinned_peers', []))
                for peer in all_peers:
                    try:
                        signed_id = telethon_utils.get_peer_id(peer)
                    except:
                        pid = getattr(peer, 'channel_id', getattr(peer, 'chat_id', getattr(peer, 'user_id', None)))
                        signed_id = pid
                    
                    # [FIX] 核心修复：只有 ID 在活跃对话列表中，才加入备份目标
                    # 这能过滤掉已经在电报里退群、但文件夹设置里还残留的“幽灵 ID”
                    if signed_id and signed_id not in seen and signed_id in active_dialog_ids:
                        seen.add(signed_id); targets.append(signed_id)

        
        if not targets:
            print("❌ 未指定任何备份目标。")
            return

        if args.run_id:
            run_id = args.run_id
            label = args.run_label or db.get_backup_label(run_id)
        else:
            run_id = db.start_backup_run(mode=args.mode, is_incremental=args.incremental, is_test=args.test, bot_name=CONFIG['app_name'])
            label = db.get_backup_label(run_id)
        start_time = time.time()
        print(f"🚀 开始备份任务 {label} ...")
        
        # [NEW] 1. 先扫描 metadata 建立 ID -> 路径索引，确保 100% 精确匹配
        id_path_index = build_id_path_index()
        
        # 初始化全局进度统计
        total_msg_estimate = 0
        target_entities = []
        
        # [FIX-v7] 预扫阶段也提供进度反馈，防止 78 个频道让用户干等
        temp_stats = {
            'phase': 'prescan',          # 识别预扫阶段
            'prescan_done': 0,           # 已完成预扫的频道数
            'total_raw_estimate': 0,
            'current_raw_count': 0,
            'total_channels': len(targets),
            'completed_channels_count': 0,
            'current_channel_name': "任务初始化中...",
            'start_time': start_time,
            'hist_speed': get_historical_speed()
        }

        for i, t in enumerate(targets):
            try:
                # Resolve entity
                sid = int(t) if (isinstance(t, str) and (t.isdigit() or t.startswith('-'))) else t
                
                # 预扫时更新 UI
                temp_stats['prescan_done'] = i
                temp_stats['current_channel_name'] = f"正在预扫 [{i+1}/{len(targets)}] {sid}"
                update_progress(temp_stats)

                ent = await client.get_entity(sid)
                if getattr(ent, 'restricted', False):
                    print(f"  ⚠️ [警告] {sid} 被标记为受限，尝试预估任务量...")
                
                total_c, _tc_exact = await get_total_message_count(client, ent)
                chat_id_signed = telethon_utils.get_peer_id(ent)
                
                
                # [NEW] 提取分类文件夹
                folder_name = "未分类"
                try:
                    idx_info = id_path_index.get(str(chat_id_signed))
                    if idx_info:
                        folder_name = idx_info['folder']
                except: pass

                # [FIX-v4] 深度预估：优先使用 DB/文件断点来计算每个频道的增量预计
                actual_estimate = total_c
                source_name = getattr(ent, 'title', str(sid))
                
                # 尝试多种手段获取断点
                last_offset = get_last_recorded_id(chat_id_signed, source_name, folder_name, args.test)
                
                if last_offset > 0:
                    # 如果非全量模式，估算增量
                    if args.incremental:
                        actual_estimate, est_is_exact = await get_total_message_count(client, ent, min_id=last_offset)
                        temp_stats['total_raw_is_exact'] = temp_stats.get('total_raw_is_exact', True) and est_is_exact
                        prefix = "" if est_is_exact else "~"
                        print(f"  📊 增量预判: {source_name} 待补充 {prefix}{actual_estimate} 条 (断点 #{last_offset})")
                    else:
                        print(f"  📊 全量预判: {source_name} 总计 ~{total_c} 条 (已探测断点 #{last_offset} 但不使用)")
                
                total_msg_estimate += actual_estimate
                target_entities.append((sid, actual_estimate, folder_name, ent)) # 复用 ent
                temp_stats['total_raw_estimate'] = total_msg_estimate
            except Exception as e:
                err_msg = str(e).lower()
                err_type = type(e).__name__.lower()
                if "channelprivate" in err_type or "chatwriteforbidden" in err_type or "chatinaccessible" in err_type:
                    print(f"🚷 预扫描发现疑似已退出或失联频道: {t}")
                target_entities.append((t, 0, "未知分类"))

        # 预加载历史备份速度
        hist_speed = get_historical_speed()

        global_stats = {
            "status": "running",
            "phase": "scanning",  # [NEW] 正式扫描阶段
            "label": label,
            "total_channels": len(target_entities),
            "accessible_channels_total": len(target_entities),
            "completed_channels_count": 0,
            "current_channel_name": "正在初始化...",
            "current_channel_id": None,
            "current_channel_total_raw": 0,
            "current_channel_raw_count": 0,
            "current_channel_groups_saved": 0,
            "total_raw_estimate": total_msg_estimate,
            "total_raw_is_exact": temp_stats.get('total_raw_is_exact', True),  # [NEW] 来自预扫阶段
            "current_raw_count": 0,
            "total_groups_saved": 0,
            "new_messages": 0,
            "total_messages": 0,
            "channels": [],
            
            "base_raw_count": 0,
            "base_count_groups": 0,
            
            "estimated_total_time_minutes": round(total_msg_estimate / hist_speed, 1) if total_msg_estimate > 0 else 0,
            "hist_speed": hist_speed,
            "start_time": start_time,
            
            "full_scan": not args.incremental,
            "last_update": time.time()
        }
        update_progress(global_stats)
        
        if os.path.exists(STOP_FLAG):
            try: os.remove(STOP_FLAG)
            except: pass

        results = []
        for i, (t, t_count, t_folder, t_ent) in enumerate(target_entities):
            if is_stopped(): break
            
            # 更新当前频道信息
            sid = t
            source_name = getattr(t_ent, 'title', str(sid)) if t_ent else str(sid) # Handle None for t_ent
            
            global_stats['current_channel_id'] = sid
            global_stats['current_channel_name'] = source_name
            global_stats['current_channel_total_raw'] = t_count
            global_stats['completed_channels_count'] = i
            update_progress(global_stats)
            
            # 开始备份 (复用 t_ent)
            res = await backup_channel(client, sid, is_test=args.test, global_stats=global_stats, run_label=label, folder_name=t_folder, entity=t_ent)
            
            if res:
                if isinstance(res, dict) and res.get('status') == 'stopped':
                    break
                if isinstance(res, dict) and res.get('skipped'):
                    # 全平台封禁：记录跳过但不计入统计
                    global_stats.setdefault('skipped_banned', []).append(res.get('name', str(t)))
                    global_stats['accessible_channels_total'] = max(0, global_stats.get('accessible_channels_total', len(target_entities)) - 1)
                    update_progress(global_stats)
                    continue
                results.append(res)
                # [FIX-v7] 频道完成后，用实际新增数替换预估数，从分母中去除虚高部分
                actual_new_raw = res.get('raw_new_count', 0)
                # t_count 是该频道的预估增量
                estimated_for_this = t_count
                overcount = max(0, estimated_for_this - actual_new_raw)
                if overcount > 0:
                    global_stats['total_raw_estimate'] = max(global_stats['current_raw_count'], 
                                                             global_stats['total_raw_estimate'] - overcount)
                
                global_stats['base_count_groups'] += res.get('new_count', 0)
                global_stats['base_raw_count'] += actual_new_raw
                
                global_stats['total_groups_saved'] = global_stats['base_count_groups']
                global_stats['current_raw_count'] = global_stats['base_raw_count']
                
                global_stats['new_messages'] += res.get('new_count', 0)
                global_stats['total_messages'] += res.get('count', 0)
                global_stats['channels'].append(res)
                global_stats['completed_channels_count'] += 1
                update_progress(global_stats)
            
        final_status = "interrupted" if is_stopped() else "completed"
        skipped_banned = global_stats.get('skipped_banned', [])
        global_stats['status'] = final_status
        global_stats['skipped_banned'] = skipped_banned
        global_stats['total_channels'] = global_stats.get('accessible_channels_total', global_stats.get('total_channels', 0))
        update_progress(global_stats)
            
        elapsed = time.time() - start_time
        
        # [NEW] 更新历史记录速度
        if elapsed > 10 and global_stats['current_raw_count'] > 0:
            actual_speed = int(global_stats['current_raw_count'] / (elapsed / 60.0))
            if actual_speed > 100:
                update_historical_speed(actual_speed)
        db.finish_backup_run(run_id, {
            "total_messages": global_stats['total_messages'],
            "new_messages": global_stats['new_messages'],
            "total_channels": global_stats.get('accessible_channels_total', global_stats['completed_channels_count']),
            "channels": global_stats['channels'],
            "duration": f"{elapsed/60:.1f} min",
            "skipped_banned": skipped_banned
        })
        print(f"\n✨ {label} 全部备份任务已完成！ (状态: {final_status})")
        if skipped_banned:
            print(f"\n🚫 以下频道因全平台封禁已跳过 ({len(skipped_banned)} 个):")
            for n in skipped_banned: print(f"  - {n}")


if __name__ == "__main__":
    asyncio.run(main())
