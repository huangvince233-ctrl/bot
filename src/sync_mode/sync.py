import os
import asyncio
import re
import json
import sys
import argparse
import traceback
from datetime import datetime
from telethon import TelegramClient, functions, types, utils
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import Database

# 强制 Windows 控制台使用 UTF-8 编码，防止 emoji 导致 GBK 报错
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        # 兼容旧版本 Python
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = 'data/sessions/copilot_user'
TARGET_GROUP_ID = int(os.getenv('TARGET_GROUP_ID'))
SOURCE_CHANNELS = os.getenv('SOURCE_CHANNELS').split(',')

def normalize_tg_id(peer_id):
    """统一 ID 格式，去除 -100 前缀或负号，方便比较"""
    s = str(peer_id)
    if s.startswith('-100'): return s[4:]
    if s.startswith('-'): return s[1:]
    return s

db = Database('data/copilot.db')

# ===== 工具函数 =====

def extract_creator(text):
    if not text:
        return "Unknown"
    hashtags = re.findall(r'#(\w+)', text)
    if hashtags:
        return hashtags[0]
    match = re.search(r'(?:By|Creator|Artist):\s*(\w+)', text, re.I)
    if match:
        return match.group(1)
    return "Unknown"

def classify_message(message):
    """分类: video / photo / gif / link / link_preview / file / text / skip"""
    text = (message.text or "").strip()
    # 核心判断：如果包含 TOS 违规关键词，无论是什么类型消息，一律跳过
    if "violated Telegram" in text or "not supported by" in text or "TOS" in text:
        return 'skip'

    if message.media:
        if isinstance(message.media, (types.MessageMediaEmpty, types.MessageMediaUnsupported)):
            return 'skip'
        if message.fwd_from and not (message.video or message.photo or message.document or getattr(message, 'web_preview', None)):
            if text:
                return 'text'
            return 'skip'
        
        # 网页预览 (MessageMediaWebPage) → link 或 link_preview
        wp = getattr(message, 'web_preview', None)
        if wp is not None:
            # 判断是否有可预览的嵌入媒体 (视频/动图/图片预览)
            has_preview_media = False
            if hasattr(wp, 'document') and wp.document:
                has_preview_media = True  # 嵌入了视频/GIF文档
            elif hasattr(wp, 'photo') and wp.photo:
                has_preview_media = True  # 嵌入了图片预览
            return 'link_preview' if has_preview_media else 'link'
        # web_preview 为 None 时，检查是否是 MessageMediaWebPage（含 WebPageEmpty）
        # Telegram 对部分网站（如成人内容）无法加载预览，返回 WebPageEmpty，
        # 此时 web_preview 属性为 None，但消息本身是一条链接消息，需正确分类
        if isinstance(message.media, types.MessageMediaWebPage):
            return 'link' if text else 'skip'
        
        # GIF 动图
        if getattr(message, 'gif', False):
            return 'gif'
            
        if message.video:
            return 'video'
        if message.photo:
            return 'photo'
        if message.document:
            return 'file'
        return 'skip'
    elif text:
        # 如果文本包含 URL，识别为 link 而非纯文字
        if count_urls(message) > 0:
            return 'link'
        return 'text'
    else:
        return 'skip'

async def get_fwd_source_name(client, message):
    """获取消息的原始转发来源名称 (异步解析以确保名字准确)"""
    fwd = message.fwd_from
    if not fwd:
        return None
    
    # 1. 如果自带名字 (通常是 User)，直接返回
    if fwd.from_name:
        return fwd.from_name
        
    # 2. 如果自带 ID (通常是 Channel/User)，尝试通过 client 解析
    if hasattr(fwd, 'from_id') and fwd.from_id:
        try:
            # 优先从本地缓存/Dialogs 中获取，减少网络请求
            entity = await client.get_entity(fwd.from_id)
            if hasattr(entity, 'title'): return entity.title
            if hasattr(entity, 'first_name'):
                return (entity.first_name or "") + (" " + entity.last_name if entity.last_name else "")
        except Exception:
            # Fallback to ID-based labels if resolution fails
            if hasattr(fwd.from_id, 'channel_id'): return f"Channel#{fwd.from_id.channel_id}"
            if hasattr(fwd.from_id, 'user_id'): return f"User#{fwd.from_id.user_id}"
            
    return None

def count_urls(message):
    """统计消息中携带的链接总数（含重复），匹配用户视觉上看到的 URL 数量"""
    text = message.text or ""
    
    # 1. 正则统计文本中所有可见 URL 出现次数（包括重复）
    count = len(re.findall(r'https?://[^\s，。；、]+', text))
    
    # 2. 补充 TextUrl 类型的隐藏链接（URL 藏在超链接文字背后，不出现在可见文本中）
    if message.entities:
        for e in message.entities:
            if isinstance(e, types.MessageEntityTextUrl):
                count += 1
    
    return count

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
    """把频道名转成安全的文件夹名"""
    if not name: return "未命名"
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()

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

def safe_caption(text, max_len=1024):
    """Telegram caption 最长 1024 字符，安全截断"""
    if not text:
        return ""
    text = text.strip()
    if len(text) > max_len:
        text = text[:max_len - 3] + "..."
    return text

def save_to_local_archive(source_name, run_label, records, folder_name="未分类"):
    """
    保存消息记录到本地文件:
      - data/archived/logs/{folder_name}/{source_name}/sync_{run_label}.json
      - docs/archived/logs/{folder_name}/{source_name}/sync_{run_label}.md
    """
    # 1. 保存 JSON 到 data/
    dir_path = os.path.join('data', 'archived', 'logs', safe_dirname(folder_name), safe_dirname(source_name))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"sync_{run_label}.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  💾 已保存到 {file_path} ({len(records)} 条)")
    
    # 2. 同步生成 MD 到 docs/
    try:
        docs_dir = os.path.join('docs', 'archived', 'logs', safe_dirname(folder_name), safe_dirname(source_name))
        os.makedirs(docs_dir, exist_ok=True)
        time_label = datetime.now().strftime("%Y%m%d_%H%M%S")
        md_path = os.path.join(docs_dir, f"sync_{safe_dirname(run_label)}_{time_label}.md")
        
        # 统计
        v_cnt = sum(1 for r in records if r.get('type') == 'video')
        p_cnt = sum(1 for r in records if r.get('type') == 'photo')
        f_cnt = sum(1 for r in records if r.get('type') == 'file')
        g_cnt = sum(1 for r in records if r.get('type') == 'gif')
        pv_cnt = sum(1 for r in records if r.get('type') == 'link_preview')
        t_cnt = sum(1 for r in records if r.get('type') == 'text')
        l_cnt = sum(1 for r in records if r.get('type') == 'link')
        
        md = [
            f"# {source_name} - 同步报告 {run_label}",
            "",
            f"### 📊 同步统计",
            f"- **同步总数**: {len(records)}",
            f"- **分类统计**: 🎬:{v_cnt} | 🖼️:{p_cnt} | 🎞️:{g_cnt} | 👁️:{pv_cnt} | 🔗:{l_cnt} | 📄:{f_cnt} | ✍️:{t_cnt}",
            f"- **同步时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"- **来源分组**: {folder_name}",
            "",
            "---",
            "",
        ]
        
        if not records:
            md.append("\n> [!NOTE]\n> 本次同步该频道未发现新消息。")
        else:
            md.append("### 📜 消息列表")
            for r in records:
                res_ids = r.get('res_ids', {})
                r_id = f"#{res_ids.get('total', '')}" if res_ids.get('total') else f"ID:{r.get('msg_id', '?')}"
                time_str = (r.get('original_time') or 'N/A')[:16].replace('T', ' ')
                icon = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️", "link": "🔗", "link_preview": "👁️"}.get(r.get('type', ''), "✍️")
                # 获取子编号
                t = r.get('type')
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
                            
                sender = r.get('sender') or 'System'
                
                # 1. 消息头
                md.append(f"**{icon} ({time_str}) - {sender}{sub_id_str} | 总: {r_id}**\n")
                
                # 2. 来源
                if r.get('creator') and r['creator'] != 'Unknown':
                    md.append(f"- **发布者**: {r['creator']}")
                
                # 3. 文本部分
                if r.get('text'):
                    md.append("")
                    clean_text = r['text'][:500] + "..." if len(r['text']) > 500 else r['text']
                    for line in clean_text.split('\n'):
                        md.append(f"> {line}" if line.strip() else ">")
                    md.append("")
                
                # 4. 文件详情
                if r.get('file_name'):
                    md.append(f"- **文件名**: `{r['file_name']}`\n")
        
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(md))
        print(f"  📝 已同步到 {md_path}")
    except Exception as e:
        print(f"  ⚠️ 生成 docs MD 失败: {e}")
    
    return file_path


# ===== 主同步逻辑 =====

async def sync_channels():
    # ===== 配置命令行参数 =====
    parser = argparse.ArgumentParser(description='Telegram Sync Bot')
    parser.add_argument('--mode', type=str, help='同步模式 (1-4)')
    parser.add_argument('--test', action='store_true', help='作为测试同步执行 (数据隔离编号不污染库)')
    parser.add_argument('--clear-test', action='store_true', help='清除所有本地测试产生的数据')
    parser.add_argument('--folder', type=str, help='指定同步的文件夹名称 (用于局部模式)')
    parser.add_argument('--ids', type=str, help='指定同步的频道 ID 列表 (逗号分隔)')
    parser.add_argument('--rollback', type=str, help='指定回滚的目标版本 (如 TEST-1 或 #3)')
    parser.add_argument('--confirm', action='store_true', help='跳过手动确认')
    args = parser.parse_args()

    # ===== 处理回滚逻辑 =====
    if args.rollback:
        print(f"\n⏳ 开始尝试回滚到目标状态: {args.rollback}...")
        try:
            deleted_labels, msg_targets = db.rollback_to(args.rollback)
            if not deleted_labels:
                print("⚠️ 未发现需要回滚的历史记录 (目标可能是最新或不存在)。")
            else:
                # 1. 尝试连接 Telegram 撤回这些消息
                do_range_delete = False
                if isinstance(msg_targets, tuple):
                    min_id, max_id = msg_targets
                    if min_id is not None and max_id is not None:
                        do_range_delete = True
                        print(f"📡 正在从 Telegram 目标群组中按边界 {min_id} ~ {max_id} 批量撤销消息...")
                        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
                            print("\nLogged in. Warming up cache for accurate target entity resolution...")
                            await client.get_dialogs() # 预热实体缓存，防止 ValueError
                            try:
                                target_entity = await client.get_entity(TARGET_GROUP_ID)
                            except Exception as e:
                                print(f"❌ 无法解析 TARGET_GROUP_ID '{TARGET_GROUP_ID}': {e}。回滚终止，请手动删除消息。")
                                return
                                        
                            all_ids = list(range(min_id, max_id + 1))
                            chunk_size = 100
                            for i in range(0, len(all_ids), chunk_size):
                                chunk = all_ids[i:i + chunk_size]
                                try:
                                    await client.delete_messages(target_entity, chunk, revoke=True)
                                    print(f"    ➡️ 成功撤销物理区间批次: {chunk[0]} ~ {chunk[-1]}")
                                except Exception as e:
                                    print(f"    ⚠️ 撤销批次失败: {e}")
                                await asyncio.sleep(0.5)
                
                if not do_range_delete:
                    # 兼容/回退逻辑: 只有具体的 forwarded_msg_id 列表
                    fwd_ids = []
                    if isinstance(msg_targets, dict):
                        fwd_ids = msg_targets.get("target_group", [])
                    
                    if fwd_ids:
                        print(f"📡 正在从 Telegram 目标群组中根据记录 ID 撤销 {len(fwd_ids)} 条已转发的消息...")
                        async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
                            print("\nLogged in. Warming up cache for accurate target entity resolution...")
                            await client.get_dialogs()
                            try:
                                target_entity = await client.get_entity(TARGET_GROUP_ID)
                            except Exception as e:
                                print(f"❌ 无法解析 TARGET_GROUP_ID '{TARGET_GROUP_ID}': {e}。回滚终止，请手动删除消息。")
                                return
                                        
                            chunk_size = 100
                            for i in range(0, len(fwd_ids), chunk_size):
                                chunk = fwd_ids[i:i + chunk_size]
                                try:
                                    await client.delete_messages(target_entity, chunk, revoke=True)
                                    print(f"    ➡️ 成功撤销 ID 批次: {i} ~ {i + len(chunk) - 1}")
                                except Exception as e:
                                    print(f"    ⚠️ 撤销批次失败: {e}")
                                await asyncio.sleep(0.5)
                            
                # 2. 清理对应物理日志文件 (JSON & Markdown)
                print(f"✅ 数据库关联记录已擦除，准备清理对应物理日志文件 (Labels: {deleted_labels})...")
                # 统一清理 data/archived/logs (JSON) 和 docs/archived/logs (MD)
                for root_dir in ['data/archived/logs', 'docs/archived/logs']:
                    if not os.path.exists(root_dir): continue
                    for dirpath, dirnames, filenames in os.walk(root_dir):
                        # 保护 backups 文件夹
                        if 'backups' in dirpath.lower():
                            continue
                            
                        for f in filenames:
                            delete_this = False
                            # 1. 匹配 JSON: sync_#1.json, sync_TEST-1.json
                            for lbl in deleted_labels:
                                if f == f"sync_{lbl}.json":
                                    delete_this = True
                                    break
                                # 2. 匹配 Markdown (由 update_docs 生成): sync_#1_20260222_141820.md
                                # 注意 update_docs 可能为了安全转换了 label (如 #1 -> 1)
                                safe_lbl = lbl.replace('#', '')
                                if f.startswith(f"sync_{lbl}_") and f.endswith(".md"):
                                    delete_this = True
                                    break
                                if f.startswith(f"sync_{safe_lbl}_") and f.endswith(".md"):
                                    delete_this = True
                                    break
                            
                            if delete_this:
                                file_path = os.path.join(dirpath, f)
                                try:
                                    os.remove(file_path)
                                    print(f"  🗑️ 已删除废弃日志: {file_path}")
                                except Exception as e:
                                    print(f"  ⚠️ 删除失败 {file_path}: {e}")
                print(f"\n🎯 回滚成功！已抹除版本: {', '.join(deleted_labels)}")
        except Exception as e:
            print(f"❌ 回滚失败: {str(e)}")
        return

    # ===== 处理测试清理逻辑 =====
    if args.clear_test:
        print("\n🗑️ 开始清理本地测试数据...")
        db.clear_test_data()
        for root_dir in ['data/archived/logs', 'docs/archived/logs']:
            if not os.path.exists(root_dir): continue
            for dirpath, dirnames, filenames in os.walk(root_dir):
                for f in filenames:
                    if 'TEST-' in f:
                        file_path = os.path.join(dirpath, f)
                        try:
                            os.remove(file_path)
                            print(f"  🗑️ 已删除测试文件: {file_path}")
                        except Exception as e:
                            pass
        print("✅ 清理完毕！ (如需清理群内测试转发，请在 Telegram 内手动删除)")
        return

    # ===== 选择同步模式 =====
    if args.mode:
        choice = args.mode
    else:
        print("\n请选择同步模式：")
        print("1. 局部更新同步 (增量, 按分组)")
        print("2. 局部全时间轴同步 (从 #1 开始, 按分组)")
        print("3. 全局更新同步 (增量, 全频道)")
        print("4. 全局全时间轴同步 (从 #1 开始, 全频道)")
        print("5. 高级回滚 (撤销到指定的历史版本)")
        
        choice = input("\n请输入选项 (1-5, 默认 1): ").strip() or "1"
        
    if choice == "5":
        target = input("👉 请输入要保留的最终版本 (例如: TEST-1 或 #3): ").strip()
        if not target:
            print("❌ 操作取消。")
            return
        
        print(f"\n⏳ 开始尝试回滚到目标状态: {target}...")
        try:
            deleted_labels, msg_targets = db.rollback_to(target)
            if not deleted_labels:
                print("⚠️ 未发现需要回滚的历史记录 (目标可能是最新或不存在)。")
            else:
                fwd_ids = msg_targets.get("target_group", [])
                if fwd_ids:
                    print(f"📡 正在从 Telegram 目标群组中撤销 {len(fwd_ids)} 条已转发的消息...")
                    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
                        target_entity = await client.get_entity(TARGET_GROUP_ID)
                        chunk_size = 100
                        for i in range(0, len(fwd_ids), chunk_size):
                            chunk = fwd_ids[i:i + chunk_size]
                            try:
                                await client.delete_messages(target_entity, chunk)
                                print(f"    ➡️ 成功撤销批次: {i} ~ {i + len(chunk) - 1}")
                            except Exception as e:
                                print(f"    ⚠️ 撤销批次失败: {e}")
                            await asyncio.sleep(1)
                            
                print(f"✅ 数据库关联记录已擦除，准备清理对应物理日志文件...")
                for root_dir in ['data/archived/logs', 'docs/archived/logs']:
                    if not os.path.exists(root_dir): continue
                    for dirpath, dirnames, filenames in os.walk(root_dir):
                        for f in filenames:
                            if any(f"_{lbl}." in f or f"_{lbl}_" in f or lbl == f.split('.')[0] for lbl in deleted_labels):
                                file_path = os.path.join(dirpath, f)
                                try:
                                    os.remove(file_path)
                                    print(f"  🗑️ 已删除废弃日志: {file_path}")
                                except Exception as e:
                                    pass
                print(f"\n🎯 回滚成功！已抹除版本: {', '.join(deleted_labels)}")
        except Exception as e:
            print(f"❌ 回滚失败: {str(e)}")
        return
    
    IS_TEST = args.test
    if not args.mode and not IS_TEST and choice in ["1", "2", "3", "4"]:
        is_t = input("\n⚠️ 是否作为测试同步执行？(如果开启, 将采用独立计数编号, 不会推进您的正式同步起止点) (y/N): ").strip().lower()
        if is_t == 'y':
            IS_TEST = True
    
    use_offset = (choice in ["1", "3"])
    only_organize = (choice in ["1", "2"])
    
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        print("\nLogged in. Warming up cache...")
        await client.get_dialogs() # 预热实体缓存，防止 ValueError
        me = await client.get_me()
        my_id = me.id
        print(f"Starting sync... (User: {me.first_name}, ID: {my_id})")
        
        # 获取所有文件夹过滤器 (用于局部同步过滤)
        target_folder_name = args.folder or "整理"
        folder_chats = [] # 存储选定文件夹中的实体
        all_filters = []
        try:
            filters_resp = await client(functions.messages.GetDialogFiltersRequest())
            all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
            for f in all_filters:
                title = getattr(f, 'title', None)
                t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
                if target_folder_name in t_str and hasattr(f, 'include_peers'):
                    print(f"📂 发现「{t_str}」文件夹")
                    for peer in f.include_peers:
                        try:
                            # 直接获取实体，此时应该已经在缓存中了
                            e = await client.get_entity(peer)
                            # 过滤掉归档的对话（如果文件夹中包含归档项）
                            if getattr(e, 'archived', False):
                                print(f"  🚫 排除已归档项目: {getattr(e, 'title', str(e.id))}")
                                continue
                            folder_chats.append(e)
                        except Exception as ee:
                            print(f"  ⚠️ 无法识别文件夹中的 Peer: {peer}, error: {ee}")
            
            if only_organize:
                print(f"🎯 已锁定「{target_folder_name}」相关文件夹中 {len(folder_chats)} 个有效频道。")
        except Exception as e:
            print(f"⚠️ 获取文件夹信息出现偏差: {e}")

        # ===== 中断信号初始化 =====
        STOP_FLAG = 'data/temp/stop_sync.flag'
        if os.path.exists(STOP_FLAG):
            try: os.remove(STOP_FLAG)
            except: pass

        sync_start = datetime.now()
        run_id = db.start_sync_run(is_test=IS_TEST)
        run_label = db.get_run_label(run_id)
        print(f"📋 同步号: {run_label} (run_id={run_id})")

        # 获取目标群组（直接 get_entity 最可靠，避免 dialog.id 格式不匹配）
        target_entity = None
        try:
            target_entity = await client.get_entity(TARGET_GROUP_ID)
            print(f"✅ Target: {getattr(target_entity, 'title', target_entity)} (id={target_entity.id})")
        except Exception as e_ge:
            print(f"⚠️ get_entity({TARGET_GROUP_ID}) failed: {e_ge}, falling back to name search...")
            async for dialog in client.iter_dialogs():
                if dialog.name == "我的私密视频库":
                    target_entity = dialog.entity
                    print(f"✅ Target found by name: {dialog.name}")
                    break
        if not target_entity:
            print("❌ 目标群组未找到。")
            return
        
        final_target_id = target_entity.id
        norm_target_id = normalize_tg_id(final_target_id)
        norm_env_target_id = normalize_tg_id(TARGET_GROUP_ID)
        print(f"🎯 目标库标称 ID: {TARGET_GROUP_ID}, 实时 ID: {final_target_id}, 标准归一化: {norm_target_id}")

        # 1. 发送同步开始的消息头（作为回滚的起始物理边界）
        start_header = [
            f"🚀 **同步流水线启动**",
            f"🔢 同步号: `{run_label}`",
            f"⏰ 启动时间: {sync_start.strftime('%Y-%m-%d %H:%M:%S')}",
            f"━━━━━━━━━━━━━━━━"
        ]
        start_msg = await client.send_message(target_entity, "\n".join(start_header))
        run_first_target_msg_id = start_msg.id
        print(f"📍 起点边界已确立: {run_first_target_msg_id}")

        # 全局统计
        g = {'groups': 0, 'videos': 0, 'photos': 0, 'files': 0, 'gifs': 0, 'links': 0, 'link_msgs': 0, 'previews': 0, 'texts': 0, 'skipped': 0}
        source_stats = {}
        msg_id_ranges = {} # channel -> (min, max)
        
        # (Target entity block moved up)

        # 确定最终要同步的频道列表
        effective_entities = []
        seen_ids = set()
        
        # --- 方案 A: 如果提供了具体的 IDs，则直接使用这些 ---
        if args.ids:
            target_ids = [s.strip() for s in args.ids.split(',') if s.strip()]
            print(f"🎯 检测到指定 ID 列表: {target_ids}")
            for tid in target_ids:
                try:
                    # 尝试转换成 int (如果是纯数字 ID)
                    e_id = int(tid) if (tid.startswith('-') or tid.isdigit()) else tid
                    e = await client.get_entity(e_id)
                    if e.id not in seen_ids:
                        effective_entities.append(e)
                        seen_ids.add(e.id)
                except Exception as ee:
                    print(f"  ⚠️ 无法获取指定 ID 实体 [{tid}]: {ee}")
            print(f"🚀 精确同步模式 | 目标: {len(effective_entities)} 个频道")
        
        # --- 方案 B: 原有的文件夹/全局同步逻辑 (仅在没有 --ids 时执行) ---
        elif only_organize:
            # 过滤来源
            for e in folder_chats:
                if e.id in seen_ids: continue
                
                n_id = normalize_tg_id(e.id)
                e_title = getattr(e, 'title', '') or getattr(e, 'first_name', '') or str(e.id)
                
                # --- 增强过滤逻辑 ---
                # 1. 排除目标仓库 (Loop Protection)
                if n_id == norm_target_id or n_id == norm_env_target_id or '私密视频库' in e_title:
                    print(f"  🚫 排除目标仓库: {e_title} ({e.id})")
                    continue
                # 2. 排除官方账号 / 机器人 / 保存的消息 / 已归档
                is_archived = getattr(e, 'archived', False)
                if e.id == 777000 or getattr(e, 'bot', False) or e.id == my_id or is_archived:
                    print(f"  🚫 排除特殊分类(含归档): {e_title} ({e.id})")
                    continue
                # 3. 严格限制仅同步 频道 (Channel)、群组 (Megagroup/Chat) 和 私聊 (User)
                is_channel = getattr(e, 'broadcast', False)
                is_group = getattr(e, 'megagroup', False) or (type(e).__name__ == 'Chat')
                is_user = (type(e).__name__ == 'User')
                
                if not (is_channel or is_group or is_user):
                    print(f"  🚫 排除无效实体类型: {e_title} ({e.id})")
                    continue
                
                effective_entities.append(e)
                seen_ids.add(e.id)
            print(f"🚀 模式: 局部同步 | 目标: {len(effective_entities)} 个频道")
        else:
            # 全局同步：使用环境变量中的列表
            for s in SOURCE_CHANNELS:
                s = s.strip()
                if not s: continue
                try:
                    e = await client.get_entity(int(s) if (s.startswith('-') or s.isdigit()) else s)
                    if e.id in seen_ids: continue
                    
                    n_id = normalize_tg_id(e.id)
                    title = getattr(e, 'title', '') or getattr(e, 'first_name', '') or str(e.id)
                    
                    # 1. 排除目标仓库
                    if n_id == norm_target_id or n_id == norm_env_target_id or '私密视频库' in title:
                        print(f"  🚫 排除目标仓库: {title} ({e.id})")
                        continue
                    # 2. 排除特殊分类 (含归档)
                    is_archived = getattr(e, 'archived', False)
                    if e.id == 777000 or getattr(e, 'bot', False) or e.id == my_id or is_archived:
                        print(f"  🚫 排除特殊分类(含归档): {title} ({e.id})")
                        continue
                    # 3. 严格限制类型
                    is_channel = getattr(e, 'broadcast', False)
                    is_group = getattr(e, 'megagroup', False) or (type(e).__name__ == 'Chat')
                    if not (is_channel or is_group):
                        print(f"  🚫 排除非广播实体: {title} ({e.id})")
                        continue
                        
                    effective_entities.append(e)
                    seen_ids.add(e.id)
                except Exception as ee:
                    print(f"  ⚠️ 无法获取全局频道 {s}: {ee}")
            print(f"🚀 模式: 全局同步 | 目标: {len(effective_entities)} 个频道")

        # --- 确认环节 ---
        if not args.confirm:
            print("\n🚨 即将开始同步以下频道：")
            for idx, e in enumerate(effective_entities, 1):
                title = getattr(e, 'title', '') or getattr(e, 'first_name', '')
                print(f"  {idx}. {title} ({e.id})")
            
            confirm = input(f"\n确认开始同步这 {len(effective_entities)} 个源吗？ (y/N): ").lower()
            if confirm != 'y':
                print("❌ 已取消同步。")
                return
        
        # 逐源同步
        interrupted = False
        for entity in effective_entities:
            # 检查中断信号
            if os.path.exists(STOP_FLAG):
                print(f"🛑 收到停止信号 ({STOP_FLAG})，终止后续源的同步。")
                break
                
            # 检查全平台封禁
            restriction_reasons = getattr(entity, 'restriction_reason', []) or []
            is_globally_banned = any(
                getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'
                for r in restriction_reasons
            )
            if is_globally_banned:
                ban_name = getattr(entity, 'title', str(entity.id))
                print(f"  🚫 [全平台封禁] {ban_name}，已跳过同步。")
                if 'skipped_banned' not in locals(): skipped_banned = []
                skipped_banned.append(ban_name)
                continue
            chat_id = utils.get_peer_id(entity)
            current_title = getattr(entity, 'title', None) or getattr(entity, 'first_name', '') or str(chat_id)
            
            # [NEW] 检查并执行可能的跨系统改名，保持与历史记录连贯
            old_title, new_title = db.check_and_update_channel_name(chat_id, current_title)
            if old_title != new_title:
                print(f"\n📢 检测到频道改名: '{old_title}' -> '{new_title}'")
                rename_channel_archives(old_title, new_title)
                
            source_name = new_title
                        
            print(f"\n>>> 开始同步: {source_name} ({chat_id})")
            
            try:
                # Get folder category
                folder_name = "未分类"
                for f in all_filters:
                    title = getattr(f, 'title', None)
                    if not title or not hasattr(f, 'include_peers'): continue
                    t_str = title.text if hasattr(title, 'text') else str(title)
                    for peer in f.include_peers:
                        pid = getattr(peer, 'channel_id', getattr(peer, 'chat_id', getattr(peer, 'user_id', None)))
                        if not pid: continue
                        # 兼容性比较：忽略符号与 -100 前缀 
                        if str(abs(chat_id)).endswith(str(abs(pid))) or str(abs(pid)).endswith(str(abs(chat_id))):
                            folder_name = t_str
                            break
                    if folder_name != "未分类": break

                if not use_offset:
                    # [NEW] Full Sync collision check
                    existing_offset = db.get_last_offset(chat_id, is_test=IS_TEST)
                    if existing_offset > 0:
                        print(f"  ⚠️ [高危操作警告] 频道 '{source_name}' 已存在历史同步记录 (截至 #{existing_offset})。")
                        print(f"  强制执行【全时间轴同步】将导致以下副作用：")
                        print(f"    1. 私密群组中可能出现大量重复转发的消息。")
                        print(f"    2. 重新编排的最新资源编号体系，将使旧的本地日志文件 (docs/logs) 变为无意义的参考。")
                        print(f"  更安全的做法是使用【高级回滚 (5)】回滚到起点后再进行全量同步。")
                        if not getattr(args, 'confirm', False):
                            confirm_override = input(f"\n  确认无视副作用，强制重置基准线并进行全量同步吗？ (y/N): ").lower()
                            if confirm_override != 'y':
                                print(f"  ⏭️ 已跳过对 {source_name} 的强制全量同步。")
                                continue
                            
                    last_id = 0
                    print(f"  🧹 全时间轴模式: 执行基准线重置，自动清理本地残留序列与关联纪元...")
                    db.reset_channel_sync(chat_id, IS_TEST)
                else:
                    last_id = db.get_last_offset(chat_id, is_test=IS_TEST)
                
                s = {'groups': 0, 'videos': 0, 'photos': 0, 'files': 0, 'gifs': 0, 'links': 0, 'link_msgs': 0, 'previews': 0, 'texts': 0, 'skipped': 0}
                local_records = []  # 本地存档
                group_index = 0
                
                print(f"\n📡 [{source_name}] 从 msg #{last_id} 开始同步...")
                
                pending_group = []
                current_group_id = None
                max_msg_id = last_id
                min_source_msg_id = None

                # 限制迭代器行为：使用 min_id 确保增量，reverse=True 确保按时间正序
                msg_count = 0
                print(f" DEBUG: Iterating messages for {source_name} (ID: {chat_id}) with min_id={last_id}")
                async for message in client.iter_messages(entity, min_id=last_id, reverse=True):
                    # print(f" DEBUG: Found message #{message.id}")
                    # 细粒度中断检查（每处理 10 条消息检查一次）
                    msg_count += 1
                    if msg_count % 10 == 0 and os.path.exists(STOP_FLAG):
                        print(f"  🛑 中断：[{source_name}] 处理中途退出...")
                        interrupted = True
                        break

                    if min_source_msg_id is None: min_source_msg_id = message.id
                    msg_type = classify_message(message)
                    
                    if msg_type == 'skip':
                        s['skipped'] += 1
                        if message.id > max_msg_id: max_msg_id = message.id
                        continue
                    
                    if msg_type in ('text', 'link', 'link_preview'):
                        if pending_group:
                            group_index += 1
                            result = await flush_media_group(
                                client, target_entity, pending_group,
                                source_name, chat_id, db, run_id, run_label, group_index, local_records
                            )
                            s['groups'] += 1; s['videos'] += result['videos']; s['photos'] += result['photos']; s['files'] += result['files']; s['gifs'] += result['gifs']; s['previews'] += result['previews']; s['links'] += result['url_count']; s['link_msgs'] += result['link_msg_count']
                            pending_group = []; current_group_id = None
                        
                        group_index += 1 
                        await forward_text(client, target_entity, message, source_name, chat_id, db, run_id, run_label, group_index, local_records)
                        if msg_type in ('text', 'link'):
                            s['texts'] += 1
                        elif msg_type == 'link_preview':
                            s['previews'] += 1  # 带预览图的链接计入带资源类 previews
                        # URL统计
                        msg_url_cnt = count_urls(message)
                        s['links'] += msg_url_cnt
                        if msg_url_cnt > 0:
                            s['link_msgs'] += 1
                        if message.id > max_msg_id: max_msg_id = message.id
                        continue

                    # 媒体逻辑
                    msg_group_id = message.grouped_id
                    is_new_group = False
                    if pending_group:
                        if msg_group_id is None or current_group_id is None:
                            is_new_group = True
                        elif msg_group_id != current_group_id:
                            is_new_group = True

                    if is_new_group:
                        group_index += 1
                        result = await flush_media_group(
                            client, target_entity, pending_group,
                            source_name, chat_id, db, run_id, run_label, group_index, local_records
                        )
                        s['groups'] += 1; s['videos'] += result['videos']; s['photos'] += result['photos']; s['files'] += result['files']; s['gifs'] += result['gifs']; s['previews'] += result['previews']; s['links'] += result['url_count']; s['link_msgs'] += result['link_msg_count']
                        pending_group = []

                    current_group_id = msg_group_id
                    pending_group.append(message)
                    if message.id > max_msg_id: max_msg_id = message.id

                # 处理收尾
                if pending_group:
                    group_index += 1
                    result = await flush_media_group(
                        client, target_entity, pending_group,
                        source_name, chat_id, db, run_id, run_label, group_index, local_records
                    )
                    s['groups'] += 1; s['videos'] += result['videos']; s['photos'] += result['photos']; s['files'] += result['files']; s['gifs'] += result['gifs']; s['previews'] += result['previews']; s['links'] += result['url_count']; s['link_msgs'] += result['link_msg_count']

                # 记录该频道的抓取范围
                if min_source_msg_id:
                    msg_id_ranges[source_name] = (min_source_msg_id, max_msg_id)

                if max_msg_id > last_id:
                    db.update_offset(chat_id, max_msg_id, is_test=IS_TEST)

                # [NEW] 提取频道级别的资源号边界
                ch_res = {'total': [], 'video': [], 'photo': [], 'gif': [], 'other': [], 'link': [], 'link_msg': [], 'preview': [], 'text': [], 'res_msg': []}
                for rec in local_records:
                    ids = rec.get("res_ids")
                    if ids:
                        for k in ch_res:
                            v = ids.get(k)
                            if v is not None:
                                if isinstance(v, list):
                                    ch_res[k].extend(v)
                                else:
                                    ch_res[k].append(v)
                ch_bounds = {}
                for k, lst in ch_res.items():
                    valid = [x for x in lst if x is not None]
                    ch_bounds[k] = (min(valid), max(valid)) if valid else None
                s['bounds'] = ch_bounds

                save_to_local_archive(source_name, run_label, local_records, folder_name)
                source_stats[source_name] = s
                for k in g: g[k] += s[k]
                
                sm = s['videos'] + s['photos'] + s['files'] + s['gifs'] + s['previews']
                total_ch = s['groups'] + s['previews'] + s['texts']
                print(f"📦 [{source_name}] 总{total_ch}条 | 资源{s['groups']}+{s['previews']} | 文本{s['texts']} | 资源{sm}(🎬{s['videos']} 🖼️{s['photos']} 🎞️{s['gifs']} 👁‍🗨️{s['previews']} 📄{s['files']}) | 📎{s['link_msgs']} | ⏭️{s['skipped']}")
                
                if interrupted: break

            except Exception as e:
                print(f"❌ Error syncing {source_name}: {e}")
                traceback.print_exc()
                with open('data/temp/sync_error.log', 'a', encoding='utf-8') as f:
                    f.write(f"\n--- {source_name} ---\n")
                    traceback.print_exc(file=f)
        
        # 3. 完成汇总与边界持久化
        sync_end = datetime.now()
        duration = str(sync_end - sync_start).split('.')[0]
        
        # 获取资源 ID 范围
        res_ranges = db.finish_sync_run(run_id, {'duration': duration, **g}) # 注意： finish_sync_run 没改，但我改了 set_sync_run_boundaries 
        # 重调一下记录边界的函数以获取资源 ID 范围 (修正后的 DB 接口)
        # res_ranges 包含: (min_res, max_res, min_vid, max_vid, min_pho, max_pho, min_txt, max_txt)
        
        total_resources = g['videos'] + g['photos'] + g['files'] + g['gifs'] + g['previews']
        total_with_res = g['groups'] + g['previews']
        total_effective_msgs = total_with_res + g['texts']
        
        status_line = "✅ 同步完成！" if not interrupted else "⚠️ 同步任务已人工提前中断"
        report = [
            f"{status_line} [{run_label}]",
            f"⏱️ 耗时: {duration}",
            "",
            f"📊 **全局统计汇总**:",
            f"  - 总消息数量: {total_effective_msgs}",
            f"  - 带资源消息: {total_with_res} ({g['groups']}组 + {g['previews']}预览)",
            f"  - 文本消息数量: {g['texts']}",
            f"  - 资源总量: {total_resources} (🎬:{g['videos']} 🖼️:{g['photos']} 🎞️:{g['gifs']} 👁‍🗨️:{g['previews']} 📄:{g['files']})",
            f"  - 链接总数: {g['links']} 🔗",
            f"  - 携带链接消息: {g['link_msgs']} 📎",
        ]
        # [NEW] 分群统计明细
        report.append("\n📈 **各频道统计明细**:")
        for name, st in source_stats.items():
            tot_res = st['videos'] + st['photos'] + st['files'] + st['gifs'] + st['previews']
            tot_w_res = st['groups'] + st['previews']
            tot_msg = tot_w_res + st['texts']
            report.append(f"  **{name}**")
            report.append(f"    - 总消息数量: {tot_msg}")
            report.append(f"    - 带资源消息: {tot_w_res} ({st['groups']}组 + {st['previews']}预览)")
            report.append(f"    - 文本消息数量: {st['texts']}")
            report.append(f"    - 资源总量: {tot_res} (🎬:{st['videos']} 🖼️:{st['photos']} 🎞️:{st['gifs']} 👁‍🗨️:{st['previews']} 📄:{st['files']})")
            report.append(f"    - 链接总数: {st['links']} 🔗")
            report.append(f"    - 携带链接消息: {st['link_msgs']} 📎")
            if st['skipped'] > 0:
                report.append(f"    - 跳过无用记录: {st['skipped']} 条 (广告/通知/空消息)")

        report.append(f"\n📋 **各频道同步消息范围 (Source IDs)**:")
        for name, r in msg_id_ranges.items():
            report.append(f"  • {name}: `#{r[0]}` ~ `#{r[1]}`")
        
        report.append("\n📋 **对话资源号范围 (Resource IDs)**:")
        # 再次调用 set_sync_run_boundaries 就是为了获取最终的全局 res_info
        temp_summary_text = "\n".join(report) + "\n(⏳ 正在计算编号范围...)"
        summary_msg = await client.send_message(target_entity, temp_summary_text)
        run_last_target_msg_id = summary_msg.id
        
        # 确立物理总结边界并获取全局编号范围
        res_info = db.set_sync_run_boundaries(run_id, run_first_target_msg_id, run_last_target_msg_id)
        
        if res_info:
            # [FIX] 移除无意义的“全局整合边界”，因为现在所有范围 ID 都是各群独立的

            # [NEW] 输出各频道分类的编号范围
            for name, st in source_stats.items():
                bounds = st.get('bounds')
                if not bounds: continue
                # 如果这个源有生成任何资源号才输出
                if any(bounds.values()):
                    report.append(f"\n  📁 **【{name}】分类编号**")
                    def _fb(k):
                        b = bounds.get(k)
                        return f"`#{b[0]}-#{b[1]}`" if b else "-"
                    if bounds['res_msg']: report.append(f"    📦 资源消息: {_fb('res_msg')}")
                    if bounds['video']: report.append(f"    🎬 视频号: {_fb('video')}")
                    if bounds['photo']: report.append(f"    🖼️ 图片号: {_fb('photo')}")
                    if bounds['gif']: report.append(f"    🎞️ GIF号: {_fb('gif')}")
                    if bounds['other']: report.append(f"    📄 文件号: {_fb('other')}")
                    if bounds['preview']: report.append(f"    👁‍🗨️ 预览链接号: {_fb('preview')}")
                    if bounds['link']: report.append(f"    🔗 链接号: {_fb('link')}")
                    if bounds['link_msg']: report.append(f"    📎 带链接消息: {_fb('link_msg')}")
                    if bounds['text']: report.append(f"    ✍️ 文字号: {_fb('text')}")
                    if bounds['total']: report.append(f"    🔢 总编号: {_fb('total')}")

            report.append(f"\n📏 **物理边界 (Target IDs)**: `#{run_first_target_msg_id}` ~ `#{run_last_target_msg_id}`")
        
        # 更新最终总结消息
        await client.edit_message(target_entity, summary_msg, "\n".join(report))
        print(f"🏁 [{run_label}] 同步结束。物理边界: {run_first_target_msg_id} ~ {run_last_target_msg_id}")

        # 🤖 自动触发文档更新 (由 sync.py 驱动，确保 Docs 与 Data 同步)
        print("🔃 正在自动更新本地文档存档 (Markdown)...")
        try:
            py = sys.executable  # 确保使用当前 Python 解释器（兼容 venv）
            # 注意：update_docs.py 现在和 sync.py 在同一个子目录下
            base_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(base_dir, "update_docs.py")
            # 1. 准备环境 (扫描文件夹结构)
            p1 = await asyncio.create_subprocess_shell(f'"{py}" "{script_path}" --prepare', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, err1 = await p1.communicate()
            if err1 and err1.strip(): print(f"  ⚠️ update_docs --prepare stderr: {err1.decode('utf-8', errors='replace')}")
            # 2. 生成全量日志
            p2 = await asyncio.create_subprocess_shell(f'"{py}" "{script_path}"', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, err2 = await p2.communicate()
            if err2 and err2.strip(): print(f"  ⚠️ update_docs stderr: {err2.decode('utf-8', errors='replace')}")
            print("✅ 本地文档存档已对齐。")
        except Exception as e_docs:
            print(f"⚠️ 自动更新文档失败: {e_docs}")


# ===== 转发函数 =====

async def flush_media_group(client, target_entity, messages, source_name, chat_id, db, run_id, run_label, group_index, local_records):
    """转发一组媒体（保留相册），记录到 DB 和本地存档"""
    if not messages:
        return {'videos': 0, 'photos': 0, 'files': 0, 'gifs': 0, 'previews': 0, 'url_count': 0}

    first_msg = messages[0]
    from datetime import timedelta
    local_date = first_msg.date + timedelta(hours=8) if first_msg.date else None
    post_time = local_date.strftime("%Y-%m-%d %H:%M") if local_date else "未知"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 按7种类型计数
    type_counts = {}
    for m in messages:
        t = classify_message(m)
        type_counts[t] = type_counts.get(t, 0) + 1
    videos = type_counts.get('video', 0)
    photos = type_counts.get('photo', 0)
    files = type_counts.get('file', 0)
    gifs = type_counts.get('gif', 0)
    links = type_counts.get('link', 0) + type_counts.get('link_preview', 0)
    previews = type_counts.get('link_preview', 0)

    # 分配资源编号 (7种独立计数器 + 携带链接消息编号 + 消息级资源号)
    total_ids, video_ids, photo_ids, gif_ids = [], [], [], []
    link_ids, link_msg_ids, preview_ids, other_ids, res_msg_ids = [], [], [], [], []
    msg_res_map = {}
    is_test = run_label.startswith('TEST-')

    for i, m in enumerate(messages):
        m_type = classify_message(m)
        m_url_count = count_urls(m)
        # 仅在组内第一条消息触发“消息级”资源号计数
        ids = db.assign_resource_ids(chat_id, m.id, m_type, is_test=is_test, url_count=m_url_count, is_new_msg=(i == 0))
        if ids:
            msg_res_map[m.id] = ids
            if ids['total']: total_ids.append(ids['total'])
            if ids['video']: video_ids.append(ids['video'])
            if ids['photo']: photo_ids.append(ids['photo'])
            if ids['gif']: gif_ids.append(ids['gif'])
            if ids['link']: link_ids.extend(ids['link'])
            if ids['link_msg']: link_msg_ids.append(ids['link_msg'])
            if ids['preview']: preview_ids.append(ids['preview'])
            if ids['other']: other_ids.append(ids['other'])
            if ids['res_msg']: res_msg_ids.append(ids['res_msg'])
    
    # 继承第一条消息的 res_msg_id 给组内所有成员 (用于 DB 存档一致性)
    group_res_msg_id = res_msg_ids[0] if res_msg_ids else None
    
    def format_range(ids):
        if not ids: return ""
        ids = [i for i in ids if i is not None]
        if not ids: return ""
        if len(ids) == 1: return f"#{ids[0]}"
        # 如果是连续区间，展示为 #min-#max，否则展示列表缩写
        id_min, id_max = min(ids), max(ids)
        if len(ids) == (id_max - id_min + 1):
            return f"#{id_min}-#{id_max}"
        # 非连续则显示范围
        return f"#{id_min}-#{id_max} (共{len(ids)}项)"

    # 媒体组编号顺序: 组 > 资源消息号 > 文字(互斥) > 带链接消息 > 总资源统计 > 详情
    num_parts = []
    # 资源消息号 (补集逻辑: 媒体组必定是资源消息)
    if res_msg_ids: num_parts.append(f"📦 资源: `{format_range(res_msg_ids)}`")
    
    if link_msg_ids: num_parts.append(f"📎 带链接消息号: `{format_range(link_msg_ids)}`")
    
    # 总资源统计 (对文件的统计)
    if total_ids: num_parts.append(f"🔢 总资源号: `{format_range(total_ids)}`")
    if video_ids: num_parts.append(f"🎬 视频: `{format_range(video_ids)}`")
    if photo_ids: num_parts.append(f"🖼️ 图片: `{format_range(photo_ids)}`")
    if gif_ids: num_parts.append(f"🎞️ GIF: `{format_range(gif_ids)}`")
    if other_ids: num_parts.append(f"📄 文件: `{format_range(other_ids)}`")
    if preview_ids: num_parts.append(f"👁‍🗨️ 可预览链接号: `{format_range(preview_ids)}`")
    if link_ids: num_parts.append(f"🔗 链接号: `{format_range(link_ids)}`")
    num_header = " | ".join(num_parts)

    # 转发来源
    fwd_source = await get_fwd_source_name(client, first_msg)
    fwd_line = f"\n📨 转自: **{fwd_source}**" if fwd_source else ""

    # 来源信息头
    header = f"📌 来源: **{source_name}** | 🔢 同步号: `{run_label}`{fwd_line}\n📦 **第 {group_index} 组消息** | {num_header}\n🕐 原始发布: {post_time}\n━━━━━━━━━━━━━━━━"
    header_sent = await client.send_message(target_entity, header)
    header_msg_id = header_sent.id if hasattr(header_sent, 'id') else 0

    # 重新发送（非转发），隐藏来源标签，避免源频道删除后视频失效
    fwd_id = 0
    try:
        if len(messages) == 1:
            msg = messages[0]
            sent = await client.send_file(
                target_entity, msg.media, caption=safe_caption(msg.text)
            )
            fwd_id = sent.id if hasattr(sent, 'id') else 0
        else:
            media_list = [msg.media for msg in messages]
            last_text = ""
            for msg in reversed(messages):
                if msg.text:
                    last_text = msg.text
                    break
            sent = await client.send_file(
                target_entity, media_list, caption=safe_caption(last_text)
            )
            if isinstance(sent, list) and sent:
                fwd_id = sent[0].id
            elif hasattr(sent, 'id'):
                fwd_id = sent.id
    except Exception as e:
        print(f"  ⚠️ send_file failed: {e}")
        print(f"  ↪ falling back to forward...")
        try:
            forwarded = await client.forward_messages(target_entity, messages)
            if isinstance(forwarded, list) and forwarded:
                fwd_id = forwarded[0].id if hasattr(forwarded[0], 'id') else 0
            elif hasattr(forwarded, 'id'):
                fwd_id = forwarded.id
        except Exception as e2:
            print(f"    ⚠️ forward also failed: {e2}")


    # 保存每条消息到 DB 和本地记录
    for msg in messages:
        msg_type = classify_message(msg)
        text = msg.text or ""
        # 优先使用转发头名称作为 Creator，其次才是文本解析，最后 Unknown
        fwd_title = await get_fwd_source_name(client, msg)
        creator = fwd_title or extract_creator(text)
        
        from datetime import timedelta
        local_msg_date = msg.date + timedelta(hours=8) if msg.date else None
        orig_time = local_msg_date.strftime("%Y-%m-%d %H:%M:%S") if local_msg_date else ""
        sender = get_sender_name(msg)
        file_name = None
        if hasattr(msg, 'file') and msg.file and hasattr(msg.file, 'name') and msg.file.name:
            file_name = msg.file.name

        res_ids = msg_res_map.get(msg.id)
        # 链接号特殊处理：存储第一个号或范围字符串
        link_val = None
        if res_ids and res_ids['link']:
            link_val = f"{min(res_ids['link'])}-{max(res_ids['link'])}" if len(res_ids['link']) > 1 else str(res_ids['link'][0])

        db.save_message(
            sync_run_id=run_id, msg_type=msg_type,
            original_msg_id=msg.id, original_chat_id=chat_id,
            original_chat_name=source_name, forwarded_msg_id=fwd_id,
            sender_name=sender, original_time=orig_time,
            forwarded_time=now_str, text_content=text,
            creator=creator, group_index=group_index, file_name=file_name,
            res_id=res_ids['total'] if res_ids else None,
            res_photo_id=res_ids['photo'] if res_ids else None,
            res_video_id=res_ids['video'] if res_ids else None,
            res_gif_id=res_ids['gif'] if res_ids else None,
            res_link_id=link_val,
            res_link_msg_id=res_ids['link_msg'] if res_ids else None,
            res_preview_id=res_ids['preview'] if res_ids else None,
            res_other_id=res_ids['other'] if res_ids else None,
            res_msg_id=group_res_msg_id
        )
        db.save_global_message(
            chat_id=chat_id, chat_name=source_name, msg_id=msg.id,
            msg_type=msg_type, sender_name=sender, original_time=orig_time,
            text_content=text, file_name=file_name,
            media_group_id=str(msg.grouped_id) if msg.grouped_id else None,
            res_id=res_ids['total'] if res_ids else None,
            res_photo_id=res_ids['photo'] if res_ids else None,
            res_video_id=res_ids['video'] if res_ids else None,
            res_gif_id=res_ids['gif'] if res_ids else None,
            res_link_id=link_val,
            res_link_msg_id=res_ids['link_msg'] if res_ids else None,
            res_preview_id=res_ids['preview'] if res_ids else None,
            res_other_id=res_ids['other'] if res_ids else None,
            res_msg_id=group_res_msg_id
        )
        local_records.append({
            "type": msg_type, "msg_id": msg.id, "group": group_index,
            "sender": sender, "original_time": orig_time,
            "forwarded_time": now_str, "source": source_name,
            "text": text, "creator": creator, "file_name": file_name,
            "res_ids": res_ids
        })

    # 统计该组消息中携带的URL数和带链接消息数
    url_count = sum(count_urls(m) for m in messages)
    link_msg_count = sum(1 for m in messages if count_urls(m) > 0)

    parts = []
    if videos: parts.append(f"🎬视频{videos}")
    if photos: parts.append(f"🖼️图片{photos}")
    if gifs: parts.append(f"🎞️GIF{gifs}")
    if previews: parts.append(f"👁️预览{previews}")
    if files: parts.append(f"文件{files}")
    if url_count: parts.append(f"链接{url_count}")
    parts_str = " ".join(parts)
    
    print(f"  📦 G{group_index}({len(messages)}个: {parts_str}) #{first_msg.id} @ {post_time}")
    await asyncio.sleep(2)
    return {'videos': videos, 'photos': photos, 'files': files, 'gifs': gifs, 'previews': previews, 'url_count': url_count, 'link_msg_count': link_msg_count, 'header_msg_id': header_msg_id}

async def forward_text(client, target_entity, message, source_name, chat_id, db, run_id, run_label, group_index, local_records):
    """转发纯文字/链接/可预览链接消息"""
    from datetime import timedelta
    sender_name = get_sender_name(message)
    # message.date 是 UTC 时间，转换为北京时间 (UTC+8)
    local_date = message.date + timedelta(hours=8) if message.date else None
    post_time = local_date.strftime("%Y-%m-%d %H:%M") if local_date else "未知"
    orig_time = local_date.strftime("%Y-%m-%d %H:%M:%S") if local_date else ""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_test = run_label.startswith('TEST-')
    actual_type = classify_message(message)  # 'text', 'link', or 'link_preview'
    msg_url_count = count_urls(message)
    res_ids = db.assign_resource_ids(chat_id, message.id, actual_type, is_test=is_test, url_count=msg_url_count, is_new_msg=True)
    
    # 编号显示按用户偏好顺序: 组 > 资源号/文字号 > 带链接消息 > 总资源统计 > 分项
    num_parts = []
    if res_ids:
        if res_ids.get('res_msg'): num_parts.append(f"📦 资源: `#{res_ids['res_msg']}`")
        if res_ids.get('text'): num_parts.append(f"✍️ 文字: `#{res_ids['text']}`")
        if res_ids.get('link_msg'): num_parts.append(f"📎 带链接消息号: `#{res_ids['link_msg']}`")
        if res_ids.get('total'): num_parts.append(f"🔢 总资源号: `#{res_ids['total']}`")
        if res_ids.get('preview'): num_parts.append(f"👁‍🗨️ 可预览链接号: `#{res_ids['preview']}`")
        if res_ids.get('link'): 
            l_ids = res_ids['link']
            if len(l_ids) > 1:
                num_parts.append(f"🔗 链接号: `#{min(l_ids)}-#{max(l_ids)}`")
            else:
                num_parts.append(f"🔗 链接号: `#{l_ids[0]}`")
    num_str = f" | {' | '.join(num_parts)}" if num_parts else ""

    if actual_type == 'link_preview': icon = "👁‍🗨️"
    elif actual_type == 'link': icon = "🔗"
    else: icon = "💬"
    
    fwd_source = await get_fwd_source_name(client, message)
    fwd_line = f"\n📨 转自: **{fwd_source}**" if fwd_source else ""
    
    header = f"{icon} 来源: **{source_name}** | 🔢 同步号: `{run_label}`{fwd_line}\n📦 **第 {group_index} 组消息**{num_str}\n👤 发言者: {sender_name} | 🕐 时间: {post_time}\n━━━━━━━━━━━━━━━━"
    header_sent = await client.send_message(target_entity, header)
    text_header_msg_id = header_sent.id if hasattr(header_sent, 'id') else None

    # 获取消息文本，若 text 为空则从 entities 中提取 URL 作为回退
    send_text = (message.text or "").strip()
    if not send_text and message.entities:
        urls = []
        for ent in message.entities:
            if hasattr(ent, 'url') and ent.url:
                urls.append(ent.url)
        if urls:
            send_text = "\n".join(urls)
    if send_text:
        await client.send_message(target_entity, send_text)
    else:
        print(f"  ⚠️ 消息 #{message.id} 无可发送文本，跳过内容发送")

    # 链接号特殊处理：存储第一个号或范围字符串
    link_val = None
    if res_ids and res_ids['link']:
        link_val = f"{min(res_ids['link'])}-{max(res_ids['link'])}" if len(res_ids['link']) > 1 else str(res_ids['link'][0])

    text = message.text or send_text or ""

    db.save_message(
        sync_run_id=run_id, msg_type=actual_type,
        original_msg_id=message.id, original_chat_id=chat_id,
        original_chat_name=source_name, forwarded_msg_id=0,
        sender_name=sender_name, original_time=orig_time,
        forwarded_time=now_str, text_content=text,
        creator=extract_creator(text), group_index=group_index,
        res_id=res_ids['total'] if res_ids else None,
        res_link_id=link_val,
        res_link_msg_id=res_ids['link_msg'] if res_ids else None,
        res_preview_id=res_ids['preview'] if res_ids else None,
        res_text_id=res_ids['text'] if res_ids else None,
        res_msg_id=res_ids['res_msg'] if res_ids else None
    )
    db.save_global_message(
        chat_id=chat_id, chat_name=source_name, msg_id=message.id,
        msg_type=actual_type, sender_name=sender_name, original_time=orig_time,
        text_content=text, file_name=None,
        media_group_id=str(message.grouped_id) if message.grouped_id else None,
        res_id=res_ids['total'] if res_ids else None,
        res_link_id=link_val,
        res_link_msg_id=res_ids['link_msg'] if res_ids else None,
        res_preview_id=res_ids['preview'] if res_ids else None,
        res_text_id=res_ids['text'] if res_ids else None,
        res_msg_id=res_ids['res_msg'] if res_ids else None
    )
    local_records.append({
        "type": actual_type, "msg_id": message.id,
        "sender": sender_name, "original_time": orig_time,
        "forwarded_time": now_str, "source": source_name,
        "text": text, "res_ids": res_ids
    })

    type_label = {"link_preview": "👁‍🗨️预览链接", "link": "🔗链接", "text": "💬文字"}.get(actual_type, "💬")
    print(f"  {type_label} #{message.id} [{sender_name}] @ {post_time}")
    await asyncio.sleep(1)
    return text_header_msg_id


if __name__ == "__main__":
    asyncio.run(sync_channels())
