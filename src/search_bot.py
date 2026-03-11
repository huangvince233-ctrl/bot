import os
import sys
import asyncio
import re
from datetime import datetime
import signal
import random
import string
import time
import json
from telethon import TelegramClient, events, Button, functions, connection, utils
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from db import Database
from utils.config import CONFIG

# 机器人身份标识 (支持多进程隔离)
BOT_NAME = CONFIG['app_name']
API_ID = CONFIG['api_id']
API_HASH = CONFIG['api_hash']
BOT_TOKEN = CONFIG['bot_token']
TARGET_GROUP_ID = CONFIG['target_group_id']
ADMIN_IDS = CONFIG.get('admin_user_ids', [])
ADMIN_USER_ID = CONFIG.get('admin_user_id') # Keep for backwards compatibility/one-off notification

SESSION_NAME = 'data/sessions/copilot_user'

db = Database('data/copilot.db')
bot = TelegramClient(CONFIG.get('bot_session', 'data/sessions/copilot_bot'), API_ID, API_HASH, connection_retries=10, retry_delay=5)
user_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

sync_job_lock = asyncio.Lock()
db_lock = asyncio.Lock() # [NEW] 数据库全局锁，防止并发游标冲突
# 存储用户选择: {chat_id: {folder_name: set(selected_ids)}}
user_selections = {}
# 用于捕捉特定用户的交互状态 (如: 正在等待输入回滚版本号)
user_states = {}
# 用于存储用户的全局测试环境标量 (True为测试，False为正式)
user_env = {}

# 机器人自身信息，用于识别 @提到
me = None
p15_process = None  # [NEW] 存储 P1.5 Web Sorter 进程句柄

# 生成本次运行的唯一 ID (RunID)
def generate_run_id():
    chars = string.ascii_uppercase + string.digits
    suffix = ''.join(random.choices(chars, k=4))
    return f"{datetime.now().strftime('%m%d-%H%M')}-{suffix}"

RUN_ID = generate_run_id()

# 将 RunID 持久化到磁盘，供 send_offline.py 等外部脚本读取
RUN_ID_FILE = 'data/run_id.txt'
try:
    os.makedirs('data', exist_ok=True)
    with open(RUN_ID_FILE, 'w', encoding='utf-8') as f:
        f.write(RUN_ID)
except Exception as e:
    print(f"⚠️ 无法写入 RunID 文件: {e}")

async def init_bot():
    global me
    if not bot:
        print("❌ Error: bot instance is None in init_bot")
        return
    me = await bot.get_me()
    print(f"🤖 Bot started as @{me.username} (RunID: {RUN_ID})")
    
    # [NEW] 设置官方命令列表 (显示在附件图标旁的 Menu 按钮中)
    try:
        from telethon.tl.functions.bots import SetBotCommandsRequest
        from telethon.tl.types import BotCommand, BotCommandScopeDefault
        commands = [
            BotCommand('menu', '🏠 呼出主菜单'),
            BotCommand('sync', '🔄 进入同步管理'),
            BotCommand('backup', '💾 进入备份管理'),
            BotCommand('search', '🔍 搜索本地资源'),
            BotCommand('status', '📊 查看运行状态'),
            BotCommand('refresh', '🔄 刷新元数据归档 (分组/封禁状态)'),
            BotCommand('help', '❓ 查看帮助说明'),
            BotCommand('ping', '📡 活跃度测试'),
            BotCommand('stop', '🛑 中断当前运行的任务'),
            BotCommand('unlock', '🔓 强制解除锁表状态'),
            BotCommand('close', '🔐 停止并安全关闭机器人')
        ]
        # 使用空字符串作为默认语言，确保所有客户端都能看到
        await bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code='', commands=commands))
        # 额外为管理员也设置一遍，确保高优先级展示
        if ADMIN_IDS:
            from telethon.tl.types import BotCommandScopePeer
            for uid in ADMIN_IDS:
                try:
                    await bot(SetBotCommandsRequest(scope=BotCommandScopePeer(uid), lang_code='', commands=commands))
                except: pass
        print("✅ Official Bot Commands set successfully.")
    except Exception as e:
        print(f"⚠️ Failed to set bot commands: {e}")

    # 启动问候 (改为私发给管理员)
    try:
        if ADMIN_IDS:
            for uid in ADMIN_IDS:
                try: await bot.send_message(uid, f"🤖 **机器人已上线** (Ver: 03:30)\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⏰ 启动时间: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n💡 **小提示**：若您最近在 Telegram 手机端调整了频道分组、新增或删除了频道，建议发送 `/refresh` 立即刷新本地元数据档案与封禁状态。\n\n✅ 系统准备就绪。")
                except: pass
        else:
            print(f"🤖 Bot Online (RunID: {RUN_ID}) - No ADMIN_IDS configured to send DM.")
    except Exception as e:
        print(f"⚠️ 发送启动问候失败: {e}")

async def shutdown_handler():
    """优雅停机：发送下线通知并清理"""
    print(f"\n🛑 Shutting down (RunID: {RUN_ID})...")
    try:
        if ADMIN_IDS:
            for uid in ADMIN_IDS:
                try:
                    await asyncio.wait_for(bot.send_message(uid, f"🛑 **机器人正在下线**\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⚠️ 该实例已停止服务。"), timeout=3.0)
                except: pass
    except Exception as e:
        print(f"⚠️ 发送离线通知失败: {e}")
    
    # 停止所有正在运行的异步任务
    try:
        if bot and bot.is_connected():
            await asyncio.wait_for(bot.disconnect(), timeout=2.0)
        if user_client and user_client.is_connected():
            await asyncio.wait_for(user_client.disconnect(), timeout=2.0)
    except:
        pass
        
    # [NEW] 强力清理 Sorter 子进程
    global p15_process
    await _force_cleanup_sorter()
    if p15_process:
        try:
            p15_process.terminate()
        except:
            pass
        p15_process = None

    print("👋 Goodbye!")
    await asyncio.sleep(0.5) 
    os._exit(0) 

# ===== UI 增强工具：防洪与防重复渲染 =====

async def safe_answer(event, message="", alert=False):
    """安全的回调响应，如果不是回调查询则尝试以消息回复"""
    try:
        if hasattr(event, 'answer'):
            await event.answer(message, alert=alert)
        else:
            if message:
                await event.respond(message)
    except Exception as e:
        print(f"⚠️ safe_answer error: {e}")

async def safe_edit(event, text, buttons=None, parse_mode='md', alert_on_flood=True):
    """安全的消息编辑，内置 FloodWait 与 MessageNotModified 处理"""
    try:
        # 如果 text 超过 4096，只保留最后部分或截断
        if len(text) > 4000:
            text = text[:4000] + "\n\n...(内容过长已截断)..."
            
        return await event.edit(text, buttons=buttons, parse_mode=parse_mode)
    except Exception as e:
        err_str = str(e).lower()
        if 'not modified' in err_str or 'identical' in err_str:
            return  # 忽略相同内容的重复更新
        if 'flood' in err_str and 'wait' in err_str:
            # 提取等待秒数
            import re
            match = re.search(r'wait of (\d+) seconds', err_str)
            seconds = match.group(1) if match else "???"
            msg = f"⚠️ [Telegram 限制] 响应过快，请等待 {seconds} 秒再操作。"
            try:
                await event.answer(msg, alert=alert_on_flood)
            except: pass
            print(f"🚫 FloodWait: {err_str}")
        else:
            print(f"⚠️ UI Edit Error: {e}")
            try:
                # 最后的兜底：如果编辑失败且不是洪水，尝试直接回复一条新消息告知
                # (仅在非测试模式或管理员环境时可选用，此处保持简洁)
                pass
            except: pass

async def safe_respond(event, text, buttons=None, parse_mode='md'):
    """安全的消息回复，内置基础异常处理"""
    try:
        if len(text) > 4000:
            text = text[:4000] + "\n\n...(过长截断)..."
        return await event.respond(text, buttons=buttons, parse_mode=parse_mode)
    except Exception as e:
        print(f"⚠️ UI Respond Error: {e}")

async def render_main_menu(event, is_edit=False):
    """渲染全局主菜单"""
    buttons = [
        [Button.inline("🔄 1. 同步管理 (转发/增量)", b"nav_sync_main")],
        [Button.inline("💾 2. 备份管理 (历史记录/全局)", b"nav_backup")],
        [Button.inline("🔍 3. 搜索中心 (快捷检索)", b"nav_search_center")],
        [Button.inline("📥 4. 手动补充信息", b"nav_mode_4_start")],
        [Button.inline("🔄 刷新元数据归档", b"nav_refresh_metadata")],
        [Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    title = (
        "🏠 **Telegram Video Copilot 主菜单**\n\n"
        "请选择您要进行的操作：\n"
        "━━━━━━━━━━━━━━\n"
        "💡 **提示**：您也可以直接使用底部面板或输入 `/` 呼出命令。"
    )
    
    persistent_keyboard = [
        ['🏠 主菜单', '🔍 快捷检索'],
        ['🔄 刷新归档']
    ]
    
    try:
        if is_edit:
            await event.edit(title, buttons=buttons)
        else:
            # [OPTIMIZATION] 仅发送主菜单。持久化面板（ReplyKeyboard）通常只需在 /start 时发送一次
            # 只有当这是第一次进入或者显式请求时才发送提示
            await event.respond(title, buttons=buttons)
            
            # 如果是 /start 或明确没有看到面板，可以启用以下行，但平时不需要重复发送
            # if getattr(event, 'text', '').startswith('/start'):
            #     await event.respond("🕹️ 控制面板已激活", buttons=persistent_keyboard)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_main_menu error: {e}")

async def render_main_sync_menu(event, is_edit=False):
    chat_id = event.chat_id
    is_test = user_env.get(chat_id, False)
    env_badge = "🧪测试" if is_test else "🚀正式"
    toggle_label = "切换为🚀正式模式" if is_test else "切换为🧪测试模式"
    
    buttons = [
        [Button.inline("1. 局部更新 (按分组)", b"sync_1")],
        [Button.inline("2. 局部全时间轴 (按分组)", b"sync_2")],
        [Button.inline("3. 全局更新同步 (增量)", b"sync_3")],
        [Button.inline("4. 全局全时间轴同步", b"sync_4")],
        [Button.inline("5. 高级回滚", b"sync_5")],
        [Button.inline("6. 同步状态一览", b"sync_6")],
        [Button.inline("7. 🎯 目标群聊管理", b"nav_target_groups")],
        [Button.inline(f"🔄 {toggle_label}", b"sync_toggle_env_main")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")],
        [Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    
    title = f"🔄 请选择同步模式 [{env_badge}]："
    try:
        if is_edit:
            await event.edit(title, buttons=buttons)
        else:
            await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_main_sync_menu error: {e}")

@bot.on(events.NewMessage(pattern=r'/(?:sync|start|help|menu)$'))
@bot.on(events.NewMessage(pattern=r'^🏠 主菜单$'))
async def request_main_menu(event):
    cmd = event.text.split()[0].lower() if event.text else ""
    print(f"📥 Received {cmd} command from {event.chat_id}")
    
    if sync_job_lock.locked() and cmd != '🏠 主菜单':
        await safe_answer(event, '⚠️ 当前正在进行任务，请稍候。', alert=True)
        return

    if cmd == '/sync':
        await render_main_sync_menu(event, is_edit=False)
    else:
        # /start, /help, /menu, 🏠 主菜单 or other triggers
        await render_main_menu(event, is_edit=False)

async def show_help_message(event):
    help_text = (
        f"👋 **您好！我是您的 Telegram 视频搬运助手**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🆔 运行实例: `{RUN_ID}`\n"
        f"📂 本地归档: `docs/archived/`\n\n"
        f"💡 **使用建议**：\n"
        f"• 当您在手机端 **调整了频道分组、新增或删除频道** 后，请发送 `/refresh` 刷新本地元数据。\n"
        f"• 这样可以确保本地档案结构与 Telegram 实时同步，并自动探测频道的封禁状态。\n\n"
        f"请点击下方按钮或发送 `/menu` 开始操作："
    )
    await event.respond(help_text)

@bot.on(events.NewMessage(pattern='/unlock'))
async def force_unlock(event):
    if sync_job_lock.locked():
        sync_job_lock.release()
        await event.respond('🔓 已强制解除同步锁。如果刚才的任务卡住了，现在可以尝试重新启动。')
        print("🔓 Sync lock force released by user.")
    else:
        await event.respond('ℹ️ 当前没有正在进行的同步任务，无需解锁。')

@bot.on(events.NewMessage(pattern='/stop'))
async def stop_sync_job(event):
    STOP_FLAG = 'data/temp/stop_sync.flag'
    with open(STOP_FLAG, 'w') as f:
        f.write('stop')
    await event.respond('🛑 已发送中断信号。机器人将在完成当前消息块后停止并生成总结报告。')
    print("🛑 Stop signal requested by user via /stop.")
    
@bot.on(events.NewMessage(pattern='/close'))
async def close_bot_command(event):
    if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
        await event.respond('⚠️ 只有管理员可以执行关闭操作。')
        return
        
    await event.respond('🛑 收到关闭指令，正在下线...')
    await shutdown_handler()

@bot.on(events.NewMessage(pattern='/ping'))
async def ping_test(event):
    await event.respond(f'💓 **Pong!**\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⏰ 当前服务器时间: `{datetime.now().strftime("%H:%M:%S")}`\n我还在运行中，请指示。')

@bot.on(events.NewMessage(pattern='/target_groups'))
async def target_groups_cmd(event):
    if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
        await event.respond('⚠️ 只有管理员可以管理目标群组。')
        return
    await render_target_groups_ui(event)

async def render_target_groups_ui(event):
    groups = db.get_target_groups(BOT_NAME)
    active_group = db.get_active_target_group(BOT_NAME)
    
    lines = ["🎯 **目标群聊管理**\n\n请选择当前要转发到的目标群聊："]
    buttons = []
    
    for g in groups:
        is_active = (active_group and g['chat_id'] == active_group['chat_id'])
        mark = "✅ " if is_active else "⚪️ "
        buttons.append([
            Button.inline(f"{mark}{g['title']}", f"tgt_set_{g['chat_id']}".encode()),
            Button.inline("🗑️ 删除", f"tgt_del_{g['chat_id']}".encode())
        ])
    
    buttons.append([Button.inline("➕ 添加当前群聊为目标库", b"tgt_add_this")])
    buttons.append([Button.inline("⬅️ 返回同步菜单", b"nav_sync_main"), Button.inline("🗑️ 关闭", b"delete_menu")])
    
    text = "\n".join(lines)
    from telethon.errors.rpcerrorlist import MessageNotModifiedError
    try:
        if isinstance(event, events.CallbackQuery.Event):
            await event.edit(text, buttons=buttons)
        else:
            await event.respond(text, buttons=buttons)
    except MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b'nav_target_groups'))
async def nav_target_groups_callback(event):
    if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
        await event.answer('⚠️ 只有管理员可以管理目标群组。', alert=True)
        return
    await render_target_groups_ui(event)

@bot.on(events.CallbackQuery(data=re.compile(br'tgt_set_(.+)')))
async def tgt_set_callback(event):
    chat_id = int(event.data_match.group(1).decode())
    db.set_active_target_group(chat_id, BOT_NAME)
    await event.answer("✅ 目标群聊已切换", alert=True)
    await render_target_groups_ui(event)

@bot.on(events.CallbackQuery(data=re.compile(br'tgt_del_(.+)')))
async def tgt_del_callback(event):
    chat_id = int(event.data_match.group(1).decode())
    active_group = db.get_active_target_group(BOT_NAME)
    if active_group and active_group['chat_id'] == chat_id:
        await event.answer("⚠️ 无法删除当前正在使用（已激活）的目标群聊，请先切换到其他群聊。", alert=True)
        return
        
    db.delete_target_group(chat_id, BOT_NAME)
    await event.answer("✅ 目标群聊已删除", alert=False)
    await render_target_groups_ui(event)

@bot.on(events.CallbackQuery(data=b'tgt_add_this'))
async def tgt_add_this_callback(event):
    # 如果在私聊点这个，提示需要转发
    if event.is_private:
        await event.answer("💡 请在目标群聊中发送 /target_groups 或呼出主菜单点击添加。", alert=True)
    else:
        # 在群组里点，直接识别
        title = (await event.get_chat()).title
        
        # 检查是否已经存在
        existing_groups = db.get_target_groups(BOT_NAME)
        if any(g['chat_id'] == event.chat_id for g in existing_groups):
            await event.answer("⚠️ 当前群聊已经在目标列表中了！", alert=True)
            return
            
        db.register_target_group(event.chat_id, title, BOT_NAME)
        await event.answer(f"✅ 已添加目标群聊: {title}", alert=True)
        await render_target_groups_ui(event)

async def execute_sync(event, mode, folder=None, **kwargs):
    if sync_job_lock.locked():
        await safe_answer(event, '⚠️ 任务冲突：当前已有同步任务在运行中，请等其结束后再试。', alert=True)
        return

    async with sync_job_lock:
        # [NEW] 为了避免 subprocess (sync.py) 出现 "database is locked" (session文件冲突)
        # 必须在启动子进程前释放 user_client 的连接
        try:
            if user_client.is_connected():
                print("🔌 Releasing User Client session for sync task...")
                await user_client.disconnect()
        except: pass

        is_rollback = kwargs.get('rollback_target', None)
        is_test = kwargs.get('is_test', False)
        
        if is_rollback:
            msg = await event.respond(f'🚀 准备执行回滚指令: {is_rollback}...', 
                             buttons=[Button.inline('🛑 停止同步', data='stop_sync_instantly')])
            
            # [Phase 1] 利用主进程中尚未断开的 user_client 执行物理撤销
            try:
                await msg.edit(f'🔍 正在预检回滚目标: {is_rollback}...')
                deleted_labels, info = db.rollback_to(is_rollback, bot_name=CONFIG['app_name'], commit=False)
                # 从 db.rollback_to 的返回 info 中提取要物理删除的消息 ID 列表（兼容不同实现）
                ids_to_del = info.get('msg_ids_to_delete', []) if info else []
                
                if not deleted_labels:
                    await msg.edit("⚠️ 未发现需要回滚的历史记录。")
                    return

                # 如果预检未返回需要物理撤回的消息 ID（例如 messages 表中没有 forwarded_msg_id），
                # 则尝试退而求其次：在目标群里搜索包含该同步标签的消息文本，以便尽可能找到并撤回那些残留的转发。
                if not ids_to_del:
                    try:
                        active_tgt = db.get_active_target_group(CONFIG['app_name'])
                        target_group_id = active_tgt['chat_id'] if active_tgt else CONFIG.get('target_group_id')
                        if target_group_id:
                            # 连接并预热
                            if not user_client.is_connected():
                                await user_client.connect()
                            await user_client.get_dialogs()

                            found = []
                            # 限制每个标签搜索的条数，避免遍历整个历史造成阻塞
                            for lbl in deleted_labels:
                                # 搜索可能包含多种渲染形式，比如 TEST-2, [TEST-2], `TEST-2`
                                search_terms = [lbl, f'[{lbl}]', f'`{lbl}`']
                                seen = set()
                                for term in search_terms:
                                    try:
                                        async for m in user_client.iter_messages(target_group_id, search=term, limit=200):
                                            if m and getattr(m, 'id', None) and m.id not in seen:
                                                found.append((target_group_id, m.id))
                                                seen.add(m.id)
                                    except Exception:
                                        # 如果 Telegram 搜索在某些会话上不被支持或超时，继续尝试下一个 term
                                        continue

                            ids_to_del = found
                    except Exception as search_err:
                        print(f"⚠️ 回滚时在目标群搜索残留消息失败: {search_err}")

                if ids_to_del:
                    await msg.edit(f'📡 正在从 Telegram 物理撤回 {len(ids_to_del)} 条转发消息...\n(Session: {CONFIG.get("app_name")})')
                    
                    # 确保连接并预热实体缓存，防止 ValueError(Could not find entity)
                    if not user_client.is_connected():
                        await user_client.connect()
                    
                    print("🔄 Warming up user_client dialogs for rollback...")
                    await user_client.get_dialogs()

                    from collections import defaultdict
                    grouped = defaultdict(list)
                    for cid, mid in ids_to_del: grouped[cid].append(mid)
                    
                    total_groups = len(grouped)
                    curr_g = 0
                    for tgt_chat_id, m_ids in grouped.items():
                        curr_g += 1
                        try:
                            # 尝试获取实体
                            t_ent = await user_client.get_entity(tgt_chat_id)
                            chunk_size = 100
                            for i in range(0, len(m_ids), chunk_size):
                                chunk = m_ids[i:i + chunk_size]
                                try:
                                    await user_client.delete_messages(t_ent, chunk, revoke=True)
                                    await msg.edit(f'📡 撤销进度: 群组 {curr_g}/{total_groups}\n当前处理: {chunk[0]} ~ {chunk[-1]}')
                                except Exception as chunk_err:
                                    print(f"  ⚠️ Chunk deletion failed ({chunk[0]}-{chunk[-1]}): {chunk_err}")
                                await asyncio.sleep(1.0) # 稍微增加延迟，防止 FloodWait
                        except Exception as de_err:
                            print(f"⚠️ 物理撤销群组 {tgt_chat_id} 失败: {de_err}")
                
                await msg.edit(f'✅ 物理撤销任务已提交完成。')
            except Exception as pre_err:
                await msg.edit(f'❌ 进程中断: {pre_err}\n数据库与本地文件未做任何更改。')
                return
            finally:
                # [CRITICAL] 物理执行完成后，必须再次断开连接，否则子进程或后续操作会发生 Session 锁定冲突
                try:
                    if user_client.is_connected():
                        print("🔌 Releasing User Client session after Phase 1...")
                        await user_client.disconnect()
                except: pass

        else:
            # --- [NEW] Collision Check for Full Sync (Modes 2 and 4) ---
            if mode in ['2', '4'] and not kwargs.get('force_override', False):
                has_collision = False
                targets = []
                if mode == '2' and kwargs.get('ids'):
                    targets = [int(p) for p in kwargs['ids'].split(',') if p.strip().isdigit()]
                elif mode == '4':
                    try:
                        targets = [int(c) for c in os.getenv('SOURCE_CHANNELS', '').split(',') if c.strip().isdigit()]
                    except: pass
                
                # Check DB offsets
                for tid in targets:
                    if db.get_last_offset(tid, is_test=is_test) > 0:
                        has_collision = True
                        break
                
                if has_collision:
                    override_cb = b'force_sync_' + mode.encode()
                    if folder: override_cb += b'_' + folder.encode('utf-8')
                    if kwargs.get('ids'): override_cb += b'_' + kwargs['ids'].encode()
                    # Append is_test flag to callback data if needed, but callback length is limited (64 bytes). 
                    # We store pending kwargs globally or encode it. 
                    # Simpler: just ask to rollback first, or provide a confirmed override action.
                    import uuid
                    req_id = str(uuid.uuid4())[:8]
                    if not hasattr(bot, 'pending_overrides'): bot.pending_overrides = {}
                    bot.pending_overrides[req_id] = {'mode': mode, 'folder': folder, 'kwargs': kwargs, 'event': event}
                    
                    warning_text = (
                        f"⚠️ **[高危操作警告]**\n\n您选择的【全时间轴同步】目标中，**包含已有历史同步记录的频道**。\n\n"
                        f"强制执行将导致：\n"
                        f"1. 私密群组中出现大量**重复转发**的消息。\n"
                        f"2. 本地旧的日志编号作废失效。\n\n"
                        f"💡 **最佳方案**：先按 `⬅️ 返回` 并选择 `5. 高级回滚` 清除该频道的旧数据，再进行全量同步。\n"
                    )
                    warn_btns = [
                        [Button.inline("✅ 确认无视副作用，强制执行", f"override_{req_id}".encode())],
                        [Button.inline("❌ 取消操作 (返回)", b"delete_menu")]
                    ]
                    if isinstance(event, events.CallbackQuery.Event):
                        await event.edit(warning_text, buttons=warn_btns)
                    else:
                        await event.respond(warning_text, buttons=warn_btns)
                    # Return and wait for user to click the override button
                    return
            # -----------------------------------------------------------
                
            folder_suffix = f" (分组: {folder})" if folder else ""
            env_s = " (沙盒测试)" if is_test else ""
            
            if isinstance(event, events.CallbackQuery.Event):
                msg = await event.edit(
                    f'⏳ 正在同步分组: {folder}\n\n任务已在后台启动，请等候完成通知。',
                    buttons=[Button.inline('🛑 停止同步', data='stop_sync_instantly')]
                )
            else:
                msg = await event.respond(
                    f'⏳ 正在同步分组: {folder}\n\n任务已在后台启动，请等候完成通知。',
                    buttons=[Button.inline('🛑 停止同步', data='stop_sync_instantly')]
                )
            
        try:
            # 1. Update Subscriptions metadata early
            await msg.edit('⏳ 准备挂载环境信息...')
            py = sys.executable
            p1 = await asyncio.create_subprocess_shell(f'"{py}" src/sync_mode/update_docs.py --prepare', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await p1.communicate()
            
            # 2. Execute sync logic
            if is_rollback:
                # [Phase 2] 启动子进程进行数据库和本地日志的清理，使用 --no-telegram 规避 Session 锁定
                cmd = f'"{py}" src/sync_mode/sync.py --rollback "{is_rollback}" --no-telegram --confirm'
            else:
                cmd = f'"{py}" src/sync_mode/sync.py --mode {mode} --confirm'
                if folder:
                    cmd += f' --folder "{folder}"'
                if kwargs.get('ids'):
                    cmd += f' --ids "{kwargs["ids"]}"'
                if is_test:
                    cmd += f' --test'
            
            p2 = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout2, stderr2 = await p2.communicate()

            if p2.returncode == 0:
                if is_rollback:
                    await msg.edit(
                        f'✅ 回滚操作已完成！\n指定的历史版本记录及本地关联物理文件均已被清空。\n\n🗑️ 已物理撤回 Target 群组中受影响的转发消息。',
                        buttons=None
                    )
                else:
                    await msg.edit(f'🚀 模式 {mode}: 正在同步更新本地元数据 (关注列表与日志)...', buttons=None)
                    p3 = await asyncio.create_subprocess_shell(f'"{py}" src/sync_mode/update_docs.py', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    await p3.communicate()
                    await msg.edit(f'✅ 模式 {mode}{folder_suffix}{env_s} 同步及元数据更新全部完成！', buttons=None)
            else:
                err_text = stderr2.decode('utf-8', errors='replace')[:500] if stderr2 else stdout2.decode('utf-8', errors='replace')[:500]
                await msg.edit(f'❌ 执行失败 (Exit {p2.returncode}):\n{err_text}', buttons=None)
            
            # [REMOVED] 不再弹出 show_completion_menu
            pass
            
        except Exception as e:
            if isinstance(event, events.CallbackQuery.Event):
                await msg.edit(f'❌ 执行过程中出现异常:\n{e}', buttons=None)
            else:
                await msg.respond(f'❌ 执行过程中出现异常:\n{e}', buttons=None)

@bot.on(events.CallbackQuery(data=re.compile(br'override_(.+)')))
async def force_sync_override_callback(event):
    req_id = event.data_match.group(1).decode('utf-8')
    if not hasattr(bot, 'pending_overrides') or req_id not in bot.pending_overrides:
        await event.answer('⚠️ 该操作已过期，请重新发起同步。', alert=True)
        return
    
    # Release will be handled by the next execute_sync's async with if necessary, 
    # but since this is a new trigger, we ensure the lock is available.
    if sync_job_lock.locked():
        sync_job_lock.release()
        
    req = bot.pending_overrides.pop(req_id)
    kwargs = req['kwargs']
    kwargs['force_override'] = True  # Set flag to bypass the collision check
    
    await event.edit("✅ 您已确认无视风险，正在强制为您重启全量同步任务...", buttons=None)
    await execute_sync(event, req['mode'], folder=req['folder'], **kwargs)

@bot.on(events.CallbackQuery(data=b'nav_main'))
async def nav_main_callback(event):
    await render_main_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=b'nav_sync_main'))
async def nav_sync_main_callback(event):
    await render_main_sync_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=b'nav_search_center'))
async def nav_search_center_callback(event):
    await render_search_center(event, is_edit=True)

@bot.on(events.CallbackQuery(data=b'nav_status_combined'))
async def nav_status_combined_callback(event):
    await event.answer('📊 正在获取运行状态，请稍候...', alert=False)
    await render_sync_status_ui(event)

@bot.on(events.CallbackQuery(data=b'nav_search'))
async def nav_search_callback(event):
    # [OPTIMIZATION] 修改为编辑原菜单，告诉用户如何操作，而不是回复新泡泡
    await event.edit('🔍 **快捷搜索**\n\n请直接发送 `/search <关键词>` 开始搜索。\n或者直接在对话框输入关键词，我将为您进行全局检索。', 
                     buttons=[[Button.inline("🔙 返回主菜单", b"nav_main")]])
    await event.answer()

@bot.on(events.CallbackQuery(data=b'nav_backup'))
async def nav_backup_callback(event):
    await render_backup_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=b'nav_refresh_metadata'))
async def nav_refresh_metadata_callback(event):
    await trigger_metadata_refresh(event, is_manual=True)

@bot.on(events.CallbackQuery(data=b'delete_menu'))
async def delete_menu_callback(event):
    await event.answer()  # 必须先 answer，否则 Telegram 端回调会超时
    try:
        await event.delete()
    except Exception as e:
        print(f"⚠️ delete_menu 失败: {e}")

@bot.on(events.CallbackQuery(data=re.compile(br'sync_(\d)')))
async def pre_run_sync_callback(event):
    mode = event.data_match.group(1).decode('utf-8')
    print(f"🔘 Callback: sync_{mode}")
    
    if mode == '6':
        # 同步状态一览
        await event.answer('正在加载同步状态...', alert=False)
        await render_sync_status_ui(event)
        return

    if mode == '5':
        # 回滚模式
        is_test = user_env.get(event.chat_id, False)
        env_type = "test" if is_test else "formal"
        
        class MockMatch:
            def group(self, i): return env_type.encode('utf-8')
            
        event.data_match = MockMatch()
        await show_rollback_list_callback(event)
        return
        
    is_test = user_env.get(event.chat_id, False)
    print(f"🔘 Mode {mode} proceeding with Test Env: {is_test}")
    
    # 模式 1/2：使用文件夹列表选择界面
    if mode in ['1', '2']:
        await event.answer('正在获取文件夹列表...', alert=False)
        await render_folder_list_ui(event, mode, is_test)
    else:
        env_display = "🧪(测试)" if is_test else "🚀(正式)"
        await event.answer(f'已选择模式 {mode} {env_display}，准备开始...', alert=False)
        await execute_sync(event, mode, is_test=is_test)


@bot.on(events.CallbackQuery(data='stop_sync_instantly'))
async def stop_sync_callback(event):
    STOP_FLAG = 'data/temp/stop_sync.flag'
    with open(STOP_FLAG, 'w') as f:
        f.write('stop')
    await event.answer('🛑 已请求中断，请等待确认...', alert=True)
    try:
        current_text = event.message.text
        await event.edit(f'{current_text}\n\n⚠️ **中断指令已送达，正在收尾...**')
    except:
        pass

@bot.on(events.CallbackQuery(data=re.compile(br'rb_list_(formal|test)')))
async def show_rollback_list_callback(event):
    env_type = event.data_match.group(1).decode('utf-8')
    is_test = (env_type == "test")
    title = "🧪 测试环境" if is_test else "🚀 正式环境"
    
    recent_runs = db.get_recent_sync_runs(is_test=is_test, limit=100)
    
    buttons = []
    # 添加归零点 (Point 0)
    zero_label = "TEST-0" if is_test else "#0"
    zero_callback = f"rb_POINT_0_{'TEST' if is_test else 'FORMAL'}".encode('utf-8')
    buttons.append([Button.inline(f"💥 彻底回滚归零 ({zero_label})", zero_callback)])
    
    for label, time_str in recent_runs:
        callback_data = f"rb_{label}".encode('utf-8')
        buttons.append([Button.inline(f"⏪ 保留至 {label} ({time_str})", callback_data)])
        
    buttons.append([Button.inline("⬅️ 返回主菜单", b"sync_back")])
    buttons.append([Button.inline(f"🔄 切换到 {'正式' if is_test else '测试'}历史", b"rb_list_formal" if is_test else "rb_list_test"), Button.inline("🗑️ 关闭", b"delete_menu")])
    
    # 如果按钮太多，Telegram 可能有限制。这里如果超过 100 个可能需要分页，但先满足用户“所有”的需求。
    # 实际上 Telegram InlineKeyboardMarkup 对行数有限制（通常是 100 行左右）。
    await event.edit(
        f"⚠️ **{title} 回滚历史**\n\n"
        "请选择您想要**保留**到的目标版本号。\n"
        "注意：点击「回归零」将抹除该环境下的**所有**同步记录。",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(data=b'sync_back'))
async def back_to_sync_menu(event):
    await render_main_sync_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=re.compile(br'rb_(?P<label>(?!list_).+)')))
async def run_rollback_callback(event):
    target_label = event.data_match.group('label').decode('utf-8')
    # 处理 POINT_0 的友好显示
    display_label = target_label
    if target_label == "POINT_0_TEST": display_label = "TEST-0 (彻归零)"
    if target_label == "POINT_0_FORMAL": display_label = "#0 (彻归零)"
    
    print(f"🔘 Rollback choice: {target_label}")
    await event.answer(f'确认回滚至 {display_label}...', alert=False)
    
    # 所有的数据库擦除与群消息物理撤回均委托 sync.py 脚本执行
    await execute_sync(event, "rollback", rollback_target=target_label)

async def get_folder_peers(folder_name):
    """Helper to get and categorize peers in a folder"""
    try:
        # [FIX] 强制保障连接有效性
        # Telethon 有时 is_connected() 为 True 但底层 socket 已断开，会导致 0 bytes read 错误
        if not user_client.is_connected():
            print("🔄 [Auto-Reconnect] user_client is disconnected, reconnecting...")
            await user_client.connect()
            
        try:
            filters_resp = await asyncio.wait_for(user_client(functions.messages.GetDialogFiltersRequest()), timeout=10.0)
        except (ConnectionError, BrokenPipeError, ConnectionResetError, EOFError, OSError) as e:
            print(f"🔄 [Auto-Reconnect] user_client connection dropped ({e}), forcing reconnect...")
            await user_client.disconnect()
            await user_client.connect()
            filters_resp = await asyncio.wait_for(user_client(functions.messages.GetDialogFiltersRequest()), timeout=10.0)
            
        all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
        
        target_filter = None
        for f in all_filters:
            title = getattr(f, 'title', None)
            t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            if t_str == folder_name:
                target_filter = f
                break
        
        if not target_filter:
            return None, []

        peers_info = []
        seen_ids = set()
        
        # 合并 include 和 pinned
        all_target_peers = list(getattr(target_filter, 'include_peers', [])) + list(getattr(target_filter, 'pinned_peers', []))
        
        for peer in all_target_peers:
            try:
                e = await user_client.get_entity(peer)
                if e.id in seen_ids:
                    continue
                seen_ids.add(e.id)
                
                title = getattr(e, 'title', None) or getattr(e, 'first_name', '私人群聊')
                from telethon.tl.types import Channel, Chat, User
                is_syncable = False
                icon = "❓"
                if isinstance(e, Channel):
                    icon = "📢" if getattr(e, 'broadcast', False) else "👥"
                    is_syncable = True
                elif isinstance(e, Chat):
                    icon = "👥"
                    is_syncable = True
                elif isinstance(e, User):
                    if getattr(e, 'is_self', False):
                        title = "收藏夹 (Saved Messages)"
                        icon = "💾"
                        is_syncable = True
                    elif not getattr(e, 'bot', False):
                        icon = "💬"
                        is_syncable = False  # 不允许同步普通用户的私聊
                    else:
                        icon = "🤖"
                
                # Exclusions
                if is_syncable:
                    if '私密视频库' in title:
                        is_syncable = False
                        icon = "🔒"
                    elif isinstance(e, (Channel, Chat)) and getattr(e, 'archived', False):
                        is_syncable = False
                        icon = "🚫"
                
                peers_info.append({
                    'id': str(e.id),
                    'title': title,
                    'icon': icon,
                    'is_syncable': is_syncable
                })
            except Exception as ex:
                print(f"⚠️ Failed to get entity for peer {peer}: {ex}")
                continue
        return folder_name, peers_info
    except Exception as e:
        print(f"❌ get_folder_peers Error: {e}")
        return folder_name, []

async def get_all_folder_peers():
    """获取所有文件夹及其频道 (去重), 返回 [(folder_name, [peers_info])]"""
    try:
        if not user_client.is_connected():
            await user_client.connect()
            
        # [OPTIMIZATION] 获取全量对话并建立实体映射，避免循环内部调用 get_entity
        active_dialogs = await user_client.get_dialogs()
        entity_map = {d.id: d.entity for d in active_dialogs}
        active_ids = set(entity_map.keys())

        filters_resp = await asyncio.wait_for(user_client(functions.messages.GetDialogFiltersRequest()), timeout=10.0)
        all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
        result = []
        for f in all_filters:
            title = getattr(f, 'title', None)
            t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            if not t_str or not hasattr(f, 'include_peers'):
                continue
            peers_info = []
            
            # [V2 Fix] 使用文件夹内去重，而非全局去重，允许频道出现在多个文件夹中
            folder_seen = set()
            # 合并包含的和置顶的 Peers
            raw_peers = list(getattr(f, 'include_peers', [])) + list(getattr(f, 'pinned_peers', []))
            
            for peer in raw_peers:
                try:
                    from telethon import utils
                    pid = utils.get_peer_id(peer)
                    if pid in folder_seen:
                        continue
                    folder_seen.add(pid)

                    # 如果 ID 根本不在 active_ids 里，说明已退出或已删，直接跳过
                    if pid not in active_ids:
                        continue 

                    # [OPTIMIZATION] 从映射中直接获取实体，不再 await 网络请求
                    e = entity_map.get(pid)
                    if not e:
                        continue
                        
                    tname = getattr(e, 'title', None) or getattr(e, 'first_name', str(getattr(e, 'id', 'Unknown')))
                    from telethon.tl.types import Channel, Chat, User
                    is_syncable, icon = False, "❓"
                    if isinstance(e, Channel):
                        icon = "📢" if getattr(e, 'broadcast', False) else "👥"
                        is_syncable = True
                    elif isinstance(e, Chat):
                        icon, is_syncable = "👥", True
                    elif isinstance(e, User):
                        if getattr(e, 'is_self', False):
                            tname, icon, is_syncable = "收藏夹", "💾", True
                        elif not getattr(e, 'bot', False):
                            icon = "💬"
                        else:
                            icon = "🤖"
                    if is_syncable and '私密视频库' in tname:
                        is_syncable, icon = False, "🔒"
                    
                    restriction_reasons = getattr(e, 'restriction_reason', []) or []
                    is_globally_banned = any(
                        getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'
                        for r in restriction_reasons
                    )
                    is_partial = bool(restriction_reasons) and not is_globally_banned
                    peers_info.append({
                        'id': str(utils.get_peer_id(e)),
                        'title': tname, 'icon': icon, 'is_syncable': is_syncable,
                        'is_globally_banned': is_globally_banned, 'is_partial': is_partial,
                    })
                except Exception:
                    continue
            if peers_info:
                result.append((t_str, peers_info))
        return result
    except Exception as e:
        print(f"❌ get_all_folder_peers Error: {e}")
        return []

async def render_tree_ui(event, action_type, is_test):
    """渲染跨文件夹树状图选择界面。action_type: 'sync_1'/'sync_2'/'backup_1'/'backup_2'"""
    chat_id = event.chat_id
    istest_val = "1" if is_test else "0"
    all_folders = await get_all_folder_peers()
    if not all_folders:
        await event.edit("❌ 未找到任何 Telegram 文件夹。")
        return

    sel_key = f"tree_{action_type}"
    exp_key = f"tree_exp_{action_type}" # 展开状态存储
    if chat_id not in user_selections:
        user_selections[chat_id] = {}
    
    # 初始化选择
    if sel_key not in user_selections[chat_id]:
        user_selections[chat_id][sel_key] = {p['id'] for _, peers in all_folders for p in peers if p['is_syncable']}
    # 初始化展开状态 (默认折叠)
    if exp_key not in user_selections[chat_id]:
        user_selections[chat_id][exp_key] = set()
        
    selections = user_selections[chat_id][sel_key]
    expanded_folders = user_selections[chat_id][exp_key]

    is_sync = action_type.startswith('sync')
    latest = db.get_latest_sync_info(is_test=is_test) if is_sync else db.get_latest_backup_info(is_test=is_test)
    env_d = "🧪测试" if is_test else "🚀正式"
    act_name = "同步" if is_sync else "备份"
    mode_n = action_type.split('_')[1]

    lines = [f"📊 **{act_name}模式 {mode_n} ({env_d})**"]
    if latest:
        t = latest['time'][:16].replace('T', ' ') if latest.get('time') else "N/A"
        lines.append(f"📌 最近正式{act_name}: {latest['label']} ({t})")
    else:
        lines.append(f"📌 暂无正式{act_name}记录")
    lines.append("")

    buttons = [[
        Button.inline("📦 全选", f"tree_selall_{action_type}_{istest_val}".encode()),
        Button.inline("🗑️ 全不选", f"tree_selnone_{action_type}_{istest_val}".encode())
    ]]
    
    selected_count = 0
    for idx, (folder_name, peers) in enumerate(all_folders):
        is_expanded = str(idx) in expanded_folders
        toggle_icon = "🔽" if is_expanded else "▶️"
        
        # 统计文件夹内选中数
        folder_sel_count = sum(1 for p in peers if p['id'] in selections)
        selected_count += folder_sel_count
        
        # 汉化：展开/收起状态标识
        toggle_icon = "🔽" if is_expanded else "▶️"
        folder_label = f"{toggle_icon} {folder_name} ({folder_sel_count}/{len(peers)})"
        
        # 汉化：确保回调匹配正则 tree_fld_(.+)_(0|1)_(\d+)
        buttons.append([Button.inline(folder_label, f"tree_fld_{action_type}_{istest_val}_{idx}".encode())])
        
        if is_expanded:
            for p in peers:
                if not p['is_syncable']:
                    continue
                is_sel = p['id'] in selections
                mark = "✅" if is_sel else "⬜"
                # 频道状态
                try:
                    if is_sync:
                        ci = db.get_latest_sync_info(int(p['id']), is_test=is_test)
                    else:
                        ci = db.get_latest_backup_info(int(p['id']))
                    st = f" — {ci['label']} {ci['time'][:10]}" if ci and ci.get('time') else ""
                except:
                    st = ""
                lines.append(f"  {mark} {p['icon']} {p['title']}{st}")
                buttons.append([Button.inline(f"{mark} {p['icon']} {p['title']}", f"tree_tgl_{action_type}_{istest_val}_{p['id']}".encode())])

    ab = []
    if selected_count > 0:
        ab.append(Button.inline(f"🚀 开始{act_name} ({selected_count}个)", f"tree_run_{action_type}_{istest_val}".encode()))
    else:
        ab.append(Button.inline("⚠️ 请先勾选目标", b"sync_none"))
    ab.append(Button.inline("⬅️ 返回", b"sync_back"))
    buttons.append(ab)
    buttons.append([Button.inline("🗑️ 清除菜单", b"delete_menu")])
    await event.edit("\n".join(lines), buttons=buttons)

async def render_folder_ui(event, mode, folder_name, peers_info, is_test):
    """Generates the message text and buttons based on current selections"""
    chat_id = event.chat_id
    selections = user_selections.get(chat_id, {}).get(folder_name, set())
    istest_val = "1" if is_test else "0"
    
    buttons = []
    # 顶部快捷操作
    buttons.append([
        Button.inline("📦 全选", f"selall_{mode}_{istest_val}_{folder_name}".encode('utf-8')),
        Button.inline("🗑️ 全不选", f"selnone_{mode}_{istest_val}_{folder_name}".encode('utf-8'))
    ])
    
    env_display = "🧪测试环境" if is_test else "🚀正式环境"
    info_text = f"📂 **分组: {folder_name} ({env_display})**\n点击项目可『✅ 勾选 / ⬜ 取消』：\n"
    valid_count = 0
    selected_count = 0
    
    for p in peers_info:
        if not p['is_syncable']:
            info_text += f"  🔒 {p['icon']} {p['title']} (不可同步)\n"
            buttons.append([Button.inline(f"🔒 {p['icon']} {p['title']}", b"noop")])
        else:
            valid_count += 1
            is_selected = p['id'] in selections
            if is_selected: selected_count += 1
            mark = "✅" if is_selected else "⬜"
            info_text += f"  {mark} {p['icon']} {p['title']}\n"
            cb = f"tgl_{mode}_{istest_val}_{folder_name}_{p['id']}".encode('utf-8')
            buttons.append([Button.inline(f"{mark} {p['icon']} {p['title']}", cb)])

    action_buttons = []
    act_name = "备份" if mode.startswith('bk') else "同步"
    if selected_count > 0:
        action_buttons.append(Button.inline(f"🚀 开始{act_name} ({selected_count}个目标)", f"fld_{mode}_{istest_val}_{folder_name}".encode('utf-8')))
    else:
        action_buttons.append(Button.inline("⚠️ 请先勾选目标", b"sync_none"))
        
    action_buttons.append(Button.inline("⬅️ 返回", b"nav_back_list" if mode.startswith('bk') else "sync_back"))
    buttons.append(action_buttons)
    buttons.append([Button.inline("🗑️ 清除菜单", b"delete_menu")])
    
    try:
        await event.edit(info_text, buttons=buttons)
    except Exception as e:
        if "not modified" not in str(e).lower():
            print(f"⚠️ render_folder_ui error: {e}")

async def render_folder_list_ui(event, mode, is_test):
    """显示文件夹列表主界面"""
    istest_val = "1" if is_test else "0"
    env_d = "🧪测试" if is_test else "🚀正式"
    
    if mode.startswith('bk'):
        mode_name = "局部备份"
        back_btn = b"nav_backup"
    else:
        mode_name = "局部更新" if mode == '1' else "局部全时间轴"
        back_btn = b"sync_back"
    
    try:
        if not user_client.is_connected():
            await user_client.connect()
        filters_resp = await asyncio.wait_for(user_client(functions.messages.GetDialogFiltersRequest()), timeout=10.0)
        all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
        
        folders = []
        # Get MANAGED_FOLDERS from CONFIG
        managed_folders = CONFIG.get('managed_folders', [])
        
        for f in all_filters:
            title = getattr(f, 'title', None)
            t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            if t_str and hasattr(f, 'include_peers'):
                if not managed_folders or "*" in managed_folders or "ALL" in [m.upper() for m in managed_folders] or t_str in managed_folders:
                    folders.append(t_str)
    except Exception as e:
        print(f"❌ render_folder_list_ui User Client Error: {e}")
    
    buttons = []
    for folder_name in folders:
        cb = f"view_{mode}_{istest_val}_{folder_name}".encode('utf-8')
        buttons.append([Button.inline(f"📁 {folder_name}", cb)])
    
    buttons.append([Button.inline("⬅️ 返回主菜单", back_btn), Button.inline("🗑️ 关闭", b"delete_menu")])
    try:
        await event.edit(
            f"📂 **模式{mode} {mode_name} [{env_d}]**\n请选择要同步的文件夹：",
            buttons=buttons
        )
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_folder_list_ui edit error: {e}")

async def render_sync_status_ui(event):
    """渲染同步情况一览页面 (包含分组过滤与空运行显示)"""
    chat_id = event.chat_id
    is_test = user_env.get(chat_id, False)
    env_badge = "🧪测试" if is_test else "🚀正式"
    
    # [NEW] 1. 页眉增加最近一次活动记录（解决空运行不显示的问题）
    latest_any = db.get_latest_sync_info(is_test=is_test)
    header_info = ""
    if latest_any:
        la_time = latest_any['time'][:16].replace('T', ' ')
        header_info = f"📌 最近活动: {latest_any['label']} ({la_time})\n"
    else:
        header_info = "📌 暂无同步记录\n"

    lines = [f"📊 **同步状态一览 [{env_badge}]**", header_info]
    
    try:
        all_folders = await get_all_folder_peers()
        managed_folders = CONFIG.get('managed_folders', []) # [FIX] 引入管辖区过滤
        
        if not all_folders:
            lines.append("❌ 未找到任何文件夹")
        else:
            for folder_name, peers in all_folders:
                # [FIX] 仅显示本 Bot 管辖范围内的文件夹
                if managed_folders and folder_name not in managed_folders:
                    continue
                    
                syncable = [p for p in peers if p['is_syncable']]
                if not syncable:
                    continue
                lines.append(f"\n📁 **{folder_name}**")
                for p in syncable:
                    ban_badge = " [🚫 已封禁]" if p.get("is_globally_banned") else ""
                    lines.append(f"  {p['icon']} {p['title']}{ban_badge}")
                    
                    try:
                        # [V2 Fix] 仅展示数据库精准记录，不再进行模糊文件扫描
                        latest = db.get_latest_sync_info(chat_id=int(p['id']), is_test=is_test)
                        if latest:
                            t = latest['time'][:16].replace('T', ' ')
                            lines.append(f"    └─ {latest['label']} · {t}")
                        else:
                            lines.append("    └─ 暂无同步记录")
                    except Exception as e_ui:
                        lines.append(f"    └─ ⚠️ 数据查询失败: {e_ui}")
    except Exception as e:
        lines.append(f"❌ 加载失败: {e}")
    
    buttons = [
        [Button.inline("⬅️ 返回主菜单", b"sync_back"), Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    
    # [FIX] 捕获并忽略 MessageNotModifiedError
    try:
        await event.edit("\n".join(lines), buttons=buttons)
    except Exception as e:
        if "not modified" not in str(e).lower():
            print(f"⚠️ render_sync_status_ui edit error: {e}")

async def render_backup_status_ui(event):
    """渲染备份情况一览页面"""
    lines = [f"📊 **备份情况一览 (树状图预览)**\n"]
    try:
        all_backup_runs = db.get_manageable_backup_runs(limit=100, bot_name=BOT_NAME)

        def resolve_snapshot_exists(ch, fallback_folder_name, fallback_title):
            json_path = ch.get('json_file')
            md_path = ch.get('md_file')
            if (json_path and os.path.exists(json_path)) or (md_path and os.path.exists(md_path)):
                return True

            cid = ch.get('id') or ch.get('chat_id')
            if cid is None:
                return False

            safe_title = re.sub(r'[<>:"/\\|?*]', '_', str(fallback_title)).strip() if fallback_title else ''
            safe_folder = re.sub(r'[<>:"/\\|?*]', '_', str(fallback_folder_name)).strip() if fallback_folder_name else ''
            channel_dir = f"{safe_title}_{abs(int(cid))}" if safe_title else None
            if not channel_dir:
                return False

            for root in ['data/archived/backups', 'docs/archived/backups']:
                folder_root = os.path.join(root, safe_folder, channel_dir)
                if os.path.isdir(folder_root):
                    for fname in os.listdir(folder_root):
                        if fname.endswith('.json') or fname.endswith('.md'):
                            return True
            return False

        def get_latest_local_backup_status(pid_str, folder_name, title):
            for run in all_backup_runs:
                for ch in run.get('channels', []) or []:
                    cid = ch.get('id') or ch.get('chat_id')
                    if str(cid) != pid_str:
                        continue
                    if resolve_snapshot_exists(ch, folder_name, title):
                        t = str(run.get('time', ''))[:16].replace('T', ' ')
                        return f"  {run.get('label', '#B?')} · {t}" if t else f"  {run.get('label', '#B?')}"
            return "  暂无本机器人备份记录"

        all_folders_raw = await get_all_folder_peers()
        managed_list = CONFIG.get('managed_folders', [])
        
        # [ISOLATION] 仅处理本 Bot 管辖范围内的文件夹
        if managed_list and "*" not in managed_list and "ALL" not in [m.upper() for m in managed_list]:
            all_folders = [f for f in all_folders_raw if f[0] in managed_list]
        else:
            all_folders = all_folders_raw
            
        active_ids = set()
        
        # [优化] 提前建立本地元数据 ID 缓存，避免 O(N*M) 的文件扫描
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
                        except: pass

        # 记录所有活跃（在文件夹中）的 ID
        for _, peers in all_folders:
            for p in peers:
                active_ids.add(str(p['id']))

        if not all_folders:
            lines.append("❌ 未找到任何活跃文件夹")
        else:
            for folder_name, peers in all_folders:
                syncable = [p for p in peers if p['is_syncable']]
                if not syncable: continue
                lines.append(f"\n📁 **{folder_name}**")
                for p in syncable:
                    pid_str = str(p['id'])
                    has_local_file = pid_str in metadata_id_map
                    
                    st = "  暂无本机器人备份记录"
                    if has_local_file:
                        try:
                            st = get_latest_local_backup_status(pid_str, folder_name, p['title'])
                        except: st = "  数据查询失败"
                    
                    ban_badge = " [🚫 已封禁]" if p.get("is_globally_banned") else ""
                    lines.append(f"  {p['icon']} {p['title']}{ban_badge}{st}")
        
        # [NEW] 历史频道虚拟分组：使用预缓存的 deleted_channels_map（O(1) 查找）
        # is_deleted 由 update_docs.py (refresh) 在检测到频道不可访问时自动写入
        historical_lines = []
        for mid_str, ch_info in deleted_channels_map.items():
            try:
                # [ISOLATION] 历史频道同样仅查阅本 Bot 记录
                # 策略：如果本 Bot 没备过，即便文件还在，也不在 status 页面显示
                bk_info = db.get_latest_backup_info(int(mid_str), bot_name=BOT_NAME)
                if bk_info and bk_info.get('time'):
                    t = bk_info['time'][:16].replace('T', ' ')
                    da = ch_info.get('deleted_at', '')
                    da_str = f"  *(断连 {da})*" if da else ""
                    historical_lines.append(f"  💤 {ch_info['name']}  {bk_info['label']} · {t}{da_str}")
            except:
                pass

        if historical_lines:
            lines.append("\n\n🗄️ **历史频道 (本地保留)**")
            lines.extend(historical_lines)

            
    except Exception as e:
        lines.append(f"❌ 加载失败: {e}")
    
    buttons = [
        [Button.inline("⬅️ 返回菜单", b"nav_backup"), Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    await safe_edit(event, "\n".join(lines), buttons=buttons)


@bot.on(events.CallbackQuery(data=b'sync_toggle_env_main'))
async def sync_toggle_env_main_callback(event):
    """在主菜单切换沙盒/正式环境"""
    curr = user_env.get(event.chat_id, False)
    user_env[event.chat_id] = not curr
    label = "🧪测试模式" if user_env[event.chat_id] else "🚀正式模式"
    await event.answer(f'已切换为{label}', alert=False)
    await render_main_sync_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=re.compile(br'view_(bk_\d(?:_full|_inc)?|\d)_([01])_(.+)')))
async def view_folder_details(event):
    mode = event.data_match.group(1).decode('utf-8')
    is_test = True if event.data_match.group(2).decode('utf-8') == '1' else False
    folder_name = event.data_match.group(3).decode('utf-8')
    
    print(f"📂 Callback: view_{mode}_{is_test}_{folder_name}")
    await event.answer(f'正在分析分组: {folder_name}', alert=False)
    
    fname, peers = await get_folder_peers(folder_name)
    if not fname:
        await event.edit(f'❌ 未找到分组: {folder_name}')
        return
        
    chat_id = event.chat_id
    if chat_id not in user_selections: user_selections[chat_id] = {}
    # 默认不勾选任何频道
    if folder_name not in user_selections[chat_id]:
        user_selections[chat_id][folder_name] = set()
        
    await render_folder_ui(event, mode, folder_name, peers, is_test)

@bot.on(events.CallbackQuery(data=b'nav_back_list'))
async def nav_back_list_callback(event):
    # 根据当前显示的文本判断模式，或者从上下文推断。这里简单处理，备份返回模式1列表
    is_test = False # 备份默认为正式
    await render_folder_list_ui(event, "bk_1", is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'tgl_(bk_\d(?:_full|_inc)?|\d)_([01])_(.+)_(\d+)')))
async def toggle_peer_callback(event):
    mode = event.data_match.group(1).decode('utf-8')
    is_test = True if event.data_match.group(2).decode('utf-8') == '1' else False
    folder_name = event.data_match.group(3).decode('utf-8')
    peer_id = event.data_match.group(4).decode('utf-8')
    
    chat_id = event.chat_id
    if chat_id in user_selections and folder_name in user_selections[chat_id]:
        s = user_selections[chat_id][folder_name]
        if peer_id in s: s.remove(peer_id)
        else: s.add(peer_id)
        
    fname, peers = await get_folder_peers(folder_name)
    await render_folder_ui(event, mode, folder_name, peers, is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'sel(all|none)_(bk_\d(?:_full|_inc)?|\d)_([01])_(.+)')))
async def select_bulk_callback(event):
    action = event.data_match.group(1).decode('utf-8')
    mode = event.data_match.group(2).decode('utf-8')
    is_test = (event.data_match.group(3).decode('utf-8') == "1")
    folder_name = event.data_match.group(4).decode('utf-8')
    
    fname, peers = await get_folder_peers(folder_name)
    chat_id = event.chat_id
    if chat_id not in user_selections: user_selections[chat_id] = {}
    
    if action == 'all':
        user_selections[chat_id][folder_name] = {p['id'] for p in peers if p['is_syncable']}
    else:
        user_selections[chat_id][folder_name] = set()
        
    await render_folder_ui(event, mode, folder_name, peers, is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'fld_(bk_\d(?:_full|_inc)?|\d)_([01])_(.+)')))
async def run_folder_sync_callback(event):
    mode = event.data_match.group(1).decode('utf-8')
    is_test = True if event.data_match.group(2).decode('utf-8') == '1' else False
    folder_name = event.data_match.group(3).decode('utf-8')
    
    chat_id = event.chat_id
    selections = user_selections.get(chat_id, {}).get(folder_name, set())
    
    is_bk = mode.startswith('bk')
    act_name = "备份" if is_bk else "同步"
    
    if not selections:
        await event.answer(f'⚠️ 请先勾选{act_name}目标', alert=True)
        return
        
    ids_str = ",".join(list(selections))
    await event.answer(f'正在启动分组 {folder_name} 的{act_name}...', alert=False)
    # 自动清理选择菜单按钮，防止重复点击
    try:
        await event.edit(f'⏳ **正在{act_name}分组: {folder_name}**\n\n任务已在后台启动，请等候完成通知。', buttons=None)
    except:
        pass
    # 调用执行函数
    if is_bk:
        await execute_backup(event, mode, folder=folder_name, ids=ids_str, is_test=is_test)
    else:
        await execute_sync(event, mode, folder=folder_name, ids=ids_str, is_test=is_test)

@bot.on(events.CallbackQuery(data=b'nav_backup'))
async def nav_backup_callback(event):
    await render_backup_menu(event, is_edit=True)

@bot.on(events.CallbackQuery(data=b'noop'))
async def noop_callback(event):
    await event.answer('🔒 该项目目前不可同步 (可能是 Bot 或已归档项目)。', alert=True)

@bot.on(events.CallbackQuery(data=b'sync_none'))
async def sync_none_callback(event):
    await event.answer('⚠️ 请至少勾选一个同步目标。', alert=True)

@bot.on(events.NewMessage(pattern=r'/backup(?:\s+(.+))?'))
async def run_backup_cmd(event):
    args = event.pattern_match.group(1)
    if args:
        # ... (快捷备份逻辑保持不变)
        target_channel = args.strip()
        # ...
        await event.respond(f'💾 开始快捷备份频道: `{target_channel}`...')
        # (此处省略部分快捷备份内部逻辑，仅为示意)
        return

    await render_backup_menu(event, is_edit=False)

async def render_backup_menu(event, is_edit=False):
    """渲染备份主菜单"""
    buttons = [
        [Button.inline("1. 局部备份", b"bk_1_full")],
        [Button.inline("2. 局部增量更新备份", b"bk_1_inc")],
        [Button.inline("3. 全局备份", b"bk_2_full")],
        [Button.inline("4. 全局增量更新备份", b"bk_2_inc")],
        [Button.inline("📊 5. 备份情况一览", b"bk_status")],
        [Button.inline("⚙️ 6. 管理备份", b"bk_manage")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")],
        [Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    title = (
        "💾 **请选择备份模式：**\n\n"
        "> [!NOTE]\n"
        "> **增量更新**：自动识别断点并追加，本地数据依然持久保留。\n"
        "> **全量备份**：强制全量抓取，会覆盖原有 JSON/MD 元数据。"
    )
    try:
        if is_edit:
            await event.edit(title, buttons=buttons)
        else:
            await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_backup_menu error: {e}")

@bot.on(events.CallbackQuery(data=b'stop_backup'))
async def stop_backup_callback(event):
    STOP_FLAG = f'data/temp/stop_backup_{BOT_NAME}.flag'
    try:
        with open(STOP_FLAG, 'w') as f:
            f.write('stop')
        await event.answer('🛑 正在发送停止信号，请稍候...', alert=True)
        # [FIX] 立即反馈 UI，移除停止按钮
        try:
            # 尝试给当前文本加上停止状态并移除按钮
            current_text = event.message.message if hasattr(event, 'message') and event.message else ""
            if "⏳ **备份进行中...**" in current_text:
                await safe_edit(event, current_text + "\n\n🛑 **正在请求停止... 请等候后台保存...**", buttons=None)
        except: pass
    except Exception as e:
        await event.answer(f'❌ 停止失败: {e}', alert=True)

@bot.on(events.CallbackQuery(data=re.compile(br'bk_(.+)')))
async def backup_menu_callback(event):
    data = event.data_match.group(1).decode('utf-8')

    if data == 'status':
        await event.answer('正在加载备份概览...', alert=False)
        await render_backup_status_ui(event)
        return

    if data == 'manage':
        await render_backup_manage_menu(event)
        return

    # 解析模式与增量开关
    # bk_1_inc, bk_2_inc, bk_1_full, bk_2_full
    parts = data.split('_')
    mode = parts[0] # '1' or '2'
    is_inc = (parts[1] == 'inc') if len(parts) > 1 else False

    if mode == '1':
        # 局部备份：先选文件夹
        mode_text = "增量更新" if is_inc else "全量覆盖"
        await event.answer(f'局部备份 - {mode_text}...', alert=False)
        # 我们把 is_inc 注入到 callback data 中透传给后续
        cb_prefix = f"bk_1_{'inc' if is_inc else 'full'}"
        await render_folder_list_ui(event, cb_prefix, is_test=False)
    else:
        # 全局备份 (mode == '2')
        mode_text = "增量更新" if is_inc else "全量覆盖"
        await event.answer(f'准备启动全局备份 ({mode_text})...', alert=False)
        await execute_backup(event, '2', is_test=False, incremental=is_inc)

async def render_backup_manage_menu(event):
    """渲染备份管理子菜单"""
    buttons = [
        [Button.inline("🗑️ 删除指定记录 (最近20条)", b"bkm_list")],
        [Button.inline("📦 瘦身备份 (仅保留各群最新)", b"bkm_prune")],
        [Button.inline("🔥 清空所有备份记录", b"bkm_clear")],
        [Button.inline("⬅️ 返回备份菜单", b"nav_backup")]
    ]
    title = '⚙️ **备份记录管理**\n\n> [!CAUTION]\n> 删除操作将同步移除数据库条目以及磁盘上的 JSON/MD 文件。'
    await safe_edit(event, title, buttons=buttons)

async def render_backup_manage_list(event):
    """提取出的渲染函数，支持多选状态"""
    chat_id = event.chat_id
    if chat_id not in user_states: user_states[chat_id] = {}
    selected = user_states[chat_id].get('selected_backups', set())
    
    runs = db.get_manageable_backup_runs(limit=20, bot_name=BOT_NAME)
    if not runs:
        await safe_edit(event, "📭 暂无备份记录可管理", buttons=[Button.inline("⬅️ 返回", b"bk_manage")])
        return
    
    lines = ["🗑️ **管理备份记录 (多选模式)**\n"]
    
    buttons = []
    for r in runs:
        is_sel = r['run_id'] in selected
        icon = "✅ " if is_sel else ""
        
        # 模式解析
        m_name = "局部" if r['mode'] == '1' else ("全局" if r['mode'] == '2' else "旧版")
        t_name = "增量" if r['incremental'] else "全量"
        if r.get('is_first_formal_baseline'):
            t_name = "全量"
        type_str = f"{m_name}{t_name}"
        
        time_str = r['time'][5:16].replace('T', ' ')
        
        # 频道解析
        ch_names = []
        if r['channels']:
            ch_names = [ch['name'] for ch in r['channels'] if ch.get('name')]
        
        ch_count = len(ch_names)
        ch_summary = ch_names[0] if ch_count == 1 else (f"{ch_names[0]}等{ch_count}群" if ch_count > 1 else "0群")
        
        # 消息数统计
        new_c = r.get('new_messages', 0)
        total_c = r.get('total_messages', 0)
        
        count_str = f"+{new_c}" if r['incremental'] else f"{total_c}"
        if r['incremental'] and total_c > 0:
            count_str = f"+{new_c}/{total_c}"
        elif r.get('is_first_formal_baseline') and total_c > 0:
            count_str = f"{total_c}"
            
        # 按钮文本
        btn_text = f"{icon}{r['label']} | {type_str} | {count_str}条 | {time_str} ({ch_summary})"
        buttons.append([Button.inline(btn_text, f"bkdel_toggle_{r['run_id']}".encode())])
        
        # 如果选中，在正文显示更多详情
        if is_sel:
            lines.append(f"🔹 **{r['label']}** ({m_name}{t_name})")
            if r.get('is_first_formal_baseline'):
                lines.append(f"  ├ 统计: `基线全量归档 {total_c} 条` (已合并相册)")
            else:
                lines.append(f"  ├ 统计: `本次新增 {new_c} 条 / 归档总计 {total_c} 条` (已合并相册)")
            lines.append(f"  └ 频道明细:")
            
            if r['channels']:
                for ch in r['channels']:
                    ch_n = ch.get('name', '未知频道')
                    ch_new = ch.get('new_count', 0)
                    ch_tot = ch.get('count', 0)
                    if r.get('is_first_formal_baseline'):
                        lines.append(f"    • {ch_n}: `{ch_tot}`")
                    else:
                        lines.append(f"    • {ch_n}: `+{ch_new}/{ch_tot}`")
            else:
                lines.append(f"    • (无详细频道信息)")

    if not selected:
        lines.append("点击下方记录启用多选。勾选后可查看具体的频道增量/全量明细。")

    # 操作按钮排
    op_row = []
    if selected:
        op_row.append(Button.inline(f"🗑️ 批量删除 ({len(selected)})", b"bkm_bulk_del"))
        op_row.append(Button.inline("🧹 取消选定", b"bkm_clear_sel"))
    
    buttons.append(op_row) if op_row else None
    buttons.append([Button.inline("⬅️ 返回管理菜单", b"bk_manage")])
    
    title = "\n".join(lines)
    try:
        await event.edit(title, buttons=buttons)
    except Exception as e:
        if "not modified" not in str(e).lower():
            print(f"⚠️ render_backup_manage_list error: {e}")

@bot.on(events.CallbackQuery(data=re.compile(br'bkm_(.+)')))
async def backup_manage_callback(event):
    cmd = event.data_match.group(1).decode('utf-8')
    chat_id = event.chat_id
    
    if cmd == 'list':
        await render_backup_manage_list(event)

    elif cmd == 'clear_sel':
        if chat_id in user_states:
            user_states[chat_id]['selected_backups'] = set()
        await render_backup_manage_list(event)

    elif cmd == 'bulk_del':
        selected = user_states[chat_id].get('selected_backups', set())
        if not selected:
            await event.answer("⚠️ 未选择任何记录", alert=True)
            return
        
        await event.answer(f"正在批量删除 {len(selected)} 条记录...", alert=False)
        
        # 逐个执行删除
        all_runs = db.get_manageable_backup_runs(limit=100, bot_name=BOT_NAME)
        total_files = 0
        affected_chat_ids = set()
        for rid in list(selected):
            target = next((r for r in all_runs if r['run_id'] == rid), None)
            if target:
                for ch in target.get('channels', []):
                    ch_id = ch.get('id') or ch.get('chat_id')
                    if ch_id is not None:
                        affected_chat_ids.add(ch_id)
                db.delete_backup_run(rid)
                count = await perform_backup_physical_cleanup(run_time=target['time'], channels=target['channels'], label=target['label'])
                total_files += count

        if affected_chat_ids:
            db.recalc_backup_offsets(bot_name=BOT_NAME, affected_chat_ids=affected_chat_ids, clear_missing=True)
        
        user_states[chat_id]['selected_backups'] = set() # 清空已选
        await event.respond(f"✅ **批量删除完成！**\n已移除 `{len(selected)}` 条记录及其关联的 `{total_files}` 个文件。")
        await render_backup_manage_list(event)

    elif cmd == 'prune':
        await event.answer("正在执行备份瘦身...", alert=False)
        count = await perform_backup_physical_cleanup(prune=True)
        await event.respond(f"✅ **备份瘦身完成！**\n已清理各频道历史冗余文件，共移除 `{count}` 个旧版本副本。")
        await render_backup_manage_menu(event)

    elif cmd == 'clear':
        await event.answer("正在全量清空...", alert=False)
        db.clear_all_backup_runs(bot_name=BOT_NAME)
        count = await perform_backup_physical_cleanup(all_clear=True)
        db.recalc_backup_offsets(bot_name=BOT_NAME, clear_missing=True)
        await event.respond(f"🔥 **备份记录已清空！**\n已重置数据库并物理删除备份文件（共 `{count}` 个）。")
        await render_backup_manage_menu(event)

@bot.on(events.CallbackQuery(data=re.compile(br'bkdel_toggle_(.+)')))
async def backup_toggle_callback(event):
    run_id = int(event.data_match.group(1).decode('utf-8'))
    chat_id = event.chat_id
    
    if chat_id not in user_states: user_states[chat_id] = {}
    if 'selected_backups' not in user_states[chat_id]:
        user_states[chat_id]['selected_backups'] = set()
    
    sel_set = user_states[chat_id]['selected_backups']
    if run_id in sel_set:
        sel_set.remove(run_id)
    else:
        sel_set.add(run_id)
    
    await render_backup_manage_list(event)

async def perform_backup_physical_cleanup(run_time=None, channels=None, label=None, all_clear=False, prune=False):
    """
    执行物理文件清理
    run_time: 指定删除的时间戳 (YYYY-MM-DDTHH:MM:SS)
    channels: 当次备份涉及的频道详情
    label: 备份编号 (如 #B1), 优先级最高，用于精准全量搜索
    all_clear: 是否清空全部
    prune: 是否仅保留最新
    """
    import shutil
    data_root = 'data/archived/backups'
    docs_root = 'docs/archived/backups'
    deleted_count = 0

    if all_clear:
        # 清空全量目录（保留根目录）
        for root in [data_root, docs_root]:
            if os.path.exists(root):
                for sub in os.listdir(root):
                    path = os.path.join(root, sub)
                    try:
                        if os.path.isdir(path): shutil.rmtree(path)
                        else: os.remove(path)
                        deleted_count += 1
                    except: pass
        return deleted_count

    if prune:
        # 对每个频道，按修改时间保留最新一个文件
        for root in [data_root, docs_root]:
            if not os.path.exists(root): continue
            for folder in os.listdir(root): # 文件夹层
                folder_path = os.path.join(root, folder)
                if not os.path.isdir(folder_path): continue
                for chan in os.listdir(folder_path): # 频道层
                    chan_path = os.path.join(folder_path, chan)
                    if not os.path.isdir(chan_path): continue
                    
                    files = [os.path.join(chan_path, f) for f in os.listdir(chan_path)]
                    files.sort(key=os.path.getmtime, reverse=True)
                    if len(files) > 1:
                        for old in files[1:]:
                            try:
                                os.remove(old)
                                deleted_count += 1
                            except: pass
        return deleted_count

    if label:
        # [NEW] 方案一：根据编号搜索 (优先级最高)
        # 只要文件名包含如 "#B1_"，无论在哪个频道目录下，都将其物理切除
        for root in [data_root, docs_root]:
            if not os.path.exists(root): continue
            for d_root, _, d_files in os.walk(root):
                for f in d_files:
                    if f"{label}_" in f:
                        try:
                            os.remove(os.path.join(d_root, f))
                            deleted_count += 1
                        except: pass
        if deleted_count > 0:
            print(f"  🗑️ 已通过标签 {label} 物理驱逐 {deleted_count} 个孤儿文件")
            return deleted_count

    if run_time and channels:
        # [NEW] 方案二：根据 Metadata 和时间搜索 (兜底)
        # 转换时间戳格式匹配文件名 (YYYYMMDD_HHMMSS)
        # 统一处理 isoformat (2026-02-27T10:05:15) 和普通格式
        ts_clean = run_time.replace('-', '').replace(':', '').replace(' ', '_').replace('T', '_')
        ts_date = ts_clean[:8] # YYYYMMDD
        ts_time = ts_clean[9:13] # HHMM (前4位比较稳健)

        for ch in channels:
            # 1. 优先尝试精确匹配元数据中的路径
            for path_key in ['json_file', 'md_file']:
                path = ch.get(path_key)
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                        deleted_count += 1
                        continue # 命中则跳过后面的搜索
                    except: pass

            # 2. 兜底搜索逻辑 (用于没有路径元数据的老记录)
            ch_name_safe = "".join([c if c.isalnum() else "_" for c in ch.get('name', '')])
            for root in [data_root, docs_root]:
                if not os.path.exists(root): continue
                for d_root, d_dirs, d_files in os.walk(root):
                    for f in d_files:
                        # 匹配规则：包含日期，且包含频道名，且时间戳的前几位大致对得上
                        if ts_date in f and (ch_name_safe in f or ch.get('name') in f):
                            # 如果时间戳的前4位 (HHMM) 也包含在文件名里，则高概率匹配
                            if ts_time in f:
                                try:
                                    os.remove(os.path.join(d_root, f))
                                    deleted_count += 1
                                except: pass
    return deleted_count

@bot.on(events.NewMessage(pattern=r'^/search(?:\s+(.+))?$'))
@bot.on(events.NewMessage(pattern=r'^🔍 快捷检索$'))
async def search_handler(event):
    print(f"📥 Received search request: {event.text}")
    # 如果是点击按钮或带参数的命令
    if event.text == '🔍 快捷检索' or event.text == '/search':
        await render_search_center(event)
        return

    # 带关键词的搜索 /search keyword
    query = event.pattern_match.group(1)
    if query:
        await execute_advanced_search(event, query)
    else:
        await render_search_center(event)

@bot.on(events.NewMessage)
async def handle_all_messages(event):
    # 1. 强力过滤：跳过自己发的消息，防止自言自语
    if event.sender_id == me.id:
        return
    
    # 2. 权限校验
    admin_ids = CONFIG.get('admin_user_ids', [CONFIG.get('admin_user_id')])
    sender_id = event.sender_id
    is_admin = sender_id in admin_ids

    # 2. 检查是否被 @提到
    is_mentioned = False
    if me and me.username:
        mention_pattern = f"@{me.username}"
        if event.text and mention_pattern in event.text:
            is_mentioned = True

    # 3. 核心：如果在群组或频道中，且没有被 @提到，则绝对保持沉默
    if (event.is_group or event.is_channel) and not is_mentioned:
        return

    # 4. 如果是已确切识别的命令，则跳过处理，由对应的函数负责
    cmd_text = event.text.split()[0].lower() if event.text else ""
    if cmd_text.startswith('/') or event.text in ['🏠 主菜单', '🔍 快捷检索']:
        return

    # 4a. [NEW] 模式 4：捕获转发的消息以提取 msg_id
    if user_states.get(event.chat_id) == 'awaiting_mode_4_forward':
        if event.fwd_from:
            # 优先从转发元数据提取原始坐标
            fwd_chat_id = utils.get_peer_id(event.fwd_from.from_id) if event.fwd_from.from_id else None
            fwd_msg_id = event.fwd_from.channel_post or event.fwd_from.saved_from_msg_id
            o_chat_id, o_msg_id = None, None
            
            async with db_lock:
                # [Step 1] 优先尝试 ID 级别匹配 (最精准)
                if fwd_msg_id and fwd_chat_id:
                    # 尝试直接在 global_messages 中查找 (针对直接从源频道转发的情况)
                    row = db.cursor.execute('SELECT chat_id, msg_id FROM global_messages WHERE chat_id = ? AND msg_id = ?', (fwd_chat_id, fwd_msg_id)).fetchone()
                    if row: 
                        o_chat_id, o_msg_id = row
                    
                    # 尝试在 messages 同步表中查找 (针对从目标群组转发的情况)
                    if not o_chat_id:
                        row = db.cursor.execute('SELECT original_chat_id, original_msg_id FROM messages WHERE forwarded_chat_id = ? AND forwarded_msg_id = ?', (fwd_chat_id, fwd_msg_id)).fetchone()
                        if row: 
                            o_chat_id, o_msg_id = row

            # [Step 2] Fallback: 内容/指纹级别匹配 (针对个人转发、ID 丢失情况)
            if not o_chat_id:
                potential_matches = []
                
                async with db_lock:
                    # 方案 A: 媒体组 ID 匹配 (针对相册图集)
                    if event.grouped_id:
                        rows = db.cursor.execute('SELECT chat_id, msg_id FROM global_messages WHERE media_group_id = ?', (str(event.grouped_id),)).fetchall()
                        potential_matches.extend(rows)
                    
                    # 方案 B: 全文内容精确匹配 (针对带有长文案的消息)
                    if not potential_matches and event.text and len(event.text.strip()) > 10:
                        rows = db.cursor.execute('SELECT chat_id, msg_id FROM global_messages WHERE text_content = ?', (event.text.strip(),)).fetchall()
                        potential_matches.extend(rows)

                    # 方案 C: 文件大小匹配 (兜底方案，针对无文本且 ID 丢失的图片/视频)
                    if not potential_matches:
                        f_size = None
                        if event.document: f_size = event.document.size
                        elif event.photo: 
                            # 挑选最大的尺寸进行比对
                            f_size = event.photo.sizes[-1].size if hasattr(event.photo, 'sizes') else None
                        
                        if f_size:
                            rows = db.cursor.execute('SELECT chat_id, msg_id FROM global_messages WHERE file_size = ?', (f_size,)).fetchall()
                            potential_matches.extend(rows)

                # 方案 D: [NEW] 针对机器人生成的“消息头 (Header)”文本进行逆向解析
                if not o_chat_id and event.text:
                    # 匹配 同步号: TEST-1 或 #1 (带 P 前缀支持副 Bot, 支持全角冒号)
                    m_label = re.search(r'同步号[:：]\s*(P?)(TEST-|#)(\d+)', event.text)
                    m_res = re.search(r'资源[:：]\s*#(\d+)', event.text)
                    
                    if m_label:
                        prefix = m_label.group(1)
                        mode_type = m_label.group(2)
                        num = int(m_label.group(3))
                        
                        target_bot = 'my_porn_private_bot' if prefix == 'P' else 'tgporncopilot'
                        is_test = 1 if 'TEST' in mode_type else 0
                        
                        async with db_lock:
                            # 查找 run_id
                            run_row = None
                            if is_test:
                                run_row = db.cursor.execute('SELECT run_id FROM sync_runs WHERE bot_name = ? AND is_test = 1 AND test_number = ?', (target_bot, num)).fetchone()
                            else:
                                run_row = db.cursor.execute('SELECT run_id FROM sync_runs WHERE bot_name = ? AND is_test = 0 AND formal_number = ?', (target_bot, num)).fetchone()
                            
                            if run_row:
                                target_run_id = run_row[0]
                                print(f"📊 RunID Found: {target_run_id} for {target_bot} (num={num} test={is_test})")
                                # 如果有资源号 (#1), 则在 messages 表中查找对应的第 N 条记录
                                if m_res:
                                    res_idx = int(m_res.group(1)) - 1 # 0-indexed
                                    msg_row = db.cursor.execute(
                                        'SELECT original_chat_id, original_msg_id FROM messages WHERE sync_run_id = ? ORDER BY id ASC LIMIT 1 OFFSET ?', 
                                        (target_run_id, res_idx)
                                    ).fetchone()
                                    if msg_row:
                                        o_chat_id, o_msg_id = msg_row
                                        print(f"🧩 Label Match Success: Found via {m_label.group(0)} / Res #{res_idx+1}")
                    
                    # 补充方案：匹配原消息的时间戳 (原始发布: 2026-02-25 18:55)
                    if not o_chat_id:
                        m_time = re.search(r'原始发布[:：]\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})', event.text)
                        if m_time:
                            t_str = m_time.group(1)
                            async with db_lock:
                                # 在 global_messages 中按时间检索
                                time_rows = db.cursor.execute('SELECT chat_id, msg_id FROM global_messages WHERE original_time LIKE ?', (t_str + '%',)).fetchall()
                                if len(time_rows) == 1:
                                    o_chat_id, o_msg_id = time_rows[0]
                                elif len(time_rows) > 1:
                                    potential_matches.extend(time_rows)

                # 去重
                potential_matches = list(set(potential_matches))
                
                if len(potential_matches) == 1:
                    o_chat_id, o_msg_id = potential_matches[0]
                elif len(potential_matches) > 1:
                    # 尝试进一步过滤：如果是因为相册导致的多条记录，且都属于同一个 chat，则取第一条
                    sub_groups = set([m[0] for m in potential_matches])
                    if len(sub_groups) == 1:
                        o_chat_id, o_msg_id = min(potential_matches, key=lambda x: x[1])
                        if len(potential_matches) > 1:
                            # [NEW] 双重关联逻辑：检查是否有“锚点”上下文
                            m4_ctx = user_states.get(event.chat_id)
                            if isinstance(m4_ctx, dict) and 'anchor' in m4_ctx:
                                a_chat, a_msg = m4_ctx['anchor']
                                # 如果锚点在匹配项中，直接跳过歧义选择
                                if (a_chat, a_msg) in potential_matches:
                                    o_chat_id, o_msg_id = a_chat, a_msg
                                    print(f"🔗 Dual-Link Success: Found anchor {a_msg} in matches.")
                            
                        if not o_chat_id:
                            if len(potential_matches) == 1:
                                o_chat_id, o_msg_id = potential_matches[0]
                            else:
                                # 提示用户手动选择
                                choice_btns = []
                                for m_chat_id, m_msg_id in potential_matches[:8]:
                                    c_name = db.cursor.execute('SELECT chat_name FROM global_messages WHERE chat_id = ? LIMIT 1', (m_chat_id,)).fetchone()
                                    name = c_name[0] if c_name else f"Unknown ({m_chat_id})"
                                    choice_btns.append([Button.inline(f"📁 {name} (ID: {m_msg_id})", f"m4_select_{m_chat_id}_{m_msg_id}".encode())])
                                
                                await event.respond(
                                    "🗳️ **检测到多重匹配**\n由于内容重复且元数据丢失，无法自动识别。请选择目标记录，或转发其**带文字的消息头**：",
                                    buttons=choice_btns
                                )
                                return

            if o_chat_id:
                # [NEW] 设置锚点，方便后续转发关联
                user_states[event.chat_id] = {'anchor': (o_chat_id, o_msg_id), 'time': time.time()}
            else:
                await event.respond("❌ **识别失败**\n找不到记录。建议：\n1. 转发**带文字的消息头**以建立锚点\n2. 确保消息已同步进库。")
                return

            await show_m4_metadata_preview(event, o_chat_id, o_msg_id)
            return
        else:
            await event.respond("⚠️ 请确保您是从目标群组**转发**了一条消息过来，而不是直接发送。")
            return

    text = event.text.lower().strip() if event.text else ""
    chat_id = event.chat_id

    # 5. 检查用户是否有待处理的交互状态 (如正在搜索)
    state = user_states.get(chat_id)
    if state == 'awaiting_search_creator':
        user_states.pop(chat_id, None)
        await execute_advanced_search(event, event.text, search_type='creator')
        return
    elif state == 'awaiting_search_actor':
        user_states.pop(chat_id, None)
        await execute_advanced_search(event, event.text, search_type='actor')
        return
    elif state == 'awaiting_search_keyword':
        user_states.pop(chat_id, None)
        await execute_advanced_search(event, event.text, search_type='keyword')
        return
    elif state and state.startswith('awaiting_m4_field_'):
        # 模式 4：按字段更新元数据 (creator, actor, keywords, supplement)
        parts = state.split('_')
        field = parts[3]
        o_chat_id = int(parts[4])
        o_msg_id = int(parts[5])
        
        new_val = event.text.strip()
        if new_val:
            # 执行更新
            db.cursor.execute(f'UPDATE global_messages SET {field} = ?, is_extracted = 0 WHERE chat_id = ? AND msg_id = ?', (new_val, o_chat_id, o_msg_id))
            db.conn.commit()
            
            user_states.pop(chat_id, None)
            await event.respond(f"✅ **【{field}】已更新为**:\n`{new_val}`\n\n🔃 正在同步本地标签文档...")
            
            # [NEW] 触发文件同步
            await trigger_index_export()
            
            await show_m4_metadata_preview(event, o_chat_id, o_msg_id)
        return

    # 6. 响应单纯的 "/" 或 问候语 (非命令模式下的交互)
    if text == '/' or any(greet in text for greet in ['你好', 'hi', 'hello', 'hey']):
        await show_help_message(event)
    elif text == '🔄 刷新归档':
        if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
            await event.respond('⚠️ 只有管理员可以执行此操作。')
            return
        await trigger_metadata_refresh(event, is_manual=True)
    else:
        # 只有在私聊或者被 @ 的情况下，才会回复“不明白”
        await event.respond('抱歉，我不明白你的意思。请输入 `/help` 查看命令列表，或者发送 `/sync` 开始同步资源。')

async def trigger_index_export(bot_name=None):
    """手动/自动触发 Program 3: 索引导出"""
    if not bot_name:
        bot_name = CONFIG.get('app_name') or 'tgporncopilot'
    
    print(f"📊 Triggering index export for {bot_name}...")
    try:
        from search_mode.program3_export.index_exporter import IndexExporter
        # [DEFENSIVE] 确保 bot_name 绝不为 None
        if not bot_name:
            print("⚠️ trigger_index_export skipped: bot_name is None")
            return False
            
        exporter = IndexExporter(bot_name=str(bot_name))
        # 异步运行导出 (如果不耗时太长，同步运行也可)
        exporter.export()
        print("✅ Index export completed.")
        return True
    except Exception as e:
        import traceback
        print(f"⚠️ Index export failed: {e}")
        traceback.print_exc()
        return False

# ===== 其他全局功能回调 =====

@bot.on(events.CallbackQuery(data=b'nav_refresh_metadata'))
async def callback_refresh_metadata(event):
    if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
        await event.answer('⚠️ 只有管理员可以执行此操作。', alert=True)
        return
    await event.answer("🔄 正在启动元数据刷新...")
    await trigger_metadata_refresh(event, is_manual=True)

# ===== 树状图回调处理 =====

@bot.on(events.CallbackQuery(data=re.compile(br'tree_fld_(sync_\d|backup_\d)_([01])_(\d+)')))
async def tree_folder_toggle_callback(event):
    action_type = event.data_match.group(1).decode('utf-8')
    is_test = (event.data_match.group(2).decode('utf-8') == '1')
    folder_idx = event.data_match.group(3).decode('utf-8')

    chat_id = event.chat_id
    exp_key = f"tree_exp_{action_type}"
    if chat_id not in user_selections: user_selections[chat_id] = {}
    if exp_key not in user_selections[chat_id]: user_selections[chat_id][exp_key] = set()

    s = user_selections[chat_id][exp_key]
    if folder_idx in s:
        s.remove(folder_idx)
        print(f"📁 Collapsing folder {folder_idx} for {action_type}")
    else:
        s.add(folder_idx)
        print(f"📂 Expanding folder {folder_idx} for {action_type}")

    await render_tree_ui(event, action_type, is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'tree_tgl_(.+)_(0|1)_(\d+)')))
async def tree_toggle_callback(event):
    action_type = event.data_match.group(1).decode('utf-8')
    is_test = (event.data_match.group(2).decode('utf-8') == '1')
    peer_id = event.data_match.group(3).decode('utf-8')

    chat_id = event.chat_id
    sel_key = f"tree_{action_type}"
    if chat_id in user_selections and sel_key in user_selections[chat_id]:
        s = user_selections[chat_id][sel_key]
        if peer_id in s: s.remove(peer_id)
        else: s.add(peer_id)

    await render_tree_ui(event, action_type, is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'tree_sel(all|none)_(.+)_(0|1)')))
async def tree_bulk_callback(event):
    action = event.data_match.group(1).decode('utf-8')
    action_type = event.data_match.group(2).decode('utf-8')
    is_test = (event.data_match.group(3).decode('utf-8') == '1')

    chat_id = event.chat_id
    sel_key = f"tree_{action_type}"
    if chat_id not in user_selections: user_selections[chat_id] = {}

    if action == 'all':
        all_folders = await get_all_folder_peers()
        user_selections[chat_id][sel_key] = {p['id'] for _, peers in all_folders for p in peers if p['is_syncable']}
    else:
        user_selections[chat_id][sel_key] = set()

    await render_tree_ui(event, action_type, is_test)

@bot.on(events.CallbackQuery(data=re.compile(br'tree_run_(.+)_(0|1)')))
async def tree_run_callback(event):
    action_type = event.data_match.group(1).decode('utf-8')
    is_test = (event.data_match.group(2).decode('utf-8') == '1')

    chat_id = event.chat_id
    sel_key = f"tree_{action_type}"
    selections = user_selections.get(chat_id, {}).get(sel_key, set())
    if not selections:
        await event.answer('⚠️ 请先勾选同步/备份目标', alert=True)
        return

    ids_str = ",".join(list(selections))
    is_sync = action_type.startswith('sync')
    act_name = "同步" if is_sync else "备份"
    parts = action_type.split('_')
    mode = parts[1] if len(parts) > 1 else "1"
    is_inc = (parts[2] == 'inc') if len(parts) > 2 else False

    await event.answer(f'正在启动所选频道的{act_name}...', alert=False)
    try:
        await event.edit(f'⏳ **正在执行{act_name} (模式 {mode}{" - 增量" if is_inc else ""})**\n\n任务已异步启动，完成后将通知。', buttons=None)
    except:
        pass

    if is_sync:
        await execute_sync(event, mode, folder="多选树", ids=ids_str, is_test=is_test)
    else:
        # 实现备份的多选运行
        await execute_backup(event, mode, ids=ids_str, is_test=is_test, incremental=is_inc)

async def execute_advanced_search(event, query, search_type='keyword'):
    """执行深度检索并渲染带 Deep Link 的结果"""
    print(f"🔍 Executing {search_type} search for: {query}")
    rows = db.search_with_sync_links(query, search_type=search_type)
    
    if not rows:
        await event.respond(f"❌ 未找到与 `{query}` 相关的资源。")
        return

    # 清洗群组 ID (去掉 -100 前缀) 用于构建 Deep Link
    clean_group_id = str(TARGET_GROUP_ID).replace('-100', '')
    
    # 构建响应文本
    type_name = '关键词' if search_type=='keyword' else '创作者' if search_type=='creator' else '模特'
    lines = [f"📊 **检索结果 ({type_name}): `{query}`**", "━━━━━━━━━━━━━━"]
    
    count = 0
    for r in rows:
        # 匹配 SQL: g.chat_name, g.msg_type, g.sender_name, g.original_time, g.text_content, m.forwarded_msg_id, g.chat_id, g.msg_id, g.search_tags
        chat_name, msg_type, sender, o_time, text, f_msg_id, o_chat_id, o_msg_id, tags = r
        
        # [FILTER] 仅展示含资源或含链接的消息
        has_url = False
        if text and re.search(r'https?://\S+|t.me/\S+', text):
            has_url = True
            
        has_media = msg_type in ('video', 'photo', 'file', 'gif')
        has_link = msg_type in ('link', 'link_preview') or has_url
        
        if not (has_media or has_link):
            continue

        count += 1
        # 截取文本摘要
        summary = text[:50].replace('\n', ' ') if text else "无描述文案"
        time_str = o_time[:10] if o_time else "未知时间"
        
        icon = "🎬" if msg_type == 'video' else "🖼️" if msg_type == 'photo' else "📎"
        
        # 构建链接
        if f_msg_id:
            link = f"https://t.me/c/{clean_group_id}/{f_msg_id}"
            line = f"{count}. {icon} [{summary}]({link})\n   └ 📅 `{time_str}` | 👤 `{sender or chat_name}`"
        else:
            # 如果没同步过，仅展示本地备份存在
            line = f"{count}. {icon} {summary} (仅备份)\n   └ 📅 `{time_str}` | 👤 `{sender or chat_name}`"
            
        if tags:
            line += f" | 🏷️ `{tags}`"
            
        lines.append(line)
        
        if count >= 15: # 最多展示 15 条
            lines.append("... (更多结果请缩小搜索范围)")
            break

    if count == 0:
        await event.respond(f"❌ 未找到包含媒体或链接的 `{query}` 相关资源。")
        return

    final_text = "\n".join(lines)
    
    # [OPTIMIZATION] 如果是从按钮触发的（CallbackQuery），先尝试编辑原菜单
    # 这样用户感觉是“切换”到了结果页面
    try:
        if isinstance(event, events.CallbackQuery.Event):
            await event.edit(final_text, link_preview=False, buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]])
        else:
            await event.respond(final_text, link_preview=False)
    except Exception as e:
        print(f"⚠️ execute_advanced_search failed: {e}")

# ===== Mode 3: 检索与分析中心 =====

@bot.on(events.CallbackQuery(data=b'nav_discovery_pipeline'))
async def nav_discovery_pipeline_callback(event):
    await trigger_discovery_pipeline(event)

def get_latest_backup_time(managed_folders):
    """获取管辖范围内所有备份 JSON 文件的最新修改时间"""
    backup_base = 'data/archived/backups'
    max_mtime = 0
    for folder in managed_folders:
        folder_path = os.path.join(backup_base, folder)
        if not os.path.exists(folder_path):
            continue
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.json') and not file.startswith('metadata'):
                    try:
                        mtime = os.path.getmtime(os.path.join(root, file))
                        if mtime > max_mtime:
                            max_mtime = mtime
                    except: pass
    return max_mtime

async def trigger_discovery_pipeline(event):
    """
    工作模式三流水线：P0 (入库) -> P1 (聚类提取) -> P1.5 (Web Sorter)
    """
    if sync_job_lock.locked():
        await safe_answer(event, "⚠️ 当前有任务正在运行中，请稍后", alert=True)
        return

    async with sync_job_lock:
        msg = await event.edit("🚀 **正在启动发现流水线 (Discovery Pipeline)...**\n\n🔍 正在检查备份文件可用性...")
        py = sys.executable
        
        try:
            # [V2.3强化] 严谨校验：元数据存在 + MD文件存在 + ID与时间戳匹配
            # 同时校验第一个分卷是否有内容，防止空文件导致 Web 端空白
            root_path = CONFIG.get('root_path', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
            candidates_docs = CONFIG.get('candidates_dir_docs')
            
            if candidates_docs:
                meta_path = os.path.join(root_path, candidates_docs, 'candidate_metadata.json')
                pool_file = os.path.join(root_path, candidates_docs, 'candidate_pool_part_1.md')
            else:
                meta_path = pool_file = None
            
            skip_p0_p1 = False
            dbg_msg = ""
            if meta_path and os.path.exists(meta_path) and pool_file and os.path.exists(pool_file):
                # 如果文件大小为 0，视为损坏/空，需要重跑
                if os.path.getsize(pool_file) < 10:
                    dbg_msg = " (候选池文件为空)"
                else:
                    try:
                        with open(meta_path, 'r', encoding='utf-8') as f:
                            m_data = json.load(f)
                            cached_backup_id = m_data.get('latest_backup_id', 'NONE')
                            cached_mtime = m_data.get('max_mtime', 0)
                            
                            current_backup_id = db.get_bot_latest_backup_label(BOT_NAME)
                            current_mtime = get_latest_backup_time(CONFIG.get('managed_folders', []))
                            
                            # [DEBUG]
                            print(f"[*] Pipeline Check: Current={current_backup_id}/{current_mtime}, Cached={cached_backup_id}/{cached_mtime}")
                            
                            if current_backup_id != "NONE" and current_backup_id == cached_backup_id:
                                if abs(current_mtime - cached_mtime) < 1.0:
                                    skip_p0_p1 = True
                                    skip_label = f"{cached_backup_id} (@{time.strftime('%H:%M:%S', time.localtime(cached_mtime))})"
                                else:
                                    dbg_msg = " (检测到备份文件内容有变)"
                            else:
                                dbg_msg = f" (备份 ID 不匹配: {cached_backup_id} vs {current_backup_id})"
                    except Exception as e:
                        print(f"⚠️ Pipeline Check Error: {e}")
                        dbg_msg = " (元数据解析失败)"
            else:
                missing = []
                if not (meta_path and os.path.exists(meta_path)): missing.append("元数据")
                if not (pool_file and os.path.exists(pool_file)): missing.append("候选池")
                dbg_msg = f" (缓存文件缺失: {', '.join(missing)})"
            
            if skip_p0_p1:
                await msg.edit(f"🚀 **发现流水线 (Discovery Pipeline)**\n\n✅ 检测到本地候选词池已是最新 ({skip_label})，正在直接恢复分拣界面...")
                await asyncio.sleep(1.5)
            else:
                await msg.edit(f"🚀 **正在启动发现流水线 (Discovery Pipeline)...**\n\n🔍 状态: {dbg_msg}\n1️⃣ 准备执行 Program 0: 备份增量投影入库...")
                # Step 1: Program 0 - Import Backups
                p0 = await asyncio.create_subprocess_shell(
                    f'"{py}" src/search_mode/program1_discovery/import_backups.py --bot "{BOT_NAME}"',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    encoding='utf-8'
                )
                await p0.communicate()
                
                # [FIX] 使用绝对路径，且根据 Bot 名称隔离
                root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                progress_file = os.path.join(root_path, f'data/temp/extractor_progress_{BOT_NAME}.json')
                pipeline_log = os.path.join(root_path, f'data/logs/pipeline_{BOT_NAME}.log')
                os.makedirs(os.path.dirname(pipeline_log), exist_ok=True)
                
                # Step 2: Program 1 - Entity Extraction
                await msg.edit("🚀 **发现流水线 (Discovery Pipeline)**\n\n1️⃣ P0 入库已完成\n2️⃣ 正在启动 Program 1: NLP 关键词发现与聚类分析...")
                
                latest_id = db.get_bot_latest_backup_label(BOT_NAME)
                # 使用 append 模式记录日志，方便追踪错误
                with open(pipeline_log, "a", encoding="utf-8") as log_f:
                    p1 = await asyncio.create_subprocess_shell(
                        f'"{py}" src/search_mode/program1_discovery/entity_extractor.py --bot "{BOT_NAME}" --backup-id "{latest_id}" --progress-file "{progress_file}"',
                        stdout=log_f, stderr=log_f
                    )
                
                start_time = time.time()
                last_msg_text = ""
                
                # [NEW] 清理旧进度文件，确保百分比条是从零开始的
                if os.path.exists(progress_file):
                    try: os.remove(progress_file)
                    except: pass
                
                while True:
                    if os.path.exists(progress_file):
                        try:
                            with open(progress_file, 'r', encoding='utf-8') as f:
                                pdata = json.load(f)
                                status = pdata.get('status', 'processing')
                                
                                new_text = ""
                                if status == 'initializing':
                                    new_text = (
                                        f"🚀 **正在启动关键词分析 (Program 1)...**\n\n"
                                        f"🧠 **正在初始化 NLP 引擎词典...**\n"
                                        f"💡 *小提示：加载大型词库可能需要 10-30 秒，请耐心等待。*"
                                    )
                                elif status == 'scanning':
                                    cur = pdata.get('files_done', 0)
                                    total = pdata.get('total_files', 1)
                                    fname = pdata.get('current_file', '...')
                                    pct = (cur / total) * 100 if total > 0 else 0
                                    bar = "█" * int(pct/5) + "░" * (20 - int(pct/5))
                                    new_text = (
                                        f"🚀 **正在进行关键词分析 (Program 1)...**\n\n"
                                        f"📊 进度: `[{bar}]` {pct:.1f}%\n"
                                        f"📂 正在处理: `{fname}`\n"
                                        f"⏱️ 已耗时: `{int(time.time() - start_time)}s`"
                                    )
                                elif status == 'completed':
                                    break
                                
                                if new_text and new_text != last_msg_text:
                                    try:
                                        await msg.edit(new_text)
                                        last_msg_text = new_text
                                    except: pass # 忽略 MessageNotModified 等错误
                        except: pass

                    try:
                        await asyncio.wait_for(p1.wait(), timeout=0.1)
                        break
                    except asyncio.TimeoutError:
                        pass

                    await asyncio.sleep(3)
                
                await p1.wait()
                
                # [FIX] 增加验证：确保 Program 1 成功结束且生成了文件
                if p1.returncode != 0:
                    await msg.edit(f"❌ **关键词分析失败 (ExitCode: {p1.returncode})**\n请检查 `data/logs/pipeline_{BOT_NAME}.log` 了解详情。")
                    return
                
                if not os.path.exists(pool_file) or os.path.getsize(pool_file) < 10:
                    await msg.edit("⚠️ **分析已完成但未提取到任何新候选词**\n可能当前备份中的所有实体均已在词库中。")
                    return

            # Step 3: Launch Web Sorter (P1.5)
            await msg.edit("✅ **分析完成！正在挂载 Web 分拣工具...**")
            await _launch_p15_and_notify(bot, event.chat_id, msg.id, py)
            
        except Exception as e:
            await msg.edit(f"❌ **流水线执行异常**:\n`{e}`")

async def render_search_center(event, is_edit=False):
    """渲染检索分析中心主界面 (Mode 3)"""
    buttons = [
        [Button.inline("🏢 创作者检索", b"sc_search_creator")],
        [Button.inline("👠 主要人物检索", b"sc_search_actor")],
        [Button.inline("🏷️ 按类目检索 (Tags)", b"nav_search_categories")],
        [Button.inline("🔍 全局关键词搜索", b"sc_search_keyword")],
        [Button.inline("🗂️ 本地索引管理 (entities.json)", b"nav_entity_manage")],
        [Button.inline("🔄 更新词库 (Discovery Pipeline)", b"nav_discovery_pipeline")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")]
    ]
    title = (
        "🔍 **Telegram Video Copilot 搜索中心**\n\n"
        "您可以通过以下维度快速锁定资源：\n"
        "━━━━━━━━━━━━━━\n"
        "💡 **提示**：直接发送搜索词给我，也可以触发全局搜索。"
    )
    try:
        if is_edit: await event.edit(title, buttons=buttons)
        else: await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_search_center error: {e}")

@bot.on(events.CallbackQuery(data=b'nav_entity_manage'))
async def nav_entity_manage_callback(event):
    """启动 Sorter 并直跳管理页面"""
    msg = await event.edit("⏳ **正在载入词库管理工具...**")
    py = sys.executable
    await _launch_p15_and_notify(bot, event.chat_id, msg.id, py, view='manager')

@bot.on(events.CallbackQuery(data=b'nav_search_categories'))
async def nav_search_categories_callback(event):
    """展示关键词分类菜单"""
    entities = db.get_entities_v2() # 我们需要一个新的 DB 方法来获取带分类的统计
    keywords = entities.get('keywords', {})
    
    lines = ["🏷️ **层级分类检索**\n\n请选择关键词分类："]
    buttons = []
    
    for cat in sorted(keywords.keys()):
        count = len(keywords[cat])
        buttons.append([Button.inline(f"📁 {cat} ({count})", f"sc_cat_{cat}".encode())])
    
    buttons.append([Button.inline("⚙️ 管理关键词分组", b"nav_entity_manage")])
    buttons.append([Button.inline("🔙 返回", b"nav_search_center")])
    await event.edit("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'sc_cat_(.+)')))
async def sc_cat_list_callback(event):
    cat_name = event.data_match.group(1).decode()
    entities = db.get_entities_v2()
    kws = entities.get('keywords', {}).get(cat_name, [])
    
    lines = [f"🏷️ **分类: {cat_name}**\n点击关键词直接搜索："]
    buttons = []
    
    # 关键词每行 2 个
    temp_row = []
    for kw in kws:
        name = kw['name']
        temp_row.append(Button.inline(name, f"do_search_{name}".encode()))
        if len(temp_row) == 2:
            buttons.append(temp_row)
            temp_row = []
    if temp_row: buttons.append(temp_row)
    
    buttons.append([Button.inline("🔙 返回分类列表", b"nav_search_categories")])
    await event.edit("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'do_search_(.+)')))
async def do_search_button_callback(event):
    query = event.data_match.group(1).decode()
    # 智能探测 query 类型（简单处理：如果在演员表里就搜演员，否则关键词）
    # 或者通过 callback data 显式传递类型。这里为了兼容现有代码，采取稍微智能的 fallback
    stype = 'keyword'
    # ... 
    await event.answer(f"🔍 正在搜索: {query}", alert=False)
    await execute_advanced_search(event, query, search_type=stype)

@bot.on(events.CallbackQuery(data=re.compile(br'sc_search_(.+)')))
async def search_input_trigger_callback(event):
    stype = event.data_match.group(1).decode()
    user_states[event.chat_id] = f'awaiting_search_{stype}'
    
    label_map = {"creator":"作者", "actor":"人物", "keyword":"关键词"}
    label = label_map.get(stype, "搜索词")
    
    if stype in ['creator', 'actor']:
        await render_alphabet_selector(event, stype)
        return

    await event.edit(f"🔍 **按{label}搜索**\n\n请输入您要查找的名称：", 
                     buttons=[[Button.inline("🔙 取消", b"nav_search_center")]])
    cmd = event.data_match.group(1).decode('utf-8')
    chat_id = event.chat_id
    
    # [REMOVED] Logic moved to global _launch_p15_and_notify

    # ─── 子菜单：选择正式更新 or 测试更新 (支持状态感知) ───
    if cmd == 'update_pipeline':
        config = CONFIG
        cand_dir = config.get('candidates_dir', f'docs/entities/{BOT_NAME}_candidates')
        meta_path = os.path.join(cand_dir, 'candidate_metadata.json')
        
        status_text = "🆕 **尚未生成过候选词池**\n建议执行【正式更新】开始挖掘。"
        is_stale = True
        has_task = False
        
        # 1. 查找本地备份中最新的时间戳
        backup_root = 'data/archived/backups'
        latest_backup_mtime = 0
        if os.path.exists(backup_root):
            for r, d, f in os.walk(backup_root):
                for file in f:
                    if file.endswith('.json'):
                        try:
                            m = os.path.getmtime(os.path.join(r, file))
                            if m > latest_backup_mtime: latest_backup_mtime = m
                        except: pass
        
        # 2. 读取候选池元数据
        if os.path.exists(meta_path):
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                cand_total = meta.get('candidate_count', 0)
                cand_mtime = meta.get('max_mtime', 0)
                has_task = cand_total > 0
                
                # 时间戳对比 (误差允许 60 秒)
                if cand_mtime >= latest_backup_mtime - 60:
                    status_text = f"✅ **数据已是最新** (包含覆盖至 {datetime.fromtimestamp(cand_mtime).strftime('%Y-%m-%d %H:%M')})\n剩余待处理词: {cand_total} 个"
                    is_stale = False
                else:
                    status_text = f"🔴 **数据已过期** (当前池仅覆盖至 {datetime.fromtimestamp(cand_mtime).strftime('%Y-%m-%d %H:%M')})\n检测到新备份，建议重新扫描。"
            except: pass

        # 3. 构造按钮
        buttons = []
        if has_task and not is_stale:
            # 数据最新且有词，优先引导进入 P1.5
            buttons.append([Button.inline("🚀 恢复/继续分拣 (网页)", b"sc_update_pipeline_test")])
            buttons.append([Button.inline("🔄 重新扫描提取 (P0→P1)", b"sc_update_pipeline_full")])
        elif has_task and is_stale:
            # 数据过期但有词，引导重扫，但也允许硬进网页
            buttons.append([Button.inline("🆕 开始新一轮提取 (重扫)", b"sc_update_pipeline_full")])
            buttons.append([Button.inline("🧪 强制进入旧网页 (不推荐)", b"sc_update_pipeline_test")])
        else:
            # 无数据
            buttons.append([Button.inline("🚀 开始首次提取 (正式更新)", b"sc_update_pipeline_full")])
            buttons.append([Button.inline("🧪 仅启动空网页 (测试模式)", b"sc_update_pipeline_test")])
            
        buttons.append([Button.inline("🔙 返回搜索中心", b"nav_search_center")])

        await event.edit(
            "🔄 **分拣流水线管理**\n\n"
            f"{status_text}\n\n"
            "💡 *提示：正式更新会全量扫描备份并更新候选词，耗时较久；测试模式直接启动当前已有的分拣环境。*",
            buttons=buttons
        )

    elif cmd == 'update_pipeline_full':
        msg_to_edit = await event.edit(
            "\U0001f504 **\u6b63\u5728\u542f\u52a8\u8bcd\u5e93\u66f4\u65b0\u6d41\u6c34\u7ebf...**\n\n"
            "\u6b65\u9aa4\uff1a\n1\ufe0f\u20e3 \u5408\u5e76\u6700\u65b0\u5907\u4efd (P0)\n2\ufe0f\u20e3 \u805a\u7c7b\u5206\u6790\u5019\u9009\u8bcd (P1)\n3\ufe0f\u20e3 \u542f\u52a8\u7f51\u9875\u5206\u62e3\u5de5\u5177 (P1.5)\n\n\u231b **\u6b63\u5728\u521d\u59cb\u5316\uff0c\u8bf7\u7a0d\u5019...**",
            buttons=None
        )
        async def run_upd():
            py = sys.executable
            chat_id = event.chat_id
            target_msg_id = msg_to_edit.id
            progress_file = 'data/temp/extractor_progress.json'
            print(f"DEBUG: Starting run_upd. Bot: {BOT_NAME}, TargetMsg: {target_msg_id}")
            if os.path.exists(progress_file):
                try: os.remove(progress_file)
                except: pass
            try:
                p_upd = await asyncio.create_subprocess_shell(
                    f'"{py}" src/search_mode/program1_discovery/entity_extractor.py --bot "{BOT_NAME}"',
                    stdout=asyncio.subprocess.DEVNULL
                )
                print(f"DEBUG: Subprocess started with PID: {p_upd.pid}")
                last_edit_text = ""
                while True:
                    if os.path.exists(progress_file):
                        try:
                            with open(progress_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            status = data.get('status')
                            new_text = ""
                            if status == 'initializing':
                                new_text = "\u231b **\u6b63\u5728\u521d\u59cb\u5316 NLP \u5206\u8bcd\u5f15\u64ce...**"
                            elif status == 'scanning':
                                done = data.get('files_done', 0)
                                total = data.get('total_files', 0)
                                msgs = data.get('total_msgs', 0)
                                pct = int(done / total * 100) if total else 0
                                bar_len = 15
                                filled = int(bar_len * done / total) if total else 0
                                bar = "\u2588" * filled + "\u2591" * (bar_len - filled)
                                new_text = (
                                    f"\U0001f504 **\u8bcd\u5e93\u66f4\u65b0\u6d41\u6c34\u7ebf \u00b7 \u626b\u63cf\u4e2d**\n\n"
                                    f"\U0001f4ca \u8fdb\u5ea6: [{bar}] {pct}%\n"
                                    f"\U0001f4c1 \u6587\u4ef6: {done}/{total}\n"
                                    f"\U0001f4ac \u6d88\u606f: {msgs:,} \u6761\n"
                                    f"\U0001f4c4 \u5f53\u524d: `{data.get('current_file', '')}`"
                                )
                            elif status == 'completed':
                                total_msgs = data.get('total_msgs', 0)
                                new_text = f"\u2705 \u626b\u63cf\u5b8c\u6210\uff01\u5171\u5904\u7406 {total_msgs:,} \u6761\u6d88\u606f\uff0c\u6b63\u5728\u751f\u6210\u5019\u9009\u6c60..."
                            if new_text and new_text != last_edit_text:
                                try:
                                    await event.client.edit_message(chat_id, target_msg_id, new_text, buttons=None)
                                    last_edit_text = new_text
                                except Exception as e_edit:
                                    print(f"DEBUG: Edit failed: {e_edit}")
                        except: pass

                    try:
                        await asyncio.wait_for(p_upd.wait(), timeout=0.1)
                        break
                    except asyncio.TimeoutError:
                        pass

                    await asyncio.sleep(5)
                await p_upd.wait()
                await event.client.edit_message(chat_id, target_msg_id, "\u2705 \u626b\u63cf\u5b8c\u6210\uff0c\u6b63\u5728\u5f00\u542f\u7f51\u9875\u5206\u62e3\u670d\u52a1 (P1.5)...")
                await _launch_p15_and_notify(event.client, chat_id, target_msg_id, py)
            except Exception as e:
                try: await event.client.edit_message(chat_id, target_msg_id, f"\u274c \u7cfb\u7edf\u4efb\u52a1\u5f02\u5e38: {e}")
                except: pass
        asyncio.create_task(run_upd())

    elif cmd == 'update_pipeline_test':
        msg_to_edit = await event.edit(
            "\U0001f9ea **\u6d4b\u8bd5\u6a21\u5f0f\uff1a\u6b63\u5728\u542f\u52a8\u5206\u62e3\u7f51\u9875...**\n\n"
            "\u23ed\ufe0f \u5df2\u8df3\u8fc7 P0/P1 \u626b\u63cf\u9636\u6bb5\n"
            "\u231b \u6b63\u5728\u52a0\u8f7d\u5df2\u6709\u7684\u5019\u9009\u8bcd\u6570\u636e\u5e76\u542f\u52a8 Web \u670d\u52a1...",
            buttons=None
        )
        async def run_test():
            py = sys.executable
            chat_id = event.chat_id
            target_msg_id = msg_to_edit.id
            try:
                await _launch_p15_and_notify(event.client, chat_id, target_msg_id, py)
            except Exception as e:
                try: await event.client.edit_message(chat_id, target_msg_id, f"\u274c \u6d4b\u8bd5\u542f\u52a8\u5f02\u5e38: {e}")
                except: pass
        asyncio.create_task(run_test())

        
    elif cmd == 'search_creator': await render_entity_list(event, 'creator', 0)
    elif cmd == 'search_actor': await render_entity_list(event, 'actor', 0)
    elif cmd == 'search_tag': await render_entity_list(event, 'tag', 0)
    elif cmd == 'search_keyword':
        user_states[chat_id] = 'awaiting_search_keyword'
        await event.edit("🔍 **全局自由搜索**\n\n请输入搜索词（支持模糊匹配文件名、标签或描述）：", 
                         buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]])
    
    elif cmd.startswith('append_'):
        etype = cmd.split('_')[1]
        user_states[chat_id] = f'awaiting_entity_append_{etype}'
        label = {"creator":"创作者", "actor":"女m/模特", "tag":"关键词"}.get(etype, etype)
        await event.edit(f"➕ **手动补充 {label}**\n\n直接发送名称给我，我将为您录入词库。\n(建议输入后再点击“更新检索数据库”同步索引)", 
                         buttons=[[Button.inline("取消", f"sc_list_{etype}_0".encode())]])
    elif cmd.startswith('list_'):
        parts = cmd.split('_') # list_{etype}_{offset}
        if len(parts) == 3: await render_entity_list(event, parts[1], int(parts[2]))

@bot.on(events.CallbackQuery(data=b'nav_mode_4_start'))
async def mode_4_start_callback(event):
    chat_id = event.chat_id
    user_states[chat_id] = 'awaiting_mode_4_forward'
    title = (
        "📥 **模式 4：手动补充信息 (Manual Append)**\n\n"
        "请从目标私密群组中**转发一条消息**给我，我将提取其 ID 并为您提供补充信息的功能。\n"
        "━━━━━━━━━━━━━━\n"
        "💡 **说明**：此功能用于为已同步的资源添加额外的标签、创作者或描述，使其更易被检索。"
    )
    await event.edit(title, buttons=[[Button.inline("取消", b"nav_main")]])

async def show_m4_metadata_preview(event, o_chat_id, o_msg_id):
    """显示模式 4 的元数据综合预览"""
    row = db.cursor.execute('''
        SELECT creator, actor, keywords, supplement, text_content 
        FROM global_messages 
        WHERE chat_id = ? AND msg_id = ?
    ''', (o_chat_id, o_msg_id)).fetchone()
    
    if not row:
        await event.respond("❌ 数据库中找不到该消息的记录。")
        return

    creator, actor, keywords, supplement, text_content = row
    
    msg_text = (
        f"✅ **识别成功！** (ID: `{o_msg_id}`)\n"
        f"━━━━━━━━━━━━━━\n"
        f"👤 **创作者**: `{creator or '未设置'}`\n"
        f"💃 **女m**: `{actor or '未设置'}`\n"
        f"🏷️ **关键词**: `{keywords or '未设置'}`\n"
        f"📝 **补充信息**: `{supplement or '未设置'}`\n"
        f"━━━━━━━━━━━━━━\n"
        f"📜 **原始文本预览**:\n`{text_content[:200] + '...' if text_content and len(text_content) > 200 else text_content or '无'}`\n\n"
        f"请选择您要修改的字段："
    )
    
    buttons = [
        [Button.inline("👤 修改创作者", f"m4_edit_{o_chat_id}_{o_msg_id}_creator".encode()),
         Button.inline("💃 修改女m", f"m4_edit_{o_chat_id}_{o_msg_id}_actor".encode())],
        [Button.inline("🏷️ 修改关键词", f"m4_edit_{o_chat_id}_{o_msg_id}_keywords".encode()),
         Button.inline("📝 修改补充信息", f"m4_edit_{o_chat_id}_{o_msg_id}_supplement".encode())],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")]
    ]
    
    # 根据事件类型决定是编辑还是回复 (防止在 NewMessage 时尝试编辑用户的转发消息)
    if isinstance(event, events.CallbackQuery.Event):
        await event.edit(msg_text, buttons=buttons)
    else:
        await event.respond(msg_text, buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'm4_(view|edit|back|select)_(-?\d+)_(\d+)(?:_(.+))?')))
async def mode_4_action_callback(event):
    data = event.data.decode('utf-8').split('_')
    # 统一格式: m4_{action}_{chat_id}_{msg_id}_{field}
    action = data[1]
    o_chat_id = int(data[2])
    o_msg_id = int(data[3])
    field = data[4] if len(data) > 4 else None
    
    if action == 'back':
        await mode_4_start_callback(event)
        return
    
    if action == 'select':
        await show_m4_metadata_preview(event, o_chat_id, o_msg_id)
        return
    
    if action == 'view':
        await show_m4_metadata_preview(event, o_chat_id, o_msg_id)
        return
        
    if action == 'edit':
        # field 已经在上方被提取
        field_map = {
            'creator': '创作者 (Creator)',
            'actor': '女m (Actor)',
            'keywords': '关键词 (Keywords)',
            'supplement': '补充信息 (Supplement)'
        }
        display_name = field_map.get(field, field)
        user_states[event.chat_id] = f'awaiting_m4_field_{field}_{o_chat_id}_{o_msg_id}'
        
        await event.respond(
            f"✍️ **正在修改【{display_name}】**\n\n请发送新的内容。发送后系统将**直接更新**数据库中的该项字段信息。",
            buttons=[Button.inline("取消并返回", f"m4_view_{o_chat_id}_{o_msg_id}".encode())]
        )
        await event.answer()

async def render_entity_review(event):
    """人工审核界面"""
    candidates = db.get_entities(status=0, limit=5)
    if not candidates:
        await event.answer("🎉 暂时没有待审核的候选人", alert=True)
        await render_search_center(event)
        return
        
    lines = ["🧪 **实体人工审核 (Candidate Review)**\n"]
    buttons = []
    
    for c in candidates:
        lines.append(f"• `{c['name']}` ({c['type']}) - 出现 {c['msg_count']} 次")
        buttons.append([
            Button.inline(f"✅ {c['name']} (创作者)", f"ent_1_creator_{c['id']}".encode()),
            Button.inline(f"💃 {c['name']} (演员)", f"ent_1_actor_{c['id']}".encode()),
            Button.inline("🚫 屏蔽", f"ent_2_none_{c['id']}".encode())
        ])
        
    buttons.append([Button.inline("⬅️ 返回检索中心", b"nav_search")])
    await event.edit("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'ent_(\d+)_(.+)_(\d+)')))
async def entity_update_callback(event):
    status = int(event.data_match.group(1).decode())
    etype = event.data_match.group(2).decode()
    eid = int(event.data_match.group(3).decode())
    
    # 如果状态是 1 (确认)，顺带更新其类型
    if status == 1:
        # 简单执行 SQL 更新类型
        db.cursor.execute('UPDATE entities SET type = ?, status = ? WHERE id = ?', (etype, status, eid))
        db.conn.commit()
        await event.answer(f"✅ 已确认实体: {etype}", alert=False)
    else:
        db.update_entity_status(eid, status)
        await event.answer("🚫 已屏蔽该词条", alert=False)
        
    await render_entity_review(event)

async def render_entity_list(event, etype, offset=0):
    """展示已确认的实体列表 (支持 creator/actor/tag)"""
    limit = 20
    db_type = 'keyword' if etype == 'tag' else etype
    entities = db.get_entities(status=1, entity_type=db_type, limit=limit, offset=offset)
    
    title_map = {"creator": "🏢 创作者与工作室", "actor": "👠 演员与女m", "tag": "🏷️ 核心关键词/Tag"}
    emoji_map = {"creator": "👤", "actor": "👠", "tag": "🏷️"}
    title = title_map.get(etype, '列表')
    emoji = emoji_map.get(etype, '📌')
    
    lines = [f"**{title}** (第 {offset//limit + 1} 页)\n━━━━━━━━━━━━━━"]
    
    if not entities:
        lines.append("\n📭 暂无已确认项目，请先运行'更新检索数据库'。")
        buttons = [[Button.inline("🔙 返回搜索中心", b"nav_search_center")]]
    else:
        buttons = []
        for e in entities:
             name = e['name']
             count = e.get('msg_count', 0)
             display = f"{emoji} {name} ({count})" if count else f"{emoji} {name}"
             buttons.append([Button.inline(display, f"do_search_{name}".encode())])
        
        nav_row = []
        if offset > 0: nav_row.append(Button.inline("⬅️ 上一页", f"sc_list_{etype}_{offset-limit}".encode()))
        if len(entities) == limit: nav_row.append(Button.inline("下一页 ➡️", f"sc_list_{etype}_{offset+limit}".encode()))
        if nav_row: buttons.append(nav_row)
        buttons.append([
        Button.inline("➕ 快速补充词条", f"sc_append_{etype}".encode()),
        Button.inline("🔙 返回搜索中心", b"nav_search_center")
    ])

    await (event.edit if isinstance(event, events.CallbackQuery.Event) else event.respond)("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'do_search_(.+)')))
async def do_search_callback(event):
    query = event.data_match.group(1).decode('utf-8')
    # 如果是带有类型前缀的搜索，例如 creator:窒物者
    stype = 'keyword'
    if ':' in query:
        stype, query = query.split(':', 1)
    await execute_advanced_search(event, query, search_type=stype)

async def render_alphabet_selector(event, etype):
    """渲染 A-Z 字母选择器"""
    label_map = {"creator": "創作者", "actor": "主要人物"}
    label = label_map.get(etype, "实体")
    
    lines = [f"🔍 **按{label}首字母检索**\n请选择字母："]
    buttons = []
    
    # 生成 A-Z 按钮，每行 6 个
    alphabet = string.ascii_uppercase
    row = []
    for char in alphabet:
        row.append(Button.inline(char, f"sc_alpha_{etype}_{char}".encode()))
        if len(row) == 6:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    
    # 增加 # 为非字母开头
    buttons.append([Button.inline("# 其他", f"sc_alpha_{etype}_#".encode())])
    buttons.append([Button.inline("🔙 返回搜索中心", b"nav_search_center")])
    
    await event.edit("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'sc_alpha_(creator|actor)_(.+)')))
async def sc_alpha_letter_callback(event):
    etype = event.data_match.group(1).decode()
    letter = event.data_match.group(2).decode()
    await render_entity_list_by_letter(event, etype, letter)

async def render_entity_list_by_letter(event, etype, letter):
    """点击字母后展示对应的实体列表"""
    # 优先从 entities.json 获取，保证即时性
    from pathlib import Path
    try:
        data_dir = CONFIG.get('currententities_dir_data', 'data/entities/tgporncopilot/currententities')
        entities_path = Path(data_dir) / 'entities.json'
        
        # 如果是相对路径，尝试基于项目根目录
        if not entities_path.is_absolute():
            entities_path = Path('.') / entities_path

        with open(entities_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        category_key = 'creators' if etype == 'creator' else 'actors'
        all_items = data.get(category_key, [])
        
        # 过滤
        def get_first_char(name):
            # 简单处理：取第一个英文字母或拼音？
            # 这里采取简单逻辑：如果是英文则英文字母，否则尝试拼音或跳过
            import pypinyin
            clean_n = name.strip().strip('#').strip('_')
            if not clean_n: return '#'
            first = clean_n[0].upper()
            if 'A' <= first <= 'Z':
                return first
            # 中文转拼音首字母
            p = pypinyin.pinyin(first, style=pypinyin.NORMAL)
            if p and p[0]:
                py_first = p[0][0][0].upper()
                if 'A' <= py_first <= 'Z':
                    return py_first
            return '#'

        filtered = []
        for item in all_items:
            # item 可能是 dict {'name': '...', 'aliases': []} 或 str
            name = item['name'] if isinstance(item, dict) else item
            if get_first_char(name) == letter:
                filtered.append(name)
        
        filtered.sort()
        
        label_map = {"creator": "創作者", "actor": "主要人物"}
        label = label_map.get(etype, "实体")
        
        lines = [f"📂 **{label} - {letter}**\n点击名称直接搜索："]
        buttons = []
        
        if not filtered:
            lines.append(f"\n抱歉，首字母为 `{letter}` 的{label}列表为空。")
        else:
            # 每行 2 个
            temp_row = []
            for name in filtered:
                # 按钮回调带类型前缀，方便 execute_advanced_search 识别
                temp_row.append(Button.inline(name, f"do_search_{etype}:{name}".encode()))
                if len(temp_row) == 2:
                    buttons.append(temp_row)
                    temp_row = []
            if temp_row: buttons.append(temp_row)
            
        buttons.append([Button.inline("⬅️ 返回字母表", f"sc_search_{etype}".encode())])
        await event.edit("\n".join(lines), buttons=buttons)
    except Exception as e:
        await event.respond(f"❌ 加载实体列表失败: {e}")

async def execute_advanced_search(event, query, search_type='keyword'):
    """执行深度检索并返回图文结果"""
    await event.answer(f"🔍 正在检索: {query}...", alert=False)
    results = db.search_with_sync_links(query)
    
    if not results:
        await event.respond(f"❌ 未找到与 `{query}` 相关的记录。")
        return

    msg = [f"🔍 **'{query}' 的检索结果 (最新30条)**\n"]
    
    for r in results:
        # r: (chat_name, msg_type, sender_name, original_time, text_content, forwarded_msg_id, forwarded_chat_id, msg_id, file_name)
        chat_name, mtype, sender, otime, text, fwd_id, cid, mid, fname = r
        icon = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️"}.get(mtype, "📝")
        
        # 尝试从文本或文件名中提取时长 [MM:SS]
        duration = ""
        combined_text = (text or "") + (fname or "")
        dur_match = re.search(r'\[(\d{1,2}:\d{2})\]', combined_text)
        if dur_match:
            duration = f"[{dur_match.group(1)}]"
        
        # 优化显示文本：优先用文本第一行，否则用文件名，否则用类型
        display_text = ""
        if text:
            first_line = text.split('\n')[0].strip().strip('#')
            # 移除已有的 [MM:SS] 避免重复
            first_line = re.sub(r'\[\d{1,2}:\d{2}\]', '', first_line).strip()
            display_text = first_line
        
        if not display_text and fname:
            display_text = re.sub(r'\[\d{1,2}:\d{2}\]', '', fname).strip()
            
        if not display_text:
            display_text = f"未命名资源 {mid}"

        # 限制长度
        if len(display_text) > 40:
            display_text = display_text[:37] + "..."

        if fwd_id:
            # [V2.0] 使用记录中的 forwarded_chat_id 构建链接
            d_id = str(cid or 0).replace('-100', '')
            link = f"https://t.me/c/{d_id}/{fwd_id}"
            msg.append(f"{icon}{duration} [{display_text}]({link})")
        else:
            msg.append(f"{icon}{duration} {display_text} *(仅备份)*")

    # 注入页脚：热搜与传送门
    hot_search = "热搜: 高中生 萝莉 精神小妹 强奸 小屁大王 巨乳 萝莉岛 cos "
    msg.append(f"\n`{hot_search}` ❝")

    final_results = "\n".join(msg)
    try:
        buttons = [[Button.inline("🔙 返回搜索中心", b"nav_search_center")]]
        if isinstance(event, events.CallbackQuery.Event):
            await event.edit(final_results, link_preview=False, buttons=buttons)
        else:
            await event.respond(final_results, link_preview=False, buttons=buttons)
    except Exception as e:
        print(f"⚠️ Search result send failed: {e}")
        # 兜底：如果 edit 失败，尝试发送新消息
        await event.respond(final_results, link_preview=False, buttons=buttons)



async def _force_cleanup_sorter():
    """强力清理系统中所有残留的 Sorter 进程 (僵尸进程回收)"""
    print("🔍 [P1.5] 正在扫描并清理系统残留的分拣服务进程...")
    try:
        if sys.platform == "win32":
            # Windows: 使用 wmic 查找命令行包含特定路径的 python 进程
            cmd = 'wmic process where "commandline like \'%tools/sorter/server.py%\'" get processid'
            proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            pids = re.findall(r'\d+', stdout.decode())
            my_pid = str(os.getpid())
            for pid in pids:
                if pid != my_pid:
                    print(f"🛑 [P1.5] 发现僵尸进程 PID {pid}，正在强力终止...")
                    os.system(f"taskkill /F /PID {pid} >nul 2>&1")
        else:
            # Linux/Mac: 暂不实现，结构对齐
            pass
    except Exception as e:
        print(f"⚠️ [P1.5] 清理残留进程时出现异常: {e}")

# ─── 共享工具：启动 P1.5 Server 并发送链接 ───
async def _launch_p15_and_notify(event_client, chat_id, target_msg_id, py_exec, view=''):
    global p15_process
    import socket
    port = int(os.getenv('SORTER_PORT', '8765'))
    
    # [FIX] 无论当前句柄是否存在，都进行全局扫描清理
    await _force_cleanup_sorter()
    
    if p15_process:
        try:
            p15_process.terminate()
            await p15_process.wait()
        except: pass
        
    server_log = os.path.join(CONFIG.get('root_path', '.'), f'data/logs/pipeline_server_{BOT_NAME}.log')
    os.makedirs(os.path.dirname(server_log), exist_ok=True)
    
    with open(server_log, "a", encoding="utf-8") as log_f:
        p15_process = await asyncio.create_subprocess_shell(
            f'"{py_exec}" tools/sorter/server.py --bot "{BOT_NAME}" --port {port} --no-browser',
            stdout=log_f,
            stderr=log_f
        )
    local_ips = []
    try:
        # 枚举所有网卡 IP，挑选真实局域网地址 (跳过 VPN/虚拟网卡)
        hostname = socket.gethostname()
        for addr in socket.getaddrinfo(hostname, None):
            ip = addr[4][0]
            # 只保留 192.168.x.x 或 10.x.x.x 的常见家庭/公司局域网段
            if ip.startswith("192.168.0.") or ip.startswith("192.168.1."):
                if ip not in local_ips:
                    local_ips.append(ip)
        # 如果没找到 .0. 或 .1. 网段，尝试其他 192.168.x.x
        if not local_ips:
            for addr in socket.getaddrinfo(hostname, None):
                ip = addr[4][0]
                if ip.startswith("192.168.") or ip.startswith("10."):
                    if ip not in local_ips:
                        local_ips.append(ip)
    except: pass
    await asyncio.sleep(5) # 增加启动稳定性
    if local_ips:
        ip_lines = f"📱 **手机访问：** `http://{local_ips[0]}:{port}`"
    else:
        ip_lines = "📱 **手机访问：** (未检测到有效局域网 IP)"
    
    suffix = f"?view={view}" if view else ""
    title = "✅ **词库管理工具已上线**" if view == 'manager' else "✅ **分拣服务已在线**"
    
    await event_client.edit_message(
        chat_id, target_msg_id,
        f"{title}\n\n"
        f"💻 **电脑访问：** `http://localhost:{port}{suffix}`\n"
        f"📱 **手机访问：** `http://{local_ips[0] if local_ips else 'localhost'}:{port}{suffix}`\n\n"
        "⚠️ **手机打不开？**\n"
        "1. 确认手机与电脑在**同一 WiFi**\n"
        "2. 暂时**关闭电脑 VPN** 或开启 **\"允许局域网流量/LAN Access\"**\n"
        f"3. 检查电脑**防火墙**是否拦截了 {port} 端口",
        buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]]
    )

async def execute_backup(event, mode, folder=None, ids=None, is_test=False, incremental=False):
    """处理备份任务执行"""
    if sync_job_lock.locked():
        await safe_answer(event, '⚠️ 任务冲突：当前已有同步/备份任务在运行中。', alert=True)
        return

    async with sync_job_lock:
        # [NEW] 为了避免 subprocess (backup.py) 出现 "database is locked" (session文件冲突)
        # 必须在启动子进程前释放 user_client 的连接
        try:
            if user_client.is_connected():
                print("🔌 Releasing User Client session for backup task...")
                await user_client.disconnect()
                await asyncio.sleep(2) # [FIX] 增加延迟确保 Windows 彻底释放 session 文件句柄
                print("✅ User Client session released.")
        except Exception as e:
            print(f"⚠️ Error releasing session: {e}")

        # 兼容性解析: 从 bk_1_full, bk_1_inc 或直接的 "1" 中提取纯数字 mode
        # 并在此处同步处理 incremental 标志
        parts = mode.split('_')
        if parts[0] == 'bk':
            exec_mode = parts[1]
            if len(parts) > 2 and parts[2] == 'inc':
                incremental = True
        else:
            exec_mode = mode.replace('bk_', '') # 回退兼容原有逻辑

        # 启动进度监控
        # [OPTIMIZATION] 如果是按钮触发，优先编辑原菜单
        if isinstance(event, events.CallbackQuery.Event):
            progress_msg = await event.edit("📊 **正在准备备份数据...**")
        else:
            progress_msg = await event.respond("📊 **正在准备备份数据...**")

        stop_btn = [Button.inline("🛑 停止备份", b"stop_backup")]
        progress_file = f'data/temp/backup_progress_{BOT_NAME}.json'
        stop_flag_file = f'data/temp/stop_backup_{BOT_NAME}.flag'

        # 清理上一次任务遗留的进度/停止文件，避免 UI 读到旧任务状态
        for stale_file in [progress_file, stop_flag_file, 'data/temp/backup_progress.json', 'data/temp/stop_backup.flag']:
            try:
                if os.path.exists(stale_file):
                    os.remove(stale_file)
            except Exception:
                pass

        # 获取当前任务的 Label
        run_id = db.start_backup_run(mode=mode, is_incremental=incremental, is_test=is_test)
        label = db.get_backup_label(run_id)
        
        try:
            cmd = f'python src/backup_mode/backup.py --mode {exec_mode} --run-id {run_id} --run-label "{label}"'
            if ids: cmd += f' --ids "{ids}"'
            if is_test: cmd += f' --test'
            if incremental: cmd += f' --incremental'

            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                encoding='utf-8'
            )

            # 监控循环
            last_report_time = 0
            has_shown_load = False
            
            while process.returncode is None:
                current_time = time.time()
                # 每 5 秒更新一次 UI [FIX: 从 1 秒改为 5 秒，避免 FloodWait]
                if current_time - last_report_time >= 5:
                    try:
                        if os.path.exists(progress_file):
                            with open(progress_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)

                            if not has_shown_load:
                                # ... existing load UI ...
                                load_text = (
                                    f"📋 **备份任务负载报告**\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"🆔 任务编号: `{data.get('label', 'N/A')}`\n"
                                    f"📂 待处理频道: `{data.get('total_channels', 0)}` 个\n"
                                    f"📧 总处理规模: `{data.get('total_raw_estimate', 0)}` 条原始记录\n"
                                    f"⏱️ 预计总耗时: `~{data.get('estimated_total_time_minutes', 0)}` 分钟 (基于历史测速 {data.get('hist_speed', 4000)}/min)\n"
                                    f"━━━━━━━━━━━━━━\n"
                                    f"⏳ 任务已启动，正在快扫描..."
                                )
                                await progress_msg.edit(load_text, buttons=None)
                                await asyncio.sleep(2) # 停留一会儿让用户看清负载报告
                                has_shown_load = True

                            # 进度计算使用稳定的原始数量
                            p_total = (data['current_raw_count'] / data['total_raw_estimate'] * 100) if data.get('total_raw_estimate', 0) > 0 else 0
                            p_channel = (data['current_channel_raw_count'] / data['current_channel_total_raw'] * 100) if data.get('current_channel_total_raw', 0) > 0 else 0

                            bar_total = "▓" * int(p_total/10) + "░" * (10 - int(p_total/10))
                            bar_chan = "█" * int(p_channel/10) + "▒" * (10 - int(p_channel/10))

                            est_min = data.get('estimated_total_time_minutes', 0)

                            text = (
                                f"⏳ **备份进行中...** (预计剩余 {est_min} 分钟)\n"
                                f"━━━━━━━━━━━━━━\n"
                                f"📦 **总进度**: `{bar_total}` {p_total:.1f}%\n"
                                f"📂 频道: `{data['completed_channels_count']}/{data['total_channels']}`\n"
                                f"📨 原始消息扫描: `{data['current_raw_count']}/{data['total_raw_estimate']}`\n"
                                f"📧 已保存提取组: `{data.get('total_groups_saved', 0)}` 组\n"
                                f"━━━━━━━━━━━━━━\n"
                                f"📍 **当前**: `{data['current_channel_name']}`\n"
                                f"📈 分频进度: `{bar_chan}` {p_channel:.1f}%\n"
                                f"✅ 当前频道捕获: `{data.get('current_channel_groups_saved', 0)}` 组"
                            )
                            # [FIX] 检测停止信号，如果已停止，立即提前跳出并移除按钮
                            if os.path.exists(stop_flag_file):
                                await safe_edit(progress_msg, text + "\n\n🛑 **正在停止中...**", buttons=None)
                                break

                            await safe_edit(progress_msg, text, buttons=stop_btn)
                            last_report_time = current_time
                    except Exception as e:
                        # 彻底忽略消息内容未变更或并发读写导致的异常
                        err_str = str(e).lower()
                        if 'not modified' not in err_str and 'identical' not in err_str and 'json' not in err_str:
                            print(f"⚠️ 进度更新出错: {e}")

                await asyncio.sleep(0.5)
                if process.returncode is not None: break

            # 获取剩余输出并检查状态
            stdout, stderr = await process.communicate()
            
            # [FIX] 解决 Race Condition：等待进程彻底退出并释放文件后，重试 3 次读取最后的汇总数据
            final_data = {}
            for attempt in range(3):
                try:
                    if os.path.exists(progress_file):
                        with open(progress_file, 'r', encoding='utf-8') as f:
                            final_data = json.load(f)
                            # 如果已经有了最终状态，则认为读取成功
                            if final_data.get('status') in ['completed', 'interrupted']:
                                break
                except: pass
                await asyncio.sleep(0.5)

            is_interrupted = os.path.exists(stop_flag_file) or final_data.get('status') == 'interrupted'
            
            if process.returncode == 0 or is_interrupted:
                final_status_text = "⚠️ **备份任务已手动停止**" if is_interrupted else "✅ **备份任务已完成！**"
                try:
                    await safe_edit(progress_msg, f"{final_status_text}\n正在梳理全局档案并锁定名称，请稍候...", buttons=None)
                except: pass
                
                # 执行 update_docs.py 进行全局元数据建档扫描
                py = sys.executable
                p3 = await asyncio.create_subprocess_shell(f'"{py}" src/sync_mode/update_docs.py', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, encoding='utf-8')
                await p3.communicate()
                
                # 发送汇总报告
                try:
                    state = "【手动停止】" if is_interrupted else "【任务完成】"
                    channels_list = final_data.get('channels', [])
                    skipped_banned = final_data.get('skipped_banned', []) or []
                    
                    completed = [c for c in channels_list if c.get('status') == 'completed']
                    interrupted = [c for c in channels_list if c.get('status') == 'interrupted']
                    skipped_banned = final_data.get('skipped_banned', []) or []
                    if not skipped_banned:
                        try:
                            all_folders_raw = await get_all_folder_peers()
                            banned_names = []
                            for folder_name, peers in all_folders_raw:
                                for p in peers:
                                    if p.get('is_syncable') and p.get('is_globally_banned'):
                                        banned_names.append(p.get('title', 'Unknown'))
                            skipped_banned = banned_names
                        except Exception:
                            skipped_banned = []

                    backfilled_channels = []
                    for ch in channels_list:
                        ch = dict(ch)
                        has_current_run_snapshot = bool(
                            ch.get('json_file') or ch.get('md_file') or ch.get('count', 0) or ch.get('raw_count', 0)
                        )
                        has_new_activity = bool(ch.get('new_count', 0) or ch.get('raw_new_count', 0))

                        # 规则：
                        # 1) 本轮有新增且已有当前快照时，优先保留本轮返回值，不用历史覆盖。
                        # 2) 仅当本轮缺少快照/统计，或本轮没有新增时，才进行历史回填。
                        should_backfill_history = (not has_current_run_snapshot) or (not has_new_activity)

                        if not should_backfill_history:
                            backfilled_channels.append(ch)
                            continue

                        # 优先尝试从最近有效的 backup_runs 中回填
                        try:
                            latest_stats = db.get_latest_backup_channel_stats(
                                ch.get('id') or ch.get('chat_id'),
                                bot_name=BOT_NAME,
                                is_test=False,
                            )
                        except Exception:
                            latest_stats = None

                        if latest_stats:
                            ch['count'] = latest_stats.get('count', ch.get('count', 0))
                            ch['raw_count'] = latest_stats.get('raw_count', ch.get('raw_count', 0))
                            ch['json_file'] = latest_stats.get('json_file', ch.get('json_file'))
                            ch['md_file'] = latest_stats.get('md_file', ch.get('md_file'))
                            ch['_from_history'] = True
                            ch['_history_source'] = 'backup_run'

                        # 兜底：若 backup_runs/JSON 给出的 raw_count 为空或明显小于 DB 中的历史记录，优先使用 DB 中的计数
                        try:
                            db_counts = db.get_channel_global_counts(ch.get('id') or ch.get('chat_id'))
                        except Exception:
                            db_counts = None

                        if db_counts and db_counts.get('raw_count', 0) > (ch.get('raw_count', 0) or 0):
                            # 覆盖为 DB 中更全面的计数
                            ch['raw_count'] = db_counts.get('raw_count', ch.get('raw_count', 0))
                            ch['count'] = db_counts.get('estimated_groups', ch.get('count', 0))
                            ch['_from_history'] = True
                            ch['_history_source'] = 'db'

                        backfilled_channels.append(ch)
                    channels_list = backfilled_channels

                    accessible_total = max(0, len(channels_list) - len(skipped_banned))
                    active_completed = [c for c in completed if (c.get('new_count', 0) or c.get('raw_new_count', 0))]

                    try:
                        global_raw_total = db.cursor.execute(
                            'SELECT COUNT(*) FROM global_messages'
                        ).fetchone()[0]
                    except Exception:
                        global_raw_total = final_data.get('total_messages', 0)
                    
                    chan_summary = f"📁 完成频道: `{len(active_completed)}/{accessible_total}`"
                    if interrupted:
                        chan_summary += f"\n⚠️ 中断频道: `{len(interrupted)}` (已保存部分数据)"
                    if skipped_banned:
                        chan_summary += f"\n🚫 全平台封禁: `{len(skipped_banned)}` (不计入分母)"

                    range_str = ""
                    if channels_list:
                        all_r = [c['ranges']['all'] for c in channels_list if c.get('ranges', {}).get('all') and c['ranges']['all'] != "-"]
                        if all_r: range_str = f"🔢 编号范围: `{all_r[-1].split('(')[0].strip()}` ~ `{all_r[0].split('(')[0].strip()}`"

                    # 口径约定：
                    # - new_count / saved_group_count: 本轮最终成功保存的增量组数
                    # - scanned_group_count: 本轮扫描命中的增量组数
                    # - count / raw_count: 当前完整快照总量
                    backed_up_groups = sum(c.get('saved_group_count', c.get('new_count', 0)) for c in channels_list)
                    scanned_groups = sum(c.get('scanned_group_count', c.get('new_count', 0)) for c in channels_list)
                    total_groups_all_channels = sum(c.get('count', 0) for c in channels_list)
                    
                    summary_report = [
                        f"📊 **备份任务报告** {state}",
                        f"━━━━━━━━━━━━━━",
                        f"🆔 任务编号: `{label}`",
                        f"📊 统计概览: `本轮备份 {backed_up_groups} 组 / 本轮扫描 {scanned_groups} 组 / 这些频道共 {total_groups_all_channels} 组` (相册已合并)",
                        f"🗃️ 本机器人全局累计原始消息: `{global_raw_total}` 条",
                        f"{chan_summary}",
                        f"{range_str}" if range_str else "",
                        f"⏰ 结束时间: `{datetime.now().strftime('%H:%M:%S')}`",
                        f"━━━━━━━━━━━━━━",
                        f"📍 **各频道明细 (成功保存增量组数 / 扫描到增量组数 / 频道总组数 | 原始消息数)**:"
                    ]
                    
                    for ch in channels_list:
                        status_char = "✅" if ch.get('status') == 'completed' else "🔸"
                        saved_group_count = ch.get('saved_group_count', ch.get('new_count', 0))
                        scanned_group_count = ch.get('scanned_group_count', ch.get('new_count', 0))
                        line = f"  {status_char} {ch.get('name', 'Unknown')}: `{saved_group_count} / {scanned_group_count} / {ch.get('count', 0)} | {ch.get('raw_count', 0)}`"
                        summary_report.append(line)

                    if skipped_banned:
                        summary_report.append("\n🚫 **被封禁频道** (不计入可访问分母):")
                        for name in skipped_banned:
                            hist_text = "如需确认历史是否已备份/同步，请在 metadata 或备份管理中按频道核对"
                            summary_report.append(f"  - {name}: `{hist_text}`")

                    summary_report.append(f"\n✨ 归档已更新至 `docs/archived/backups/`。")
                    
                    try:
                        await safe_edit(progress_msg, f"{final_status_text}\n━━━━━━━━━━━━━━\n详细总结已发送至您的私聊。", buttons=None)
                    except: pass
                    
                    target_user = event.sender_id
                    report_str = "\n".join(summary_report)
                    await bot.send_message(target_user, report_str)
                    
                    # [REMOVED] 备份完成后不再自动弹出菜单
                    await asyncio.sleep(1)
                    # await render_main_sync_menu(event, is_edit=False)
                    
                    await safe_edit(progress_msg, f"✅ **备份完成！**\n\n- 总条数: `{final_data.get('total_groups_saved', 0)}`\n- 存放路径: `docs/archived/backups/`\n\n正在自动同步元数据归档中...", buttons=None)
                    
                    # [NEW] 每次备份完成后，自动刷新元数据映射
                    await trigger_metadata_refresh(event)

                    # 发送给管理员 (如果不同)
                    if ADMIN_USER_ID and ADMIN_USER_ID != target_user:
                        await bot.send_message(ADMIN_USER_ID, report_str)
                except Exception as ex:
                    print(f"⚠️ 发送总结报告失败: {ex}")
            else:
                err_msg = stderr.decode().strip() or stdout.decode().strip()
                await safe_edit(progress_msg, f"❌ **备份失败 (Exit {process.returncode}):**\n```{err_msg[-1000:]}```", buttons=None)
        except Exception as e:
            await safe_edit(progress_msg, f'❌ 备份异常:\n{e}', buttons=None)

def _stdin_listener(loop):
    """后台线程：监听命令行输入，输入 q 触发优雅关机"""
    print("[提示] 输入 q 并回车可以优雅关闭机器人并发送下线通知。")
    while True:
        try:
            line = input()
            if line.strip().lower() == 'q':
                print("[关机] 正在发送下线通知并退出...")
                asyncio.run_coroutine_threadsafe(shutdown_handler(), loop)
                break
        except (EOFError, OSError):
            break

# ===== [NEW] 刷新元数据指令 =====
async def trigger_metadata_refresh(event, is_manual=False):
    """
    运行 update_docs.py，刷新本地 metadata md/json 档案
    """
    if is_manual:
        text = "🔄 **正在全量同步本地元数据档案...**\n\n- 正在同步文件夹结构\n- 正在探测频道封禁状态\n- 正在更新本地 MD/JSON 映射\n\n请稍候..."
        if isinstance(event, events.CallbackQuery.Event):
            msg = await event.edit(text)
        else:
            msg = await event.respond(text)
    
    try:
        # 0. 确保 User Client 在线
        if not user_client.is_connected():
            print("🌐 User Client disconnected. Attempting to reconnect...")
            await user_client.connect()
        
        # 如果依然没连上，或者未授权，尝试 start (这会处理 session 问题)
        if not await user_client.is_user_authorized():
            print("👤 User Client not authorized. Attempting to start...")
            await user_client.start()

        from sync_mode.update_docs import run_metadata_update
        # 直接调用函数，共享已有的 user_client 和 db，避免 session 锁定报错
        stats = await run_metadata_update(client=user_client, db_instance=db)
        
        if is_manual:
            report_msg = "✅ **元数据同步成功！**\n\n本地 `data/metadata` 与 `docs/metadata` 已全量对齐。"
            
            if stats:
                sections = []
                
                # 1. 文件夹变动
                moves = stats.get('moves', [])
                if moves:
                    move_lines = [f"• `{n}`: {f} ➔ {t}" for n, f, t in moves]
                    sections.append(f"📁 **文件夹位置变更 ({len(moves)})**:\n" + "\n".join(move_lines))
                
                # 2. 新增频道
                new_ch = stats.get('new_channels', [])
                if new_ch:
                    new_lines = [f"• `{n}` ➔ `[{f}]`" for n, f in new_ch]
                    sections.append(f"✨ **新增频道建档 ({len(new_ch)})**:\n" + "\n".join(new_lines))
                
                # 3. 智能清理
                del_count = stats.get('deleted_count', 0)
                pre_count = stats.get('preserved_count', 0)
                if del_count or pre_count:
                    clean_msg = f"🧹 **智能清理报告**:"
                    if del_count:
                        d_names = stats.get('deleted_names', [])
                        clean_msg += f"\n- 已清除失效本地档案: `{del_count}` 个 (名单: {', '.join(d_names)})"
                    if pre_count:
                        p_names = stats.get('preserved_names', [])
                        clean_msg += f"\n- **历史保护 (不予清理)**: `{pre_count}` 个 (名单: {', '.join(p_names)})\n  _(检测到本地存有 Backup/Logs 历史，已保留元数据)_"
                    sections.append(clean_msg)
                
                # 4. 状态变化 (封禁探测)
                status_changes = stats.get('status_changes', [])
                if status_changes:
                    status_lines = [f"• `{n}`: {o} ➔ {nw}" for n, o, nw in status_changes]
                    sections.append(f"🛑 **屏蔽/状态变化探测 ({len(status_changes)})**:\n" + "\n".join(status_lines))
                
                if sections:
                    report_msg += "\n\n" + "\n\n---\n\n".join(sections)
                else:
                    report_msg += "\n\n*(本次更新未检测到位置变动、新加频道或状态变更)*"
            
            # [OPTIMIZATION] 修改为编辑原消息，避免多出泡泡
            try:
                await msg.edit(report_msg)
            except:
                await event.respond(report_msg)
        else:
            print(f"✅ Automatic metadata refresh completed. Stats: moves={len(stats.get('moves', []))}, new={len(stats.get('new_channels', []))}, bans={len(stats.get('status_changes', []))}")
    except Exception as e:
        if is_manual:
            await msg.edit(f"❌ **同步过程中出现异常**:\n`{e}`")
        else:
            print(f"❌ Exception during automatic refresh: {e}")

@bot.on(events.NewMessage(pattern='/refresh'))
async def refresh_metadata_handler(event):
    if ADMIN_IDS and event.sender_id not in ADMIN_IDS:
        await event.respond('⚠️ 只有管理员可以执行此操作。')
        return
    await trigger_metadata_refresh(event, is_manual=True)

async def start_everything():
    print(f"🚀 Bot is initializing... (RunID: {RUN_ID}) [Ver: 2026-03-10 03:20]")
    try:
        # 1. 启动 Bot 客户端
        await bot.connect()
        await bot.start(bot_token=BOT_TOKEN)
        
        # 2. 启动 User 客户端 (用于读取文件夹)
        print("👤 Starting User Client...")
        await user_client.start()
        if not await user_client.is_user_authorized():
            print("❌ User Client 未授权！请先运行初始化登录脚本。")
            # 这里如果不授权，后续 get_folder_peers 会挂，所以最好提前终止
            if ADMIN_IDS:
                for uid in ADMIN_IDS:
                    try: await bot.send_message(uid, "❌ **启动失败**: User Client (用户号) 未授权，请在服务器中执行登录。")
                    except: pass
        

        await init_bot()
        print("🔗 Both clients are now connected and running event loop...")
        
        # 启动 stdin 监听线程
        import threading
        listener_thread = threading.Thread(
            target=_stdin_listener,
            args=(asyncio.get_event_loop(),),
            daemon=True
        )
        listener_thread.start()
        
        await bot.run_until_disconnected()
    except KeyboardInterrupt:
        await shutdown_handler()
    except Exception as e:
        import traceback
        print(f"❌ Bot Runtime Error: {e}")
        traceback.print_exc()
    finally:
        if bot and bot.is_connected():
            await bot.disconnect()

if __name__ == "__main__":
    if sys.platform == "win32":
        import msvcrt
        try:
            os.makedirs('data/temp', exist_ok=True)
            lock_file_path = os.path.abspath('data/temp/search_bot.lock')
            lock_fd = os.open(lock_file_path, os.O_CREAT | os.O_WRONLY)
            msvcrt.locking(lock_fd, msvcrt.LK_NBLCK, 1)
        except IOError:
            print("❌ 启动被拦截：检测到系统中已有一个 Bot 实例正在运行。为了保护数据库安全，已放弃重复启动。")
            sys.exit(1)
            
    try:
        asyncio.run(start_everything())
    except KeyboardInterrupt:
        pass # shutdown_handler 已经处理了
    except Exception as e:
        print(f"🚨 Fatal Launcher Error: {e}")
