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
from telethon import TelegramClient, events, Button, functions, connection
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
from db import Database

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
TARGET_GROUP_ID = int(os.getenv('TARGET_GROUP_ID'))
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID')
if ADMIN_USER_ID:
    try:
        ADMIN_USER_ID = int(ADMIN_USER_ID)
    except:
        print("⚠️ ADMIN_USER_ID 格式错误，请检查 .env")
        ADMIN_USER_ID = None

SESSION_NAME = 'data/sessions/copilot_user'

db = Database('data/copilot.db')
bot = TelegramClient('data/sessions/copilot_bot', API_ID, API_HASH, connection_retries=10, retry_delay=5)
user_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

sync_job_lock = asyncio.Lock()
# 存储用户选择: {chat_id: {folder_name: set(selected_ids)}}
user_selections = {}
# 用于捕捉特定用户的交互状态 (如: 正在等待输入回滚版本号)
user_states = {}
# 用于存储用户的全局测试环境标量 (True为测试，False为正式)
user_env = {}

# 机器人自身信息，用于识别 @提到
me = None

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
        if ADMIN_USER_ID:
            from telethon.tl.types import BotCommandScopePeer
            try:
                await bot(SetBotCommandsRequest(scope=BotCommandScopePeer(ADMIN_USER_ID), lang_code='', commands=commands))
            except: pass
        print("✅ Official Bot Commands set successfully.")
    except Exception as e:
        print(f"⚠️ Failed to set bot commands: {e}")

    # 启动问候 (改为私发给管理员)
    try:
        if ADMIN_USER_ID:
            await bot.send_message(ADMIN_USER_ID, f"🤖 **机器人已上线**\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⏰ 启动时间: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n💡 **小提示**：若您最近在 Telegram 手机端调整了频道分组、新增或删除了频道，建议发送 `/refresh` 立即刷新本地元数据档案与封禁状态。\n\n✅ 系统准备就绪，仅管理员可见。")
        else:
            print(f"🤖 Bot Online (RunID: {RUN_ID}) - No ADMIN_USER_ID configured to send DM.")
    except Exception as e:
        print(f"⚠️ 发送启动问候失败: {e}")

async def shutdown_handler():
    """优雅停机：发送下线通知并清理"""
    print(f"\n🛑 Shutting down (RunID: {RUN_ID})...")
    try:
        if ADMIN_USER_ID:
            # 尝试私发离线通知，限时 3 秒防挂起
            try:
                await asyncio.wait_for(bot.send_message(ADMIN_USER_ID, f"🛑 **机器人正在下线**\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⚠️ 该实例已停止服务。"), timeout=3.0)
            except:
                print("⚠️ 无法发送离线通知 (超时或已断开)")
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
        
    print("👋 Goodbye!")
    await asyncio.sleep(0.5) 
    os._exit(0) 

async def render_main_menu(event, is_edit=False):
    """渲染全局主菜单"""
    buttons = [
        [Button.inline("🔄 1. 同步管理 (转发/增量)", b"nav_sync_main")],
        [Button.inline("💾 2. 备份管理 (历史记录/全局)", b"nav_backup")],
        [Button.inline("🔍 3. 搜索中心 (快捷检索)", b"nav_search_center")],
        [Button.inline("📥 4. 手动补充信息 (Manual Append)", b"nav_mode_4_start")],
        [Button.inline("📊 5. 运行状态一览", b"nav_status_combined")],
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
            await event.respond(title, buttons=buttons)
            # 同时发送/更新持久化面板
            await event.respond("🕹️ 控制面板已激活", buttons=persistent_keyboard)
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
        [Button.inline("5. 高级回滚 (Rollback)", b"sync_5")],
        [Button.inline("6. 同步情况一览", b"sync_6")],
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
        await event.answer('⚠️ 当前正在进行任务，请稍候。', alert=True)
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
    if ADMIN_USER_ID and event.sender_id != ADMIN_USER_ID:
        await event.respond('⚠️ 只有管理员可以执行关闭操作。')
        return
        
    await event.respond('🛑 收到关闭指令，正在下线...')
    await shutdown_handler()

@bot.on(events.NewMessage(pattern='/ping'))
async def ping_test(event):
    await event.respond(f'💓 **Pong!**\n━━━━━━━━━━━━━━\n🆔 运行标识: `{RUN_ID}`\n⏰ 当前服务器时间: `{datetime.now().strftime("%H:%M:%S")}`\n我还在运行中，请指示。')

async def execute_sync(event, mode, folder=None, **kwargs):
    if sync_job_lock.locked():
        await event.answer('⚠️ 任务冲突：当前已有同步任务在运行中，请等其结束后再试。', alert=True)
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
            msg = await event.respond(f'🚀 正在启动同步任务 (模式 {mode})... 请稍候。\n\n💡 如果需要中途停止，请发送 `/stop` 或点击下方按钮。', 
                             buttons=[Button.inline('🛑 停止同步', data='stop_sync_instantly')])
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
                    # Remember to release lock!
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
                cmd = f'"{py}" src/sync_mode/sync.py --rollback "{is_rollback}" --confirm'
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
                        f'✅ 回滚操作已完成！\n指定的历史版本记录及本地关联物理文件均已被清空。\n\n⚠️ 注: Target 群组的历史转发已被远程撤销。',
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
    
    # Release the lock if it was acquired by the original execute_sync call but is now blocking the retry
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
    await render_search_center(event)

@bot.on(events.CallbackQuery(data=b'nav_status_combined'))
async def nav_status_combined_callback(event):
    await render_sync_status_ui(event)

@bot.on(events.CallbackQuery(data=b'nav_search'))
async def nav_search_callback(event):
    await event.respond('🔍 请直接发送 `/search <关键词>` 开始搜索。')
    await event.answer()

@bot.on(events.CallbackQuery(data=b'nav_backup'))
async def nav_backup_callback(event):
    await render_backup_menu(event, is_edit=True)

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
        # 同步情况一览
        await event.answer('正在加载同步情况...', alert=False)
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
            
        # [NEW] 获取全量对话列表作为“存活检测”的终极参考 (包括归档)
        active_dialogs = await user_client.get_dialogs()
        active_ids = {d.id for d in active_dialogs}

        filters_resp = await asyncio.wait_for(user_client(functions.messages.GetDialogFiltersRequest()), timeout=10.0)
        all_filters = getattr(filters_resp, 'filters', filters_resp) if not isinstance(filters_resp, list) else filters_resp
        result = []
        global_seen = set()
        for f in all_filters:
            title = getattr(f, 'title', None)
            t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            if not t_str or not hasattr(f, 'include_peers'):
                continue
            peers_info = []
            
            # 合并包含的和置顶的 Peers
            raw_peers = list(getattr(f, 'include_peers', [])) + list(getattr(f, 'pinned_peers', []))
            
            for peer in raw_peers:
                try:
                    # 1. 第一层过滤：如果 ID 根本不在 active_ids 里，说明已退出或已删，直接跳过
                    # Telethon 的 peer 可能有不同的 ID 获取方式
                    from telethon import utils
                    pid = utils.get_peer_id(peer)
                    if pid not in active_ids:
                        continue # 幽灵频道，跳过

                    # 2. 第二层：获取实体并记录
                    e = await user_client.get_entity(peer)
                    if e.id in global_seen:
                        continue
                    global_seen.add(e.id)
                    
                    tname = getattr(e, 'title', None) or getattr(e, 'first_name', str(e.id))
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
                    # 检查全平台封禁 vs 局部受限
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
                except Exception as get_e:
                    # 无法获取实体的多半也是幽灵
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
        for f in all_filters:
            title = getattr(f, 'title', None)
            t_str = (title.text if hasattr(title, 'text') else str(title)) if title else ""
            if t_str and hasattr(f, 'include_peers'):
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
    """渲染同步情况一览页面"""
    chat_id = event.chat_id
    is_test = user_env.get(chat_id, False)
    env_badge = "🧪测试" if is_test else "🚀正式"
    toggle_label = "切换为🚀正式" if is_test else "切换为🧪测试"
    
    lines = [f"📊 **同步情况一览 [{env_badge}]**\n"]
    
    try:
        all_folders = await get_all_folder_peers()
        if not all_folders:
            lines.append("❌ 未找到任何文件夹")
        else:
            for folder_name, peers in all_folders:
                syncable = [p for p in peers if p['is_syncable']]
                if not syncable:
                    continue
                lines.append(f"\n📁 **{folder_name}**")
                for p in syncable:
                    try:
                        info = db.get_latest_sync_info(int(p['id']), is_test=is_test)
                        if info and info.get('time'):
                            t = info['time'][:16].replace('T', ' ')
                            st = f"  {info['label']} · {t}"
                        else:
                            st = "  暂无同步记录"
                    except:
                        st = "  数据查询失败"
                    ban_badge = " [🚫 已封禁]" if p.get("is_globally_banned") else ""
                    lines.append(f"  {p['icon']} {p['title']}{ban_badge}{st}")
    except Exception as e:
        lines.append(f"❌ 加载失败: {e}")
    
    buttons = [
        [Button.inline("⬅️ 返回主菜单", b"sync_back"), Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    
    await event.edit("\n".join(lines), buttons=buttons)

async def render_backup_status_ui(event):
    """渲染备份情况一览页面"""
    lines = [f"📊 **备份情况一览 (树状图预览)**\n"]
    try:
        all_folders = await get_all_folder_peers()
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
                    
                    st = "  暂无备份记录"
                    if has_local_file:
                        try:
                            # 数据库中的 ID 现已标准化为 signed Peer ID
                            info = db.get_latest_backup_info(int(p['id']))
                            if info and info.get('time'):
                                t = info['time'][:16].replace('T', ' ')
                                st = f"  {info['label']} · {t}"
                        except: st = "  数据查询失败"
                    
                    ban_badge = " [🚫 已封禁]" if p.get("is_globally_banned") else ""
                    lines.append(f"  {p['icon']} {p['title']}{ban_badge}{st}")
        
        # [NEW] 历史频道虚拟分组：使用预缓存的 deleted_channels_map（O(1) 查找）
        # is_deleted 由 update_docs.py (refresh) 在检测到频道不可访问时自动写入
        historical_lines = []
        for mid_str, ch_info in deleted_channels_map.items():
            try:
                bk_info = db.get_latest_backup_info(int(mid_str))
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
    await event.edit("\n".join(lines), buttons=buttons)


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
    STOP_FLAG = 'data/temp/stop_backup.flag'
    try:
        with open(STOP_FLAG, 'w') as f:
            f.write('stop')
        await event.answer('🛑 正在发送停止信号，请稍候...', alert=True)
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
    await event.edit(title, buttons=buttons)

async def render_backup_manage_list(event):
    """提取出的渲染函数，支持多选状态"""
    chat_id = event.chat_id
    if chat_id not in user_states: user_states[chat_id] = {}
    selected = user_states[chat_id].get('selected_backups', set())
    
    runs = db.get_manageable_backup_runs(limit=20)
    if not runs:
        await event.edit("📭 暂无备份记录可管理", buttons=[Button.inline("⬅️ 返回", b"bk_manage")])
        return
    
    lines = ["🗑️ **管理备份记录 (多选模式)**\n"]
    
    buttons = []
    for r in runs:
        is_sel = r['run_id'] in selected
        icon = "✅ " if is_sel else ""
        
        # 模式解析
        m_name = "局部" if r['mode'] == '1' else ("全局" if r['mode'] == '2' else "旧版")
        t_name = "增量" if r['incremental'] else "全量"
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
            
        # 按钮文本
        btn_text = f"{icon}{r['label']} | {type_str} | {count_str}条 | {time_str} ({ch_summary})"
        buttons.append([Button.inline(btn_text, f"bkdel_toggle_{r['run_id']}".encode())])
        
        # 如果选中，在正文显示更多详情
        if is_sel:
            lines.append(f"🔹 **{r['label']}** ({m_name}{t_name})")
            lines.append(f"  ├ 统计: `本次新增 {new_c} 条 / 归档总计 {total_c} 条` (已合并相册)")
            lines.append(f"  └ 频道明细:")
            
            if r['channels']:
                for ch in r['channels']:
                    ch_n = ch.get('name', '未知频道')
                    ch_new = ch.get('new_count', 0)
                    ch_tot = ch.get('count', 0)
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
        all_runs = db.get_manageable_backup_runs(limit=100)
        total_files = 0
        for rid in list(selected):
            target = next((r for r in all_runs if r['run_id'] == rid), None)
            if target:
                db.delete_backup_run(rid)
                count = await perform_backup_physical_cleanup(run_time=target['time'], channels=target['channels'], label=target['label'])
                total_files += count
        
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
        db.clear_all_backup_runs()
        count = await perform_backup_physical_cleanup(all_clear=True)
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

    query = args[1]
    await execute_advanced_search(event, query)

@bot.on(events.NewMessage)
async def handle_all_messages(event):
    # 1. 强力过滤：跳过自己发的消息，防止自言自语
    if event.sender_id == me.id:
        return

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
            # 优先从转发元数据提取，如果是从私密群转发，通常能拿到 channel_id 和 id
            f_msg_id = event.fwd_from.channel_post or event.id
            # 有时 event.id 就是转发后的新 ID，我们以此为准查找 messages 表
            user_states.pop(event.chat_id, None)
            
            buttons = [
                [Button.inline("👁️ 查看现有信息", f"m4_view_{event.id}".encode())],
                [Button.inline("✍️ 追加/修改描述", f"m4_append_{event.id}".encode())],
                [Button.inline("⬅️ 返回", b"m4_back_0")]
            ]
            await event.respond(f"✅ **识别成功！**\n检测到消息 ID: `{event.id}`\n\n请选择后续操作：", buttons=buttons)
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
    elif state and state.startswith('awaiting_m4_text_'):
        # 模式 4：持久化追加文本
        parts = state.split('_')
        o_chat_id = int(parts[3])
        o_msg_id = int(parts[4])
        
        append_text = event.text.strip()
        if append_text:
            # 获取原内容
            row = db.cursor.execute('SELECT text_content FROM global_messages WHERE chat_id = ? AND msg_id = ?', (o_chat_id, o_msg_id)).fetchone()
            current = row[0] if row and row[0] else ""
            new_content = (current + "\n" + append_text).strip()
            
            db.cursor.execute('UPDATE global_messages SET text_content = ?, is_extracted = 0 WHERE chat_id = ? AND msg_id = ?', (new_content, o_chat_id, o_msg_id))
            db.conn.commit()
            
            # 提示成功，并保持监听状态以便继续追加 (除非用户点取消或发送完毕)
            await event.respond(f"✅ 已成功追加！目前内容长度: {len(new_content)} 字。\n您可以继续发送文本追加，或点击“结束录入”。", buttons=[Button.inline("结束录入", b"nav_main")])
        return

    # 6. 响应单纯的 "/" 或 问候语 (非命令模式下的交互)
    if text == '/' or any(greet in text for greet in ['你好', 'hi', 'hello', 'hey']):
        await show_help_message(event)
    elif text == '🔄 刷新归档':
        if ADMIN_USER_ID and event.sender_id != ADMIN_USER_ID:
            await event.respond('⚠️ 只有管理员可以执行此操作。')
            return
        await trigger_metadata_refresh(event, is_manual=True)
    else:
        # 只有在私聊或者被 @ 的情况下，才会回复“不明白”
        await event.respond('抱歉，我不明白你的意思。请输入 `/help` 查看命令列表，或者发送 `/sync` 开始同步资源。')

# ===== 其他全局功能回调 =====

@bot.on(events.CallbackQuery(data=b'nav_refresh_metadata'))
async def callback_refresh_metadata(event):
    if ADMIN_USER_ID and event.sender_id != ADMIN_USER_ID:
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
    
    for i, r in enumerate(rows):
        # 匹配 SQL: g.chat_name, g.msg_type, g.sender_name, g.original_time, g.text_content, m.forwarded_msg_id, g.chat_id, g.msg_id, g.search_tags
        chat_name, msg_type, sender, o_time, text, f_msg_id, o_chat_id, o_msg_id, tags = r
        
        # 截取文本摘要
        summary = text[:50].replace('\n', ' ') if text else "无描述文案"
        time_str = o_time[:10] if o_time else "未知时间"
        
        icon = "🎬" if msg_type == 'video' else "🖼️" if msg_type == 'photo' else "📎"
        
        # 构建链接
        if f_msg_id:
            link = f"https://t.me/c/{clean_group_id}/{f_msg_id}"
            line = f"{i+1}. {icon} [{summary}]({link})\n   └ 📅 `{time_str}` | 👤 `{sender or chat_name}`"
        else:
            # 如果没同步过，仅展示本地备份存在
            line = f"{i+1}. {icon} {summary} (仅备份)\n   └ 📅 `{time_str}` | 👤 `{sender or chat_name}`"
            
        if tags:
            line += f" | 🏷️ `{tags}`"
            
        lines.append(line)
        
        if i >= 14: # 最多展示 15 条
            lines.append("... (更多结果请缩小搜索范围)")
            break

    await event.respond("\n".join(lines), link_preview=False)

# ===== Mode 3: 检索与分析中心 =====

@bot.on(events.CallbackQuery(data=b'nav_search_center'))
async def nav_search_callback(event):
    await render_search_center(event, is_edit=True)

async def render_search_center(event, is_edit=False):
    """渲染检索分析中心主界面 (Mode 3)"""
    buttons = [
        [Button.inline("🔄 1. 更新检索数据库", b"sc_update_db")],
        [Button.inline("👤 2. 找创作者 (Creator)", b"sc_search_creator")],
        [Button.inline("💃 3. 找模特 (Actor)", b"sc_search_actor")],
        [Button.inline("🏷️ 4. 关键字搜索 (Keyword)", b"sc_search_keyword")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")],
        [Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    
    title = (
        "🔍 **情报检索与分析中心 (Mode 3)**\n\n"
        "请选择搜索方式。系统将优先返回具备消息直达链接 (Deep Link) 的结果。\n"
        "━━━━━━━━━━━━━━\n"
        "💡 **提示**：关键字搜索支持模糊匹配文件名、描述以及您手动补充的信息。"
    )
    try:
        if is_edit:
            await event.edit(title, buttons=buttons)
        else:
            await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_search_center error: {e}")

@bot.on(events.CallbackQuery(data=re.compile(br'sc_(.+)')))
async def search_center_callback(event):
    cmd = event.data_match.group(1).decode('utf-8')
    chat_id = event.chat_id
    
    if cmd == 'update_db':
        await event.answer("正在增量索引数据库，请稍候...", alert=False)
        # 执行 apply 脚本
        import subprocess
        try:
            # 使用 sys.executable 确保使用当前 Python 环境
            process = subprocess.Popen([sys.executable, 'src/search_mode/update_search_db.py', '--apply'], 
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()
            if process.returncode == 0:
                await event.respond(f"✅ **数据库索引更新成功！**\n━━━━━━━━━━━━━━\n{stdout.strip()}")
            else:
                await event.respond(f"❌ **索引更新失败**\n错误详情: {stderr}")
        except Exception as e:
            await event.respond(f"⚠️ 执行脚本出错: {e}")
        await render_search_center(event, is_edit=True)
        
    elif cmd == 'search_creator':
        user_states[chat_id] = 'awaiting_search_creator'
        await event.respond("👤 **查找创作者**\n\n请输入您想查找的创作者或工作室名字：", buttons=[Button.inline("取消", b"nav_search_center")])
        await event.answer()
        
    elif cmd == 'search_actor':
        user_states[chat_id] = 'awaiting_search_actor'
        await event.respond("💃 **查找模特/演员**\n\n请输入您想查找的模特或演员名字：", buttons=[Button.inline("取消", b"nav_search_center")])
        await event.answer()
        
    elif cmd == 'search_keyword':
        user_states[chat_id] = 'awaiting_search_keyword'
        await event.respond("🏷️ **关键字搜索**\n\n请输入搜索词（支持匹配文件名、标签或补充描述）：", buttons=[Button.inline("取消", b"nav_search_center")])
        await event.answer()

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

@bot.on(events.CallbackQuery(data=re.compile(br'm4_(view|append|back)_(\d+)')))
async def mode_4_action_callback(event):
    action = event.data_match.group(1).decode('utf-8')
    f_msg_id = int(event.data_match.group(2).decode('utf-8'))
    chat_id = event.chat_id
    
    if action == 'back':
        await mode_4_start_callback(event)
        return
        
    # 根据 f_msg_id 寻找原始消息
    row = db.cursor.execute('SELECT original_chat_id, original_msg_id FROM messages WHERE forwarded_msg_id = ?', (f_msg_id,)).fetchone()
    if not row:
        await event.answer("❌ 找不到该消息的同步记录，无法补充信息。", alert=True)
        return
    o_chat_id, o_msg_id = row

    if action == 'view':
        msg = db.cursor.execute('SELECT text_content, search_tags FROM global_messages WHERE chat_id = ? AND msg_id = ?', (o_chat_id, o_msg_id)).fetchone()
        if msg:
            content = msg[0] or "空"
            tags = msg[1] or "无"
            await event.respond(f"👁️ **当前信息预览 (f_id: {f_msg_id})**\n━━━━━━━━━━━━━━\n📝 描述内容:\n`{content}`\n\n🏷️ 搜索标签:\n`{tags}`")
        await event.answer()
        
    elif action == 'append':
        user_states[chat_id] = f'awaiting_m4_text_{o_chat_id}_{o_msg_id}'
        await event.respond(f"✍️ **正在为消息 #{f_msg_id} 追加信息**\n\n请直接发送您想补充的文本（如模特名、系列名或长描述）。\n系统将自动将其追加到现有文案末尾并更新索引。", buttons=[Button.inline("结束录入", b"nav_main")])
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
    """展示已确认的实体列表"""
    limit = 20
    entities = db.get_entities(status=1, entity_type=etype, limit=limit, offset=offset)
    
    title_map = {"creator": "🏢 创作者与工作室", "actor": "💃 演员与角色"}
    lines = [f"**{title_map.get(etype, '列表')}** (第 {offset//limit + 1} 页)\n"]
    
    if not entities:
        lines.append("📭 暂无已确认实体，请先去审核中心确认。")
        buttons = [[Button.inline("⬅️ 返回", b"nav_search")]]
    else:
        buttons = []
        for e in entities:
             # 点击即触发搜索
             buttons.append([Button.inline(f"{e['name']} ({e['msg_count']})", f"do_search_{e['name']}".encode())])
        
        # 翻页按钮
        nav_row = []
        if offset > 0:
            nav_row.append(Button.inline("⬅️ 上一页", f"sc_list_{etype}_{offset-limit}".encode()))
        if len(entities) == limit:
            nav_row.append(Button.inline("下一页 ➡️", f"sc_list_{etype}_{offset+limit}".encode()))
        if nav_row: buttons.append(nav_row)
        buttons.append([Button.inline("⬅️ 返回检索中心", b"nav_search")])

    await event.edit("\n".join(lines), buttons=buttons)

@bot.on(events.CallbackQuery(data=re.compile(br'do_search_(.+)')))
async def do_search_callback(event):
    query = event.data_match.group(1).decode('utf-8')
    await execute_advanced_search(event, query)

async def execute_advanced_search(event, query):
    """执行深度检索并返回图文结果"""
    await event.answer(f"🔍 正在检索: {query}...", alert=False)
    results = db.search_with_sync_links(query)
    
    if not results:
        await event.respond(f"❌ 未找到与 `{query}` 相关的记录。")
        return

    msg = [f"🔍 **'{query}' 的检索结果 (最新30条)**\n"]
    for r in results:
        # r: (chat_name, msg_type, sender_name, original_time, text_content, forwarded_msg_id, chat_id, msg_id)
        chat_name, mtype, sender, otime, text, fwd_id, cid, mid = r
        icon = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️"}.get(mtype, "📝")
        
        # 截断正文
        safe_text = (text[:60] + "...") if len(text) > 60 else text
        safe_text = safe_text.replace('\n', ' ')
        
        time_short = otime[5:16]
        
        if fwd_id:
            # 获取私密群组 ID (从环境变量或数据库)
            # 这里由于私密群 ID 可能变化，最稳妥是跳转原群 (如果是公开的) 或私有链接
            # 用户需求是“直接资源链接”，即跳转到保存到的群。
            # 这里假设用户已经配置了 DESTINATION_CHANNEL
            dest_id = os.getenv('DESTINATION_CHANNEL')
            if dest_id:
                # 处理 ID 格式
                d_id = str(dest_id).replace('-100', '')
                link = f"https://t.me/c/{d_id}/{fwd_id}"
                msg.append(f"{icon} **[{time_short}]** [{safe_text}]({link})")
            else:
                msg.append(f"{icon} **[{time_short}]** {safe_text} (已同步)")
        else:
            msg.append(f"{icon} **[{time_short}]** {safe_text} *(仅备份)*")

    # 如果消息太长，分页发送
    await event.respond("\n".join(msg), link_preview=False)


async def execute_backup(event, mode, folder=None, ids=None, is_test=False, incremental=False):
    """处理备份任务执行"""
    if sync_job_lock.locked():
        await event.answer('⚠️ 任务冲突：当前已有同步/备份任务在运行中。', alert=True)
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
        progress_msg = await event.respond("📊 **正在准备备份数据...**")

        stop_btn = [Button.inline("🛑 停止备份", b"stop_backup")]

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
                stderr=asyncio.subprocess.PIPE
            )

            # 监控循环
            last_report_time = 0
            has_shown_load = False
            progress_file = 'data/temp/backup_progress.json'
            
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
                            await progress_msg.edit(text, buttons=stop_btn)
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

            is_interrupted = os.path.exists('data/temp/stop_backup.flag') or final_data.get('status') == 'interrupted'
            
            if process.returncode == 0 or is_interrupted:
                final_status_text = "⚠️ **备份任务已手动停止**" if is_interrupted else "✅ **备份任务已完成！**"
                try:
                    await progress_msg.edit(f"{final_status_text}\n正在梳理全局档案并锁定名称，请稍候...")
                except: pass
                
                # 执行 update_docs.py 进行全局元数据建档扫描
                py = sys.executable
                p3 = await asyncio.create_subprocess_shell(f'"{py}" src/sync_mode/update_docs.py', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                await p3.communicate()
                
                # 发送汇总报告
                try:
                    state = "【手动停止】" if is_interrupted else "【任务完成】"
                    channels_list = final_data.get('channels', [])
                    
                    completed = [c for c in channels_list if c.get('status') == 'completed']
                    interrupted = [c for c in channels_list if c.get('status') == 'interrupted']
                    
                    chan_summary = f"📁 完成频道: `{len(completed)}/{final_data.get('total_channels', 0)}`"
                    if interrupted:
                        chan_summary += f"\n⚠️ 中断频道: `{len(interrupted)}` (已保存部分数据)"

                    range_str = ""
                    if channels_list:
                        all_r = [c['ranges']['all'] for c in channels_list if c.get('ranges', {}).get('all') and c['ranges']['all'] != "-"]
                        if all_r: range_str = f"🔢 编号范围: `{all_r[-1].split('(')[0].strip()}` ~ `{all_r[0].split('(')[0].strip()}`"

                    new_m = final_data.get('new_messages', 0)
                    total_m = final_data.get('total_messages', 0)
                    
                    summary_report = [
                        f"📊 **备份任务报告** {state}",
                        f"━━━━━━━━━━━━━━",
                        f"🆔 任务编号: `{label}`",
                        f"📊 统计概览: `本次新增 {new_m} 条 / 归档总计 {total_m} 条` (已合并相册)",
                        f"{chan_summary}",
                        f"{range_str}" if range_str else "",
                        f"⏰ 结束时间: `{datetime.now().strftime('%H:%M:%S')}`",
                        f"━━━━━━━━━━━━━━",
                        f"📍 **各频道明细 (+新增 / 存档总计)**:"
                    ]
                    
                    for ch in channels_list:
                        status_char = "✅" if ch.get('status') == 'completed' else "🔸"
                        summary_report.append(f"  {status_char} {ch.get('name', 'Unknown')}: `+{ch.get('new_count', 0)} / {ch.get('count', 0)}`")

                    summary_report.append(f"\n✨ 归档已更新至 `docs/archived/backups/`。")
                    
                    try:
                        await progress_msg.edit(f"{final_status_text}\n━━━━━━━━━━━━━━\n详细总结已发送至您的私聊。")
                    except: pass
                    
                    target_user = event.sender_id
                    await bot.send_message(target_user, "\n".join(summary_report))
                    
                    # [REMOVED] 备份完成后不再自动弹出菜单
                    await asyncio.sleep(1)
                    # await render_main_sync_menu(event, is_edit=False)
                    
                    await progress_msg.edit(f"✅ **备份完成！**\n\n- 总条数: `{final_data.get('total_groups_saved', 0)}`\n- 存放路径: `docs/archived/backups/`\n\n正在自动同步元数据归档中...")
                    
                    # [NEW] 每次备份完成后，自动刷新元数据映射
                    await trigger_metadata_refresh(event)

                    # 发送给管理员 (如果不同)
                    if ADMIN_USER_ID and ADMIN_USER_ID != target_user:
                        await bot.send_message(ADMIN_USER_ID, summary_report)
                except Exception as ex:
                    print(f"⚠️ 发送总结报告失败: {ex}")
            else:
                err_msg = stderr.decode().strip() or stdout.decode().strip()
                await progress_msg.edit(f"❌ **备份失败 (Exit {process.code if hasattr(process, 'code') else process.returncode}):**\n```{err_msg[-1000:]}```")
        except Exception as e:
            await progress_msg.edit(f'❌ 备份异常:\n{e}')

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
        msg = await event.respond("🔄 **正在全量同步本地元数据档案...**\n\n- 正在同步文件夹结构\n- 正在探测频道封禁状态\n- 正在更新本地 MD/JSON 映射\n\n请稍候...")
    
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
            
            await msg.edit(report_msg)
        else:
            print(f"✅ Automatic metadata refresh completed. Stats: moves={len(stats.get('moves', []))}, new={len(stats.get('new_channels', []))}, bans={len(stats.get('status_changes', []))}")
    except Exception as e:
        if is_manual:
            await msg.edit(f"❌ **同步过程中出现异常**:\n`{e}`")
        else:
            print(f"❌ Exception during automatic refresh: {e}")

@bot.on(events.NewMessage(pattern='/refresh'))
async def refresh_metadata_handler(event):
    if ADMIN_USER_ID and event.sender_id != ADMIN_USER_ID:
        await event.respond('⚠️ 只有管理员可以执行此操作。')
        return
    await trigger_metadata_refresh(event, is_manual=True)

async def start_everything():
    print(f"🚀 Bot is initializing... (RunID: {RUN_ID})")
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
            if ADMIN_USER_ID:
                await bot.send_message(ADMIN_USER_ID, "❌ **启动失败**: User Client (用户号) 未授权，请在服务器中执行登录。")
        
        # 监听所有消息的简易心跳日志
        @bot.on(events.NewMessage)
        async def heartbeat(event):
            sender_info = event.sender_id
            print(f"💓 [Heartbeat] Received message from {sender_info}: {event.text[:30] if event.text else '<No Text>'}")

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
