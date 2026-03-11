
import os
import re

file_path = 'src/search_bot.py'

def fix_file():
    # 强制以 UTF-8 读取（或者如果损坏，尝试恢复）
    content = None
    for enc in ['utf-8', 'gbk', 'latin-1']:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                content = f.read()
                print(f"Loaded with {enc}")
                break
        except:
            continue
    
    if not content:
        print("Failed to load file")
        return

    # 定义 render_search_center 的完美版本 (基于用户需求和截图)
    # 注意：使用用户 screenshot 的风格
    new_render = """async def render_search_center(event, is_edit=False):
    \"\"\"渲染检索与分析中心主界面 (Mode 3)\"\"\"
    title = (
        "🔍 **情报检索与分析中心 (Mode 3)**\\n\\n"
        "请选择搜索方式。系统将优先返回具备消息直达链接 (Deep Link) 的结果。\\n"
        "━━━━━━━━━━━━━━\\n"
        "💡 **提示**：全局自由搜索支持模糊匹配文件名、描述以及您手动补充的信息。"
    )
    buttons = [
        [Button.inline("🔄 1. 更新检索数据库", b"sc_update_db")],
        [Button.inline("👤 2. 找创作者 (Creator)", b"sc_search_creator")],
        [Button.inline("💃 3. 找女m/模特 (Actor)", b"sc_search_actor")],
        [Button.inline("🏷️ 4. 找关键词 (Tag)", b"sc_search_tag")],
        [Button.inline("🔍 5. 全局自由搜索", b"sc_search_keyword")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main")],
        [Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    try:
        if is_edit or isinstance(event, events.CallbackQuery.Event):
            await event.edit(title, buttons=buttons)
        else:
            await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_search_center error: {e}")"""

    # 定义 search_center_callback 的完美版本
    new_callback_logic = """@bot.on(events.CallbackQuery(data=re.compile(br'sc_(.+)')))
async def search_center_callback(event):
    cmd = event.data_match.group(1).decode('utf-8')
    chat_id = event.chat_id
    
    if cmd == 'update_db':
        await event.answer("正在启动更新流水线...", alert=False)
        await event.edit(
            "🔄 **正在执行全自动更新流程**\\n\\n"
            "步骤：\\n1️⃣ 合并备份 (P0)\\n2️⃣ 聚类分析 (P1)\\n3️⃣ 启动分拣 UI (P1.5)\\n\\n⏳ 请稍候...",
            buttons=None
        )
        async def run_pipeline():
            py = sys.executable
            msg = event.message
            try:
                # 运行提取脚本 (P0/P1)
                proc = await asyncio.create_subprocess_shell(
                    f'"{py}" src/search_mode/program1_discovery/entity_extractor.py --bot "{BOT_NAME}"'
                )
                await proc.communicate()
            except: pass
            
            # 启动分拣服务 (P1.5)
            port = 8765
            try:
                await asyncio.create_subprocess_shell(f'"{py}" tools/sorter/server.py --bot "{BOT_NAME}" --port {port} --no-browser')
                await asyncio.sleep(2)
                await msg.edit(
                    f"✅ **提取完成！分拣服务已运行**\\n\\n"
                    f"🌐 **请在此访问：** `http://localhost:{port}`\\n\\n"
                    f"在网页分拣提交后，系统将自动完成打标并更新数据库。",
                    buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]]
                )
            except Exception as e:
                await msg.edit(f"❌ 启动分拣服务失败: {e}", buttons=[[Button.inline("🔙 返回", b"nav_search_center")]])

        asyncio.create_task(run_pipeline())
        
    elif cmd == 'search_creator':
        await render_entity_list(event, 'creator', offset=0)
        
    elif cmd == 'search_actor':
        await render_entity_list(event, 'actor', offset=0)
        
    elif cmd == 'search_tag':
        await render_entity_list(event, 'tag', offset=0)
        
    elif cmd == 'search_keyword':
        user_states[chat_id] = 'awaiting_search_keyword'
        await event.edit("🔍 **全局自由搜索**\\n\\n请输入任意内容进行全文模糊检索：", 
                         buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]])
    
    elif cmd.startswith('list_'):
        try:
            _, etype, offset = cmd.split('_')
            await render_entity_list(event, etype, int(offset))
        except: pass"""

    # 替换 render_search_center
    content = re.sub(r'async def render_search_center\(event, is_edit=False\):.*?print\(f"⚠️ render_search_center error: {e}"\)', 
                     new_render, content, flags=re.DOTALL)
    
    # 替换 search_center_callback (尝试用更宽泛的正则匹配之前被乱码破坏的部分)
    content = re.sub(r'@bot\.on\(events\.CallbackQuery\(data=re\.compile\(br\'sc_\(\.\+\)\'\)\).*?await render_search_center\(event, is_edit=True\)', 
                     new_callback_logic, content, flags=re.DOTALL)
    # 如果没匹配到，试试匹配乱码版本
    content = re.sub(r'@bot\.on\(events\.CallbackQuery\(data=re\.compile\(br\'sc_\(\.\+\)\'\)\).*?elif cmd\.startswith\(\'list_\'\):.*?except: pass', 
                     new_callback_logic, content, flags=re.DOTALL)

    # 确保 handle_search_text 处理 keyword
    if 'awaiting_search_keyword' not in content:
        keyword_logic = """
    if state == 'awaiting_search_keyword':
        user_states.pop(chat_id, None)
        await execute_advanced_search(event, event.text.strip())
        return
"""
        content = re.sub(r'async def handle_search_text\(event\):.*?(chat_id = event\.chat_id.*?state = user_states\.get\(chat_id, \'\'\))', 
                         r'async def handle_search_text(event):\n\1' + keyword_logic, content, flags=re.DOTALL)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

fix_file()
print("Fixed.")
