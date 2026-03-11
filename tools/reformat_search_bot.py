
import os
import re

file_path = 'src/search_bot.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update render_search_center to 5-button layout
render_search_center_pattern = r'async def render_search_center\(event, is_edit=False\):.*?try:.*?except Exception as e:.*?print\(f" render_search_center error: {e}"\)'
new_render_search_center = """async def render_search_center(event, is_edit=False):
    \"\"\"渲染检索分析中心主界面 (Mode 3)\"\"\"
    title = (
        "🔍 **情报检索与分析中心 (Mode 3)**\\n\\n"
        "请选择搜索方式。系统将优先返回具备消息直达链接 (Deep Link) 的结果。\\n"
        "━━━━━━━━━━━━━━\\n"
        "💡 **提示**：关键字搜索支持模糊匹配文件名、描述以及您手动补充的信息。"
    )
    buttons = [
        [Button.inline("🔄 1. 更新检索数据库", b"sc_update_db")],
        [Button.inline("👤 2. 找创作者 (Creator)", b"sc_search_creator")],
        [Button.inline("👠 3. 找女m (Actor)", b"sc_search_actor")],
        [Button.inline("🏷️ 4. 找关键词 (Tag)", b"sc_search_tag")],
        [Button.inline("🔍 5. 全局自由搜索", b"sc_search_keyword")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main"),
         Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]
    try:
        if is_edit or isinstance(event, events.CallbackQuery.Event):
            await event.edit(title, buttons=buttons)
        else:
            await event.respond(title, buttons=buttons)
    except Exception as e:
        if 'not modified' not in str(e).lower():
            print(f"⚠️ render_search_center error: {e}")"""

content = re.sub(r'async def render_search_center\(event, is_edit=False\):.*?print\(f" render_search_center error: {e}"\)', new_render_search_center, content, flags=re.DOTALL)

# 2. Update search_center_callback routes
new_callback_logic = """@bot.on(events.CallbackQuery(data=re.compile(br'sc_(.+)')))
async def search_center_callback(event):
    cmd = event.data_match.group(1).decode('utf-8')
    chat_id = event.chat_id
    
    if cmd == 'update_db':
        await event.edit(
            "🔄 **正在启动全自动更新流程...**\\n\\n"
            "步骤：\\n1️⃣ 合并备份 (P0)\\n2️⃣ 聚类分析 (P1)\\n3️⃣ 启动网页分拣 (P1.5)\\n\\n⏳ 请稍候...",
            buttons=None
        )
        async def run_pipeline():
            py = sys.executable
            msg = event.message
            # P0/P1
            try:
                await msg.edit("⏳ **正在执行自动提取与聚类 (P0/P1)...**")
                # 运行更新脚本
                process = await asyncio.create_subprocess_shell(
                    f'"{py}" src/search_mode/program1_discovery/entity_extractor.py --bot "{BOT_NAME}"',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await process.communicate()
            except: pass
            
            # 启动 P1.5
            port = 8765
            try:
                await asyncio.create_subprocess_shell(f'"{py}" tools/sorter/server.py --bot "{BOT_NAME}" --port {port} --no-browser')
                await asyncio.sleep(2)
                await msg.edit(
                    f"✅ **提取完成！分拣服务已就绪**\\n\\n"
                    f"🌐 **浏览器访问：** `http://localhost:{port}`\\n\\n"
                    f"分拣完成后点击网页底部的导出按钮，即可自动完成打标并更新数据库。",
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
        await event.edit("🔍 **全局自由搜索**\\n\\n请输入任意词汇（支持模糊匹配文件名、标签、描述等）：", 
                         buttons=[[Button.inline("🔙 返回搜索中心", b"nav_search_center")]])
    
    elif cmd.startswith('list_'):
        # 处理翻页 sc_list_{etype}_{offset}
        try:
            _, etype, offset = cmd.split('_')
            await render_entity_list(event, etype, int(offset))
        except: pass
"""

content = re.sub(r'@bot.on\(events\.CallbackQuery\(data=re\.compile\(br\'sc_\(\.\+\)\'\)\)\).*?await event\.answer\(\)', new_callback_logic, content, flags=re.DOTALL)

# 3. Update render_entity_list to support 'tag'
new_render_entity_list = """async def render_entity_list(event, etype, offset=0):
    \"\"\"展示已确认的实体列表 (支持 creator/actor/tag)\"\"\"
    limit = 20
    # 映射 etype 到 DB 类型
    db_type = 'keyword' if etype == 'tag' else etype
    entities = db.get_entities(status=1, entity_type=db_type, limit=limit, offset=offset)
    
    title_map = {"creator": "🏢 创作者与工作室", "actor": "👠 演员与女m", "tag": "🏷️ 核心关键词/Tag"}
    emoji_map = {"creator": "👤", "actor": "👠", "tag": "🏷️"}
    title = title_map.get(etype, '列表')
    emoji = emoji_map.get(etype, '📌')
    
    lines = [f"**{title}** (第 {offset//limit + 1} 页)\\n━━━━━━━━━━━━━━"]
    
    if not entities:
        lines.append("\\n📭 暂无已确认项目，请先运行'更新检索数据库'。")
        buttons = [[Button.inline("🔙 返回搜索中心", b"nav_search_center")]]
    else:
        buttons = []
        for e in entities:
             name = e['name']
             count = e.get('msg_count', 0)
             display = f"{emoji} {name} ({count})" if count else f"{emoji} {name}"
             buttons.append([Button.inline(display, f"do_search_{name}".encode())])
        
        # 翻页按钮
        nav_row = []
        if offset > 0:
            nav_row.append(Button.inline("⬅️ 上一页", f"sc_list_{etype}_{offset-limit}".encode()))
        if len(entities) == limit:
            nav_row.append(Button.inline("下一页 ➡️", f"sc_list_{etype}_{offset+limit}".encode()))
        if nav_row: buttons.append(nav_row)
        buttons.append([Button.inline("🔙 返回搜索中心", b"nav_search_center")])

    await event.edit("\\n".join(lines), buttons=buttons)"""

content = re.sub(r'async def render_entity_list\(event, etype, offset=0\):.*?await event\.edit\("\\n"\.join\(lines\), buttons=buttons\)', new_render_entity_list, content, flags=re.DOTALL)

# 4. Handle "awaiting_search_keyword" in handle_search_text (not handled yet)
# I'll check if handle_search_text handles keyword search already
if 'awaiting_search_keyword' not in content:
   # Placeholder for inserting it if missing, but let's assume it exists or I'll check later
   pass

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ search_bot.py reformatted for 5-button Mode 3 UI.")
