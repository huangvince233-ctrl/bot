
import os
import re

file_path = 'src/search_bot.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. 修改 render_search_center 菜单
old_buttons = """    buttons = [
        [Button.inline("🔍 关键词搜索", b"search_enter_keyword")],
        [Button.inline("👤 搜创作者", b"search_enter_creator"),
         Button.inline("👠 搜女m", b"search_enter_actor")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main"),
         Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]"""

new_buttons = """    buttons = [
        [Button.inline("🔍 关键词搜索", b"search_enter_keyword")],
        [Button.inline("👤 搜创作者", b"search_enter_creator"),
         Button.inline("👠 搜女m", b"search_enter_actor"),
         Button.inline("🏷️ 搜关键词", b"search_enter_tag")],
        [Button.inline("🔄 更新词库 (P1→分拣→P2→P3)", b"search_update_pipeline")],
        [Button.inline("⬅️ 返回主菜单", b"nav_main"),
         Button.inline("🗑️ 关闭菜单", b"delete_menu")]
    ]"""

content = content.replace(old_buttons, new_buttons)

# 2. 插入新的回调函数 (在 search_enter_actor_callback 之后)
insertion_point = """@bot.on(events.CallbackQuery(data=b'search_enter_actor'))
async def search_enter_actor_callback(event):
    \"\"\"女m/模特搜索入口 -> 先显示名单\"\"\"
    await render_entity_list(event, etype='actor', offset=0)"""

new_handlers = """
@bot.on(events.CallbackQuery(data=b'search_enter_tag'))
async def search_enter_tag_callback(event):
    \"\"\"Tag 搜索入口\"\"\"
    await render_entity_list(event, etype='tag', offset=0)

def _load_entities_json_list(etype):
    \"\"\"从 entities.json 读取指定分类的实体列表\"\"\"
    import json as _json
    key_map = {'creator': 'creators', 'actor': 'actors', 'tag': 'keywords'}
    entities_path = os.path.join(os.path.dirname(__file__), '../data/entities', f'{BOT_NAME}_entities.json')
    try:
        with open(entities_path, 'r', encoding='utf-8') as f:
            data = _json.load(f)
    except:
        return []
    key = key_map.get(etype, '')
    items = data.get(key, [])
    result = []
    for item in items:
        if isinstance(item, dict): result.append((item.get('name', ''), 0))
        else: result.append((str(item), 0))
    return result

@bot.on(events.CallbackQuery(data=re.compile(br'entity_append_(creator|actor|tag)')))
async def entity_append_callback(event):
    \"\"\"添加词条入口\"\"\"
    etype = event.data_match.group(1).decode()
    user_states[event.chat_id] = f'awaiting_entity_append_{etype}'
    label_map = {'creator': '创作者', 'actor': '女m/模特', 'tag': '关键词'}
    await event.edit(
        f"➕ **添加新 {label_map.get(etype, etype)} 词条**\\n\\n直接发送内容，我将写入词库。",
        buttons=[[Button.inline("取消", f"elp_{etype}_0".encode())]]
    )

@bot.on(events.CallbackQuery(data=b'search_update_pipeline'))
async def search_update_pipeline_callback(event):
    \"\"\"全流程触发：P0 -> P1 -> P1.5\"\"\"
    await event.edit(\"🔄 **启动更新流程...**\\n1️⃣ 合并备份\\n2️⃣ 聚类分析\\n3️⃣ 启动分拣网页\\n\\n⏳ 扫描中...\", buttons=None)
    async def run_upd():
        py = sys.executable
        msg = event.message
        # P0
        proc = await asyncio.create_subprocess_shell(f'\"{py}\" src/search_mode/program1_discovery/import_backups.py --bot \"{BOT_NAME}\"')
        await proc.communicate()
        # P1
        await msg.edit(\"⏳ **[2/2] 正在分析候选词 (P1)...**\")
        p1 = await asyncio.create_subprocess_shell(f'\"{py}\" src/search_mode/program1_discovery/entity_extractor.py --bot \"{BOT_NAME}\"', stdout=asyncio.subprocess.PIPE)
        stdout1, _ = await p1.communicate()
        # P1.5
        web_p = await asyncio.create_subprocess_shell(f'\"{py}\" tools/sorter/server.py --bot \"{BOT_NAME}\" --port 8765 --no-browser')
        await asyncio.sleep(2)
        await msg.edit(f\"✅ **分析完成！**\\n🌐 分拣地址: `http://localhost:8765`\\n\\n分拣后点击网页上的导出按钮。\", 
                      buttons=[[Button.inline(\"🔙 搜索中心\", b\"nav_search_center\")]])
    asyncio.create_task(run_upd())
"""

content = content.replace(insertion_point, insertion_point + new_handlers)

# 3. 更新 render_entity_list
old_render = """async def render_entity_list(event, etype, offset=0):
    \"\"\"展示实体名单（分页）。\"\"\"
    limit = 10
    entities = db.get_entities(etype, status=1)
    total = len(entities)
    
    title_map = {'creator': '👤 创作者名单', 'actor': '👠 女m名单'}
    emoji = '👤' if etype == 'creator' else '👠'
    title = title_map.get(etype, '名单')
    
    text = f"**{title}** (共 {total} 位)\\n━━━━━━━━━━━━━━\\n请选择或直接发送名字检索：\\n\"
    if not entities:
        text += \"\\n*(暂无已确认的名单)*\"
    
    buttons = []
    # 当前页实体
    page_data = entities[offset:offset+limit]
    for idx, name, count in page_data:
        callback_data = f\"ent_{etype}_{name}\".encode('utf-8')
        display_name = (name[:16] + '..') if len(name) > 18 else name
        buttons.append([Button.inline(f\"{emoji} {display_name} ({count}条)\", callback_data)])"""

new_render = """async def render_entity_list(event, etype, offset=0):
    \"\"\"展示实体名单（分页），优先读 entities.json\"\"\"
    limit = 10
    title_map = {'creator': '👤 创作者名单', 'actor': '👠 女m名单', 'tag': '🏷️ 关键词名单'}
    emoji_map = {'creator': '👤', 'actor': '👠', 'tag': '🏷️'}
    emoji = emoji_map.get(etype, '📌')
    title = title_map.get(etype, '名单')
    raw = _load_entities_json_list(etype)
    if not raw: raw = [(name, cnt) for _, name, cnt in (db.get_entities(etype, status=1) or [])]
    total = len(raw)
    text = f"**{title}** (共 {total} 位)\\n━━━━━━━━━━━━━━\\n请选择或直接发送名字检索：\\n"
    if not raw: text += "\\n*(暂无名单，请运行\\'更新词库\\')*"
    buttons = []
    page_data = raw[offset:offset+limit]
    for name, count in page_data:
        cb = f"ent_{etype}_{name}".encode('utf-8')
        display = (name[:16] + '..') if len(name) > 18 else name
        cnt_s = f" ({count}条)" if count else ""
        buttons.append([Button.inline(f"{emoji} {display}{cnt_s}", cb)])"""

content = content.replace(old_render, new_render)

# 4. 修改 handle_search_text 逻辑
handle_pattern = r'(async def handle_search_text\(event\):)'
handle_logic = """
    chat_id = event.chat_id
    state = user_states.get(chat_id, '')
    if state.startswith('awaiting_entity_append_'):
        etype = state.replace('awaiting_entity_append_', '')
        name = (event.text or '').strip()
        if name and not name.startswith('/'):
            user_states.pop(chat_id, None)
            import json as _j
            path = os.path.join(os.path.dirname(__file__), '../data/entities', f'{BOT_NAME}_entities.json')
            try:
                with open(path, 'r', encoding='utf-8') as f: data = _j.load(f)
                key = {'creator':'creators', 'actor':'actors', 'tag':'keywords'}.get(etype)
                if key:
                    if key not in data: data[key] = []
                    if name not in [ (i['name'] if isinstance(i,dict) else i) for i in data[key]]:
                        if etype == 'tag': data[key].append(name)
                        else: data[key].append({"name": name, "aliases": []})
                        with open(path, 'w', encoding='utf-8') as f: _j.dump(data, f, ensure_ascii=False, indent=4)
                        await event.respond(f"✅ 已添加 {etype}: `{name}`")
                    else: await event.respond(f"ℹ️ `{name}` 已存在。")
                await render_entity_list(event, etype, offset=0)
            except Exception as e: await event.respond(f"❌ 失败: {e}")
            return
"""
content = re.sub(handle_pattern, r'\\1' + handle_logic.replace('\\', '\\\\'), content)

# 5. 翻页和选择逻辑扩展
content = content.replace("elp_(creator|actor)_(\\d+)", "elp_(creator|actor|tag)_(\\d+)")
content = content.replace("ent_(creator|actor)_(.+)", "ent_(creator|actor|tag)_(.+)")
content = content.replace("search_input_(creator|actor)", "search_input_(creator|actor|tag)")

# 6. 特殊按钮扩展
nav_search_insert = """    buttons.append([
        Button.inline(\"⌨️ 搜索\", f\"search_input_{etype}\".encode()),
        Button.inline(\"➕ 添加词条\", f\"entity_append_{etype}\".encode()),
        Button.inline(\"🔙 搜索中心\", b\"nav_search_center\")
    ])"""

# 寻找 render_entity_list 中的按钮追加点并替换
# (简化处理)
content = content.replace('buttons.append([Button.inline("⌨️ 手动输入名字", input_callback),', 'pass #')
content = content.replace('Button.inline("🔙 返回搜索中心", b"nav_search_center")])', nav_search_insert)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)
"""
