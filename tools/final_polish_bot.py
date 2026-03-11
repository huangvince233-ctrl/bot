
import os
import re

file_path = 'src/search_bot.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. 在 handle_search_text 中添加 awaiting_entity_append_* 处理
# 定位插入点：在 state = user_states.get(chat_id) 之后
insert_pattern = r'(state = user_states\.get\(chat_id, \'\'\)\s+)' # 兼容不同引用
if 'awaiting_entity_append_' not in content:
    logic = """
    # [NEW] 处理手动快速补充词库
    if state and state.startswith('awaiting_entity_append_'):
        etype = state.replace('awaiting_entity_append_', '')
        name = (event.text or '').strip()
        if name and not name.startswith('/'):
            user_states.pop(chat_id, None)
            # 写入 entities.json
            import json as _j
            from src.utils.config import get_bot_config as _get_cfg
            cfg = _get_cfg(BOT_NAME)
            path = os.path.join(os.path.dirname(__file__), '../', cfg.get('entities_json', ''))
            try:
                with open(path, 'r', encoding='utf-8') as f: data = _j.load(f)
                key = {'creator':'creators', 'actor':'actors', 'tag':'keywords'}.get(etype)
                if key:
                    if key not in data: data[key] = []
                    # 检查重复
                    names = [(i['name'] if isinstance(i,dict) else i) for i in data[key]]
                    if name not in names:
                        if etype == 'tag': data[key].append(name)
                        else: data[key].append({"name": name, "aliases": []})
                        with open(path, 'w', encoding='utf-8') as f: _j.dump(data, f, ensure_ascii=False, indent=4)
                        await event.respond(f"✅ 已手动补充 {etype} 词条: `{name}`\\n💡 提示：新的词库将在下次运行索引更新时生效。")
                    else:
                        await event.respond(f"ℹ️ 词条 `{name}` 已在库中。")
                await render_entity_list(event, etype, offset=0)
            except Exception as e:
                await event.respond(f"❌ 补充词条失败: {e}")
            return
"""
    # 查找 dispatcher 的开始
    content = re.sub(r'(async def handle_search_text\(event\):.*?state = user_states\.get\(chat_id\))', r'\1' + logic, content, flags=re.DOTALL)

# 2. 确保 render_entity_list 包含 [➕ 快速补充词条] 按钮
if 'sc_append_' not in content:
    old_buttons = 'buttons.append([Button.inline("🔙 返回搜索中心", b"nav_search_center")])'
    new_buttons = """buttons.append([
        Button.inline("➕ 快速补充词条", f"sc_append_{etype}".encode()),
        Button.inline("🔙 返回搜索中心", b"nav_search_center")
    ])"""
    content = content.replace(old_buttons, new_buttons)

# 3. 添加 sc_append 回调处理
if 'elif cmd.startswith(\'append_\'):' not in content:
    callback_append = """    elif cmd.startswith('append_'):
        etype = cmd.split('_')[1]
        user_states[chat_id] = f'awaiting_entity_append_{etype}'
        label = {"creator":"创作者", "actor":"女m/模特", "tag":"关键词"}.get(etype, etype)
        await event.edit(f"➕ **手动补充 {label}**\\n\\n直接发送名称给我，我将为您录入词库。\\n(建议输入后再点击“更新检索数据库”同步索引)", 
                         buttons=[[Button.inline("取消", f"sc_list_{etype}_0".encode())]])
"""
    # 插入到 search_center_callback 的 elif cmd.startswith('list_'): 之前
    content = content.replace("    elif cmd.startswith('list_'):", callback_append + "    elif cmd.startswith('list_'):")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Done.")
