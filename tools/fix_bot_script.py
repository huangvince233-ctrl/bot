
import os
import re

file_path = 'src/search_bot.py'
content = None
for enc in ['utf-8', 'gbk', 'utf-16', 'latin-1']:
    try:
        with open(file_path, 'r', encoding=enc) as f:
            content = f.read()
            print(f"✅ Loaded with {enc}")
            break
    except:
        continue

if not content:
    print("❌ Failed to load file with common encodings.")
    exit(1)

# 1. 修复引号冲突
content = content.replace('text += "\\n*(暂无名单，请先运行"更新词库")*"', 'text += "\\n*(暂无名单，请先运行\'更新词库\')*"')
# 兜底：如果上面没匹配到，试试可能存在的转义版
content = content.replace('text += "\\n*(暂无名单，请先运行\\"更新词库\\")*"', 'text += "\\n*(暂无名单，请先运行\'更新词库\')*"')

# 2. 完善 handle_search_text 逻辑
handle_search_pattern = r'(async def handle_search_text\(event\):)'
handle_search_logic = """
    chat_id = event.chat_id
    state = user_states.get(chat_id, '')
    
    if state.startswith('awaiting_entity_append_'):
        etype = state.replace('awaiting_entity_append_', '')
        name = (event.text or '').strip()
        if name and not name.startswith('/'):
            user_states.pop(chat_id, None)
            import json as _json
            entities_path = os.path.join(os.path.dirname(__file__), '../data/entities', f'{BOT_NAME}_entities.json')
            try:
                with open(entities_path, 'r', encoding='utf-8') as f:
                    data = _json.load(f)
                key_map = {'creator': 'creators', 'actor': 'actors', 'tag': 'keywords'}
                key = key_map.get(etype)
                if key:
                    if key not in data: data[key] = []
                    exists = False
                    for item in data[key]:
                        check_n = item['name'] if isinstance(item, dict) else item
                        if check_n == name:
                            exists = True; break
                    if not exists:
                        if etype == 'tag': data[key].append(name)
                        else: data[key].append({"name": name, "aliases": []})
                        with open(entities_path, 'w', encoding='utf-8') as f:
                            _json.dump(data, f, ensure_ascii=False, indent=4)
                        await event.respond(f"✅ 已成功添加 {etype} 词条: `{name}`")
                    else:
                        await event.respond(f"ℹ️ 词条 `{name}` 已存在。")
                await render_entity_list(event, etype, offset=0)
            except Exception as e:
                await event.respond(f"❌ 添加失败: {e}")
            return
"""

if 'state.startswith(\'awaiting_entity_append_\')' not in content:
    content = re.sub(handle_search_pattern, r'\1' + handle_search_logic, content)

# 统一写回 UTF-8
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ search_bot.py fixed.")
