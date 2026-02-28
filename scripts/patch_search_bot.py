#!/usr/bin/env python3
# 用于替换 search_bot.py 中的历史频道逻辑
import os

f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()

# 定位标记
MARKER = '        # [NEW] 处理离线/历史存档部分'
MARKER_END = '        if historical_lines:\n            lines.append("\\n\\n🗄️ **历史存档 / 已断连 (本地保留)**")\n            lines.extend(historical_lines)'

start = content.find(MARKER)
end = content.find(MARKER_END)

if start == -1 or end == -1:
    print(f'MARKER found: {start}, END found: {end}')
    print(repr(content[content.find('处理离线'):content.find('处理离线')+500]))
    exit(1)

# 结束位置要包含 MARKER_END
end += len(MARKER_END)

old_block = content[start:end]
print(f'Old block ({len(old_block)} chars):')
print(repr(old_block[:100]))

new_block = '''        # [NEW] 历史频道虚拟分组：读取本地 metadata 中 is_deleted=true 的频道
        # is_deleted 由 update_docs.py (refresh) 检测到频道不可访问时自动写入
        historical_lines = []
        for mid_str, (name, folder_label) in metadata_id_map.items():
            try:
                # 扫描文件找到对应 JSON，检查 is_deleted 标志
                for root, dirs, files in os.walk(os.path.join('data', 'metadata')):
                    for fname in files:
                        if fname.endswith('.json'):
                            fpath = os.path.join(root, fname)
                            with open(fpath, 'r', encoding='utf-8') as fh:
                                jd = json.load(fh)
                            if str(jd.get('id')) == mid_str and jd.get('is_deleted'):
                                bk_info = db.get_latest_backup_info(int(mid_str))
                                if bk_info and bk_info.get('time'):
                                    t = bk_info['time'][:16].replace('T', ' ')
                                    deleted_at = jd.get('deleted_at', '')[:10]
                                    da_str = f"  *(断连 {deleted_at})*" if deleted_at else ""
                                    historical_lines.append(f"  \U0001f4a4 {name}  {bk_info['label']} · {t}{da_str}")
                                break
                    else:
                        continue
                    break
            except:
                pass

        if historical_lines:
            lines.append("\\n\\n\U0001f5c4\ufe0f **历史频道 (本地保留)**")
            lines.extend(historical_lines)'''

content = content[:start] + new_block + content[end:]
open('src/search_bot.py', 'w', encoding='utf-8').write(content)
print('OK - search_bot.py updated')
