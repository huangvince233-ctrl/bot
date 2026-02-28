#!/usr/bin/env python3
# 用 deleted_channels_map 替换历史频道的 O(N^2) os.walk 遍历

f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()

old_hist_block = '''        # [NEW] 历史频道虚拟分组：读取本地 metadata 中 is_deleted=true 的频道
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

new_hist_block = '''        # [NEW] 历史频道虚拟分组：使用预缓存的 deleted_channels_map（O(1) 查找）
        # is_deleted 由 update_docs.py (refresh) 在检测到频道不可访问时自动写入
        historical_lines = []
        for mid_str, ch_info in deleted_channels_map.items():
            try:
                bk_info = db.get_latest_backup_info(int(mid_str))
                if bk_info and bk_info.get('time'):
                    t = bk_info['time'][:16].replace('T', ' ')
                    da = ch_info.get('deleted_at', '')
                    da_str = f"  *(断连 {da})*" if da else ""
                    historical_lines.append(f"  \U0001f4a4 {ch_info['name']}  {bk_info['label']} · {t}{da_str}")
            except:
                pass

        if historical_lines:
            lines.append("\\n\\n\U0001f5c4\ufe0f **历史频道 (本地保留)**")
            lines.extend(historical_lines)'''

if old_hist_block in content:
    content = content.replace(old_hist_block, new_hist_block, 1)
    open('src/search_bot.py', 'w', encoding='utf-8').write(content)
    print('OK - historical section optimized')
else:
    print('NOT FOUND')
    idx = content.find('历史频道虚拟分组')
    print(repr(content[idx-5:idx+200]))
