#!/usr/bin/env python3
"""
Patch 1: get_all_folder_peers 加 restriction/banned 字段
"""
f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()

OLD = "peers_info.append({'id': str(utils.get_peer_id(e)), 'title': tname, 'icon': icon, 'is_syncable': is_syncable})"

NEW = """
                    # 检查封禁/受限状态 (全平台封禁 vs 局部受限)
                    restriction_reasons = getattr(e, 'restriction_reason', []) or []
                    is_globally_banned = any(
                        getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'
                        for r in restriction_reasons
                    )
                    is_partial = bool(restriction_reasons) and not is_globally_banned
                    peers_info.append({
                        'id': str(utils.get_peer_id(e)),
                        'title': tname,
                        'icon': icon,
                        'is_syncable': is_syncable,
                        'is_globally_banned': is_globally_banned,
                        'is_partial': is_partial,
                    })"""

if OLD in content:
    content = content.replace(OLD, NEW.strip(), 1)
    open('src/search_bot.py', 'w', encoding='utf-8').write(content)
    print('OK - peers_info now has restriction info')
else:
    print('NOT FOUND')
    idx = content.find('peers_info.append')
    print(repr(content[idx:idx+150]))
