#!/usr/bin/env python3
"""
综合 Patch:
1. get_all_folder_peers: 加 is_globally_banned / is_partial 字段
2. render_sync_status_ui: 加封禁标记
3. render_backup_status_ui: 加封禁标记
"""

f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()
changes = 0

# ===== PATCH 1: get_all_folder_peers - add restriction fields =====
OLD1 = "peers_info.append({'id': str(utils.get_peer_id(e)), 'title': tname, 'icon': icon, 'is_syncable': is_syncable})"
NEW1 = (
    "# 检查全平台封禁 vs 局部受限\n"
    "                    restriction_reasons = getattr(e, 'restriction_reason', []) or []\n"
    "                    is_globally_banned = any(\n"
    "                        getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'\n"
    "                        for r in restriction_reasons\n"
    "                    )\n"
    "                    is_partial = bool(restriction_reasons) and not is_globally_banned\n"
    "                    peers_info.append({\n"
    "                        'id': str(utils.get_peer_id(e)),\n"
    "                        'title': tname, 'icon': icon, 'is_syncable': is_syncable,\n"
    "                        'is_globally_banned': is_globally_banned, 'is_partial': is_partial,\n"
    "                    })"
)
if OLD1 in content:
    content = content.replace(OLD1, NEW1, 1)
    changes += 1
    print('PATCH 1 OK - restriction fields added to peers_info')
else:
    print('PATCH 1 FAILED')

# ===== PATCH 2: render_sync_status_ui - add ban badge to channel line =====
OLD2 = (
    '                    lines.append(f"  {p[\'icon\']} {p[\'title\']}{st}")\n'
    '    except Exception as e:\n'
    '        lines.append(f"❌ 加载失败: {e}")\n'
    '    \n'
    '    buttons = [\n'
    '        [Button.inline("⬅️ 返回主菜单", b"sync_back")'
)
NEW2 = (
    '                    ban_badge = " 🚫" if p.get("is_globally_banned") else (" ⚠️" if p.get("is_partial") else "")\n'
    '                    lines.append(f"  {p[\'icon\']} {p[\'title\']}{ban_badge}{st}")\n'
    '    except Exception as e:\n'
    '        lines.append(f"❌ 加载失败: {e}")\n'
    '    \n'
    '    buttons = [\n'
    '        [Button.inline("⬅️ 返回主菜单", b"sync_back")'
)
if OLD2 in content:
    content = content.replace(OLD2, NEW2, 1)
    changes += 1
    print('PATCH 2 OK - sync UI ban badge')
else:
    print('PATCH 2 FAILED - searching...')
    idx = content.find('暂无同步记录')
    print(repr(content[idx:idx+300]))

# ===== PATCH 3: render_backup_status_ui - add ban badge to channel line =====
OLD3 = '                    lines.append(f"  {p[\'icon\']} {p[\'title\']}{st}")\n        \n        # [NEW] 历史频道虚拟分组'
NEW3 = (
    '                    ban_badge = " 🚫" if p.get("is_globally_banned") else (" ⚠️" if p.get("is_partial") else "")\n'
    '                    lines.append(f"  {p[\'icon\']} {p[\'title\']}{ban_badge}{st}")\n'
    '        \n        # [NEW] 历史频道虚拟分组'
)
if OLD3 in content:
    content = content.replace(OLD3, NEW3, 1)
    changes += 1
    print('PATCH 3 OK - backup UI ban badge')
else:
    print('PATCH 3 FAILED - searching...')
    idx = content.find('历史频道虚拟分组')
    print(repr(content[idx-200:idx]))

open('src/search_bot.py', 'w', encoding='utf-8').write(content)
print(f'\nDone - {changes}/3 patches applied')
