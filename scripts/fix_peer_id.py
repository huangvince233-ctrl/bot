#!/usr/bin/env python3
# Fix: peers_info uses e.id (raw) instead of get_peer_id(e) (signed)

f = open('src/search_bot.py', 'r', encoding='utf-8')
content = f.read()
f.close()

TARGET = "peers_info.append({'id': str(e.id), 'title': tname, 'icon': icon, 'is_syncable': is_syncable})"
REPLACE = "peers_info.append({'id': str(utils.get_peer_id(e)), 'title': tname, 'icon': icon, 'is_syncable': is_syncable})"

if TARGET in content:
    content = content.replace(TARGET, REPLACE, 1)
    open('src/search_bot.py', 'w', encoding='utf-8').write(content)
    print('OK - fixed e.id -> utils.get_peer_id(e)')
else:
    print('NOT FOUND - searching context...')
    idx = content.find("peers_info.append")
    print(repr(content[idx:idx+200]))
