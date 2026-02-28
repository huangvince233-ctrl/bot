#!/usr/bin/env python3
"""
Patch backup.py: 全平台封禁频道直接跳过，最后在汇报中列出
"""
f = open('src/backup_mode/backup.py', 'r', encoding='utf-8')
content = f.read()
f.close()

# 在 entity 获取后，全平台封禁直接 raise/return 一个跳过信号
OLD = (
    "        entity = await client.get_entity(source_id)\n"
    "        if getattr(entity, 'restricted', False):\n"
    "            print(f\"  ⚠️ [警告] 频道被 Telegram 标记为受限 (Restricted)，尝试继续访问...\")\n"
    "            # 不再抛出异常，允许 API 尝试抓取"
)
NEW = (
    "        entity = await client.get_entity(source_id)\n"
    "        # 检查全平台封禁 vs 局部受限\n"
    "        restriction_reasons = getattr(entity, 'restriction_reason', []) or []\n"
    "        is_globally_banned = any(\n"
    "            getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'\n"
    "            for r in restriction_reasons\n"
    "        )\n"
    "        if is_globally_banned:\n"
    "            name = getattr(entity, 'title', str(source_id))\n"
    "            print(f\"  🚫 [全平台封禁] {name} 无法访问，已跳过。\")\n"
    "            return {'skipped': True, 'name': name, 'reason': 'globally_banned'}\n"
    "        if getattr(entity, 'restricted', False):\n"
    "            print(f\"  ⚠️ [警告] 频道被 Telegram 标记为局部受限，仍尝试访问...\")\n"
    "            # 局部受限仍可访问，不跳过"
)

if OLD in content:
    content = content.replace(OLD, NEW, 1)
    open('src/backup_mode/backup.py', 'w', encoding='utf-8').write(content)
    print('OK - backup.py skip logic added')
else:
    print('NOT FOUND')
    idx = content.find('restricted')
    print(repr(content[idx-5:idx+200]))
