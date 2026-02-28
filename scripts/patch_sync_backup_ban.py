#!/usr/bin/env python3
"""
Patch sync.py: 全平台封禁频道跳过 + 汇报
Also patch backup.py main loop: collect skipped channels and report in summary
"""
# ===== sync.py patch =====
f = open('src/sync_mode/sync.py', 'r', encoding='utf-8')
sync_content = f.read()
f.close()

OLD_SYNC = (
    "            chat_id = utils.get_peer_id(entity)\n"
    "            current_title = getattr(entity, 'title', None) or getattr(entity, 'first_name', '') or str(chat_id)"
)
NEW_SYNC = (
    "            # 检查全平台封禁\n"
    "            restriction_reasons = getattr(entity, 'restriction_reason', []) or []\n"
    "            is_globally_banned = any(\n"
    "                getattr(r, 'platform', '') == 'all' and getattr(r, 'reason', '') == 'terms'\n"
    "                for r in restriction_reasons\n"
    "            )\n"
    "            if is_globally_banned:\n"
    "                ban_name = getattr(entity, 'title', str(entity.id))\n"
    "                print(f\"  🚫 [全平台封禁] {ban_name}，已跳过同步。\")\n"
    "                if 'skipped_banned' not in locals(): skipped_banned = []\n"
    "                skipped_banned.append(ban_name)\n"
    "                continue\n"
    "            chat_id = utils.get_peer_id(entity)\n"
    "            current_title = getattr(entity, 'title', None) or getattr(entity, 'first_name', '') or str(chat_id)"
)

if OLD_SYNC in sync_content:
    sync_content = sync_content.replace(OLD_SYNC, NEW_SYNC, 1)
    open('src/sync_mode/sync.py', 'w', encoding='utf-8').write(sync_content)
    print('OK - sync.py skip logic added')
else:
    print('FAILED - sync.py')

# ===== backup.py main loop: handle skipped result + collect for summary =====
f = open('src/backup_mode/backup.py', 'r', encoding='utf-8')
backup_content = f.read()
f.close()

OLD_BK = (
    "            res = await backup_channel(client, t, is_test=args.test, global_stats=global_stats, run_label=label)\n"
    "            \n"
    "            if res:\n"
    "                if isinstance(res, dict) and res.get('status') == 'stopped':\n"
    "                    break\n"
    "                results.append(res)"
)
NEW_BK = (
    "            res = await backup_channel(client, t, is_test=args.test, global_stats=global_stats, run_label=label)\n"
    "            \n"
    "            if res:\n"
    "                if isinstance(res, dict) and res.get('status') == 'stopped':\n"
    "                    break\n"
    "                if isinstance(res, dict) and res.get('skipped'):\n"
    "                    # 全平台封禁：记录跳过但不计入统计\n"
    "                    global_stats.setdefault('skipped_banned', []).append(res.get('name', str(t)))\n"
    "                    global_stats['completed_channels_count'] += 1\n"
    "                    update_progress(global_stats)\n"
    "                    continue\n"
    "                results.append(res)"
)

if OLD_BK in backup_content:
    backup_content = backup_content.replace(OLD_BK, NEW_BK, 1)
    open('src/backup_mode/backup.py', 'w', encoding='utf-8').write(backup_content)
    print('OK - backup.py loop handles skipped result')
else:
    print('FAILED - backup.py loop')

# ===== backup.py finish_backup_run: include skipped_banned in report =====
OLD_FIN = (
    '        db.finish_backup_run(run_id, {\n'
    '            "total_messages": global_stats[\'total_messages\'],\n'
    '            "new_messages": global_stats[\'new_messages\'],\n'
    '            "total_channels": global_stats[\'completed_channels_count\'],\n'
    '            "channels": global_stats[\'channels\'],\n'
    '            "duration": f"{elapsed/60:.1f} min"\n'
    '        })\n'
    '        print(f"\\n✨ {label} 全部备份任务已完成！ (状态: {final_status})")'
)
NEW_FIN = (
    '        skipped_banned = global_stats.get(\'skipped_banned\', [])\n'
    '        db.finish_backup_run(run_id, {\n'
    '            "total_messages": global_stats[\'total_messages\'],\n'
    '            "new_messages": global_stats[\'new_messages\'],\n'
    '            "total_channels": global_stats[\'completed_channels_count\'],\n'
    '            "channels": global_stats[\'channels\'],\n'
    '            "duration": f"{elapsed/60:.1f} min",\n'
    '            "skipped_banned": skipped_banned\n'
    '        })\n'
    '        print(f"\\n✨ {label} 全部备份任务已完成！ (状态: {final_status})")\n'
    '        if skipped_banned:\n'
    '            print(f"\\n🚫 以下频道因全平台封禁已跳过 ({len(skipped_banned)} 个):")\n'
    '            for n in skipped_banned: print(f"  - {n}")'
)
if OLD_FIN in backup_content:
    backup_content = backup_content.replace(OLD_FIN, NEW_FIN, 1)
    open('src/backup_mode/backup.py', 'w', encoding='utf-8').write(backup_content)
    print('OK - backup.py finish report includes skipped_banned')
else:
    print('FAILED - backup.py finish')
