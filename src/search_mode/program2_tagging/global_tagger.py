import sqlite3
import json
import re
import os
import argparse
import sys
import time

# 工作流 Program 2: 全局自动化打标 (全量回填)
# 逻辑：加载 entities.json -> 编译正则 -> 批量扫描数据库 -> 回写标签

class GlobalTagger:
    def __init__(self, bot_name='tgporncopilot', db_path='data/copilot.db'):
        # 延迟导入以避免主块未运行时的路径问题
        from utils.config import get_bot_config
        self.config = get_bot_config(bot_name)
        
        self.db_path = db_path
        self.entities_path = os.path.join(self.config['currententities_dir_data'], 'entities.json')
        self.managed_folders = self.config['managed_folders']
        self.entities = self.load_entities()
        self.patterns = self.compile_patterns()
        
        # 确定该 Bot 管辖的 Chat ID 集合
        self.managed_chat_ids = self.get_managed_chat_ids()

    def load_entities(self):
        if not os.path.exists(self.entities_path):
            print(f"❌ 找不到实体字典: {self.entities_path}")
            return {"creators": [], "actors": [], "keywords": []}
        with open(self.entities_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def compile_patterns(self):
        patterns = {'creators': [], 'actors': [], 'keywords': []}
        
        # 统一提取逻辑函数
        def _extract_items(data_source):
            items = []
            if isinstance(data_source, list):
                items = data_source
            elif isinstance(data_source, dict):
                for sub_list in data_source.values():
                    items.extend(sub_list)
            return items

        for cat in ['creators', 'actors', 'keywords']:
            items = _extract_items(self.entities.get(cat, []))
            for item in items:
                main_name = item.get('name')
                if not main_name: continue
                
                aliases = item.get('aliases', [])
                # 核心逻辑：主名和别名都加入搜索，但最终都映射给 main_name
                all_names = [main_name] + aliases
                escaped_names = [re.escape(n) for n in all_names if n]
                if not escaped_names: continue
                
                # 构造正则：部分匹配（不使用 \b），忽略大小写
                pattern_str = '|'.join(escaped_names)
                regex = re.compile(pattern_str, re.IGNORECASE)
                patterns[cat].append((main_name, regex))
        return patterns

    def get_managed_chat_ids(self):
        """
        通过本地备份文件夹名称中的 ID 后缀，反查数据库中的 Chat ID。
        文件夹名通常为: 频道名_123456789
        """
        managed_ids = set()
        for folder in self.managed_folders:
            folder_path = os.path.join('data/archived/backups', folder)
            if os.path.exists(folder_path):
                for channel_dir in os.listdir(folder_path):
                    # 尝试从名字末尾提取 ID (例如 _1002480302932)
                    if '_' in channel_dir:
                        parts = channel_dir.split('_')
                        last_part = parts[-1]
                        if last_part.isdigit():
                            # 转换为数据库存储的负数形式 (Channel ID 规则)
                            # 如果是以 100 开头的长 ID，说明已经是完整 ID 绝对值
                            if last_part.startswith("100") and len(last_part) > 10:
                                chat_id = int(f"-{last_part}")
                            else:
                                chat_id = int(f"-100{last_part}")
                            managed_ids.add(chat_id)
                        
        if not managed_ids:
            # Fallback: 如果没找到 ID 后缀，尝试直接按文件夹名查 canonical_name
            # 这里省略，直接报错
            print("⚠️ 警告: 未能从文件夹名中提取到任何有效的 Chat ID。")
            return set()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 验证这些 ID 确实存在于 global_messages 中
        placeholders = ",".join(['?'] * len(managed_ids))
        query = f"SELECT DISTINCT chat_id FROM global_messages WHERE chat_id IN ({placeholders})"
        cursor.execute(query, list(managed_ids))
        valid_ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        print(f"📡 探测到管辖频道 ID 数: {len(valid_ids)} (基于文件夹 ID 提取)")
        return valid_ids

    def tag_all(self, batch_size=1000, progress_file=None):
        if not os.path.exists(self.db_path):
            print(f"❌ 找不到数据库: {self.db_path}")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 构建过滤条件
        where_clause = ""
        params = []
        if self.managed_chat_ids:
            # SQLite 不支持直接 IN 超过 1000 个，但这里通常不会超过这个数
            placeholders = ",".join(['?'] * len(self.managed_chat_ids))
            where_clause = f"WHERE chat_id IN ({placeholders})"
            params = list(self.managed_chat_ids)
        else:
            print("⚠️ 警告: 未找到管辖频道，打标将跳过。")
            conn.close()
            return

        # 获取各个频道的任务量
        cursor.execute(f"SELECT chat_id, chat_name, COUNT(*) FROM global_messages {where_clause} GROUP BY chat_id, chat_name", params)
        channel_tasks = cursor.fetchall()
        total = sum(c[2] for c in channel_tasks)
        total_channels = len(channel_tasks)
        
        print(f"🚀 开始全量打标 ({self.config['app_name']})，目标频道数: {total_channels}，目标消息数: {total}")

        if progress_file:
            try:
                os.makedirs(os.path.dirname(progress_file), exist_ok=True)
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({'status': 'running', 'total': total, 'current': 0, 'updated': 0}, f)
            except: pass

        updated_count = 0
        msgs_done = 0
        channels_done = 0
        start_time = time.time()
        last_report_time = 0
        
        for chat_id, chat_name, channel_total in channel_tasks:
            chat_name_disp = chat_name if chat_name else str(chat_id)
            channel_done = 0
            offset = 0
            
            while offset < channel_total:
                cursor.execute(f"""
                    SELECT rowid, text_content, file_name 
                    FROM global_messages 
                    WHERE chat_id = ?
                    LIMIT ? OFFSET ?
                """, (chat_id, batch_size, offset))
                rows = cursor.fetchall()
                updates = []
                
                for rowid, text, file_name in rows:
                    full_text = f"{text or ''} {file_name or ''}"
                    tags = {'creator': [], 'actor': [], 'keywords': []}
                    
                    for cat_key, db_key in [('creators', 'creator'), ('actors', 'actor'), ('keywords', 'keywords')]:
                        for main_name, regex in self.patterns[cat_key]:
                            if regex.search(full_text):
                                tags[db_key].append(main_name)
                    
                    c_str = ", ".join(sorted(list(set(tags['creator']))))
                    a_str = ", ".join(sorted(list(set(tags['actor']))))
                    k_str = ", ".join(sorted(list(set(tags['keywords']))))
                    updates.append((c_str, a_str, k_str, rowid))

                if updates:
                    cursor.executemany("""
                        UPDATE global_messages 
                        SET creator = ?, actor = ?, keywords = ? 
                        WHERE rowid = ?
                    """, updates)
                    updated_count += len(updates)
                
                added = len(rows)
                offset += added
                channel_done += added
                msgs_done += added
                
                now = time.time()
                # 每 1 秒报告一次进度
                if now - last_report_time >= 1.0 or msgs_done >= total:
                    elapsed = now - start_time
                    spd = msgs_done / elapsed if elapsed > 0 else 0
                    rem_calc = (total - msgs_done) / spd if spd > 0 else 0
                    eta_min = round(rem_calc / 60, 1)
                    
                    p_total = (msgs_done / total * 100) if total > 0 else 0
                    p_chan = (channel_done / channel_total * 100) if channel_total > 0 else 0
                    
                    bar_total = "▓" * int(p_total/10) + "░" * (10 - int(p_total/10))
                    bar_chan = "█" * int(p_chan/10) + "▒" * (10 - int(p_chan/10))
                    
                    detail_text = (
                        f"⏳ <b>打标进行中...</b> (预计剩余 {eta_min} 分钟)<br>"
                        f"━━━━━━━━━━━━━━<br>"                        f"📦 <b>总进度</b>: <span style='font-family: monospace;'>{bar_total}</span> {p_total:.1f}%<br>"
                        f"📂 频道: {channels_done}/{total_channels}<br>"
                        f"📨 条目处理: {msgs_done}/{total}<br>"
                        f"━━━━━━━━━━━━━━<br>"
                        f"📍 <b>当前</b>: {chat_name_disp}<br>"
                        f"📈 分频进度: <span style='font-family: monospace;'>{bar_chan}</span> {p_chan:.1f}%<br>"
                    )
                    
                    print(f"⏳ 进度: {msgs_done}/{total} [{chat_name_disp}]")
                    if progress_file:
                        try:
                            with open(progress_file, 'w', encoding='utf-8') as f:
                                json.dump({
                                    'status': 'running', 
                                    'total': total, 
                                    'current': msgs_done, 
                                    'updated': updated_count,
                                    'step_msg': f"正在处理 {chat_name_disp} ({msgs_done}/{total})",
                                    'detail_html': detail_text,
                                    'eta_min': eta_min,
                                    'p_total': round(p_total, 1)
                                }, f)
                        except: pass
                    last_report_time = now
            
            # channel end
            channels_done += 1
            
        conn.commit()
        conn.close()
        
        if progress_file:
            try:
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'status': 'completed', 
                        'total': total, 
                        'current': total, 
                        'updated': updated_count,
                        'msg': f"✅ 全量打标完成！共处理 {updated_count} 条。"
                    }, f)
            except: pass

        print(f"✅ [{self.config['app_name']}] 全量打标完成！共处理 {updated_count} 条消息。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Global Tagger')
    parser.add_argument('--bot', type=str, default='tgporncopilot', help='指定 Bot 身份')
    parser.add_argument('--progress-file', type=str, default=None, help='进度 JSON 文件路径')
    args = parser.parse_args()

    # 路径修正
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    tagger = GlobalTagger(bot_name=args.bot)
    tagger.tag_all(progress_file=args.progress_file)
