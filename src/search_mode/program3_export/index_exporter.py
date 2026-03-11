import sqlite3
import os
import argparse
import sys
import re
import time

# 工作流 Program 3: 精简版索引导出 (docs/tags/)
# 逻辑：从数据库提取打标信息 -> 按频道聚合 -> 生成精简预览 MD

class IndexExporter:
    def __init__(self, bot_name='tgporncopilot', db_path='data/copilot.db'):
        from utils.config import get_bot_config
        self.config = get_bot_config(bot_name)
        
        self.db_path = db_path
        # 修改输出目录为 Bot 特定子目录，防止多 Bot 混淆
        self.output_dir = os.path.join('docs/tags', bot_name)
        self.managed_folders = self.config['managed_folders']
        
        # 确定该 Bot 管辖的频道名称集合
        self.managed_chat_names = self.get_managed_chat_names()

    def _format_list(self, val):
        if not val or val == '-': return '`-`'
        items = [i.strip() for i in val.split(',') if i.strip()]
        return " ".join([f"`{i}`" for i in items])

    def get_managed_chat_names(self):
        """物理扫描备份目录，获取当前 Bot 管辖的频道名集合"""
        backup_base = 'data/archived/backups'
        chat_names = set()
        if not os.path.exists(backup_base):
            return chat_names
            
        print(f"🔍 正在检索管辖范围 [{','.join(self.managed_folders)}] 下的频道列表...")
        for folder in self.managed_folders:
            folder_path = os.path.join(backup_base, folder)
            if not os.path.exists(folder_path): continue
            
            # 这里的子目录名通常就是频道名 (或包含 ID)
            for channel_dir in os.listdir(folder_path):
                # 如果文件夹名含有 [ID]，通常前部分是频道名
                # 但 SQL 中 chat_name 倾向于原始频道标题
                # 最稳妥的方法是查数据库对应 chat_id 的最新名称，或者直接用目录名匹配
                chat_names.add(channel_dir)
        
        print(f"✅ 找到 {len(chat_names)} 个潜在频道文件夹。")
        return chat_names

    def export(self, progress_file=None):
        if not os.path.exists(self.db_path):
            print(f"❌ 找不到数据库: {self.db_path}")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 核心：按托管目录分组导出
        backup_base = 'data/archived/backups'
        
        # 预计算频道总数
        all_channels = []
        for folder in self.managed_folders:
            folder_path = os.path.join(backup_base, folder)
            if not os.path.exists(folder_path): continue
            for channel_dir in os.listdir(folder_path):
                if os.path.isdir(os.path.join(folder_path, channel_dir)):
                    all_channels.append((folder, channel_dir))
        
        total_channels = len(all_channels)
        print(f"📂 开始导出预览索引，共 {total_channels} 个频道...")

        if progress_file:
            try:
                os.makedirs(os.path.dirname(progress_file), exist_ok=True)
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({'status': 'running', 'total': total_channels, 'current': 0}, f)
            except: pass

        # [NEW] 1. 预扫描：计算全局任务量 (消息总数) 以便渲染进度条
        all_chat_ids = []
        for folder, channel_dir in all_channels:
            chat_id = None
            if '_' in channel_dir:
                parts = channel_dir.split('_')
                last_part = parts[-1]
                if last_part.isdigit():
                    if last_part.startswith("100") and len(last_part) > 10:
                        chat_id = int(f"-{last_part}")
                    else:
                        chat_id = int(f"-100{last_part}")
            if chat_id:
                all_chat_ids.append(chat_id)
        
        total_msgs_global = 0
        if all_chat_ids:
            placeholders = ",".join(["?"] * len(all_chat_ids))
            cursor.execute(f"SELECT COUNT(*) FROM global_messages WHERE chat_id IN ({placeholders})", all_chat_ids)
            total_msgs_global = cursor.fetchone()[0] or 0

        print(f"🚀 开始索引导出: 管辖 {len(all_channels)} 个频道, 共计 {total_msgs_global} 条消息...")
        
        global_done_msgs = 0
        channels_done = 0
        total_channels = len(all_channels)
        
        for folder, channel_dir in all_channels:
            channels_done += 1
            folder_path = os.path.join(backup_base, folder)
            output_subfolder = os.path.join(self.output_dir, folder)
            os.makedirs(output_subfolder, exist_ok=True)
            
            chat_id = None
            if '_' in channel_dir:
                parts = channel_dir.split('_')
                last_part = parts[-1]
                if last_part.isdigit():
                    if last_part.startswith("100") and len(last_part) > 10:
                        chat_id = int(f"-{last_part}")
                    else:
                        chat_id = int(f"-100{last_part}")
            
            if not chat_id:
                print(f"  ⚠️ 跳过: {channel_dir} (无法提取 ID)")
                continue

            # 查询获取该频道名称 (仅用于展示) 和所有已打标消息
            cursor.execute("SELECT DISTINCT chat_name FROM global_messages WHERE chat_id = ?", (chat_id,))
            row_name = cursor.fetchone()
            db_name = row_name[0] if row_name else channel_dir

            # 查询备份消息 (按 ID DESC 获取，随后在 Python 中按组聚合并二次排序)
            cursor.execute("""
                SELECT msg_id, media_group_id, original_time, text_content, creator, actor, keywords, supplement,
                       msg_type
                FROM global_messages
                WHERE chat_id = ?
                ORDER BY msg_id DESC
            """, (chat_id,))

            rows = cursor.fetchall()
            chan_total_msgs = len(rows)
            
            # 第一阶段：按 Media Group 聚合
            groups_dict = {} # key为 mg_id 或 single_msg_id
            chan_done_msgs = 0
            
            for i, (msg_id, mg_id, time_val, text, creator, actor, keywords, supplement, msg_type) in enumerate(rows):
                chan_done_msgs += 1
                global_done_msgs += 1
                
                # 实时汇报进度 (每 20 条消息或频道开始/结束)
                if progress_file and (i % 20 == 0 or i == chan_total_msgs - 1):
                    try:
                        p_total = (global_done_msgs / total_msgs_global * 100) if total_msgs_global > 0 else 0
                        p_chan = (chan_done_msgs / chan_total_msgs * 100) if chan_total_msgs > 0 else 0
                        
                        bar_total = "▓" * int(p_total/10) + "░" * (10 - int(p_total/10))
                        bar_chan = "█" * int(p_chan/10) + "┈" * (10 - int(p_chan/10))
                        
                        detail_text = (
                            f"⏳ <b>索引导出进行中...</b><br>━━━━━━━━━━━━━━<br>"
                            f"📦 <b>总进度</b>: {p_total:.1f}%<br>"
                            f"📂 频道: {channels_done}/{total_channels}<br>"
                            f"📨 条目处理: {global_done_msgs}/{total_msgs_global}<br>━━━━━━━━━━━━━━<br>"
                            f"📍 <b>当前</b>: {db_name}<br>"
                            f"📈 分频进度: {p_chan:.1f}%<br>"
                        )
                        
                        with open(progress_file, 'w', encoding='utf-8') as f:
                            json.dump({
                                'status': 'running', 
                                'total': total_msgs_global, 
                                'current': global_done_msgs,
                                'p_total': round(p_total, 1),
                                'step_msg': f"正在导出: {db_name} ({p_chan:.1f}%)",
                                'detail_html': detail_text
                            }, f)
                    except: pass

                # 增强型资源/链接判定
                has_url = False
                if text:
                    if re.search(r'https?://\S+|t.me/\S+', text):
                        has_url = True
                has_media = msg_type in ('video', 'photo', 'file', 'gif')
                has_link = msg_type in ('link', 'link_preview') or has_url
                is_resource_msg = has_media or has_link
                
                group_key = mg_id if mg_id is not None else f"single_{msg_id}"
                
                if group_key not in groups_dict:
                    groups_dict[group_key] = {
                        'msg_id': msg_id,
                        'time': time_val,
                        'text': text,
                        'creator': creator if creator and creator != '-' else '',
                        'actor': actor if actor and actor != '-' else '',
                        'keywords': keywords if keywords and keywords != '-' else '',
                        'supplement': supplement if supplement and supplement != '-' else '',
                        'has_resource_or_link': is_resource_msg,
                        'max_id': msg_id
                    }
                else:
                    g = groups_dict[group_key]
                    g['has_resource_or_link'] |= is_resource_msg
                    
                    # 文本聚合 (取第一个非空的)
                    if not g['text'] and text:
                        g['text'] = text
                    
                    # 标签属性聚合 (聚合所有消息的标签)
                    def _merge_tags(existing, news):
                        if not news or news == '-': return existing
                        items = [i.strip() for i in news.split(',') if i.strip()]
                        curr = [i.strip() for i in existing.split(',') if i.strip()]
                        return ", ".join(sorted(list(set(curr + items))))

                    g['creator'] = _merge_tags(g['creator'], creator)
                    g['actor'] = _merge_tags(g['actor'], actor)
                    g['keywords'] = _merge_tags(g['keywords'], keywords)
                    
                    if supplement and supplement != '-' and supplement not in g['supplement']:
                        g['supplement'] = (g['supplement'] + " " + supplement).strip()

                    if msg_id > g['max_id']:
                        # 确保 g 数据反映的是组内 ID 最大的那条消息的基础信息
                        g['max_id'] = msg_id
                        g['msg_id'] = msg_id
                        g['time'] = time_val

            # 第二阶段：按 max_id 排序确保顺序和编号与备份完全一致
            groups = sorted(groups_dict.values(), key=lambda x: x['max_id'], reverse=True)
            
            if not groups:
                print(f"  ⏭️ 跳过: {db_name} (无数据)")
                continue

            print(f"  📄 正在导出: {db_name} -> {folder}/{channel_dir}.md ({len(groups)} 组)")
            safe_name = re.sub(r'[^\w\u4e00-\u9fa5]', '_', channel_dir)
            output_file = os.path.join(output_subfolder, f"{safe_name}.md")

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"# 🏷️ 频道预览索引: {db_name}\n\n")
                f.write(f"📂 分组: `{folder}` / `{channel_dir}`\n")
                f.write(f"此文件由 [{self.config['app_name']}] Program 3 自动导出，用于核对打标结果。\n\n")

                total_groups = len(groups)
                for idx, g in enumerate(groups):
                    group_num = total_groups - idx
                    f.write(f"---\n\n")
                    # 恢复 第 N 组消息 格式，并附带消息 ID 确保对齐
                    f.write(f"### 第 {group_num} 组消息 ({g.get('time') or '未知时间'}) [ID:{g.get('msg_id', '未知')}]\n\n")
                    
                    if g['text']:
                        f.write(f"> {g['text']}\n\n")
                    else:
                        f.write(f"> (无文本内容)\n\n")
                    
                    # 仅针对带资源/链接的消息展示四项打标属性 (创作者、人物、关键词、补充)
                    if g.get('has_resource_or_link'):
                        creator_val = self._format_list(g['creator'])
                        f.write(f"- 🎨 **创作者**: {creator_val}\n")
                        
                        actor_val = self._format_list(g['actor'])
                        f.write(f"- 👠 **主要人物**: {actor_val}\n")
                        
                        kw_val = self._format_list(g['keywords'])
                        f.write(f"- 🏷️ **关键词**: {kw_val}\n")
                        
                        supp_val = g['supplement'].strip() if g['supplement'] else '`-`'
                        f.write(f"- 📝 **补充**: {supp_val}\n")
                    
                    f.write("\n")
            
        conn.close()
        
        if progress_file:
            try:
                with open(progress_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'status': 'completed', 
                        'total': total_channels, 
                        'current': total_channels,
                        'msg': f"✅ 索引导出完成！共生成 {total_channels} 个文档。"
                    }, f)
            except: pass

        print(f"✅ [{self.config['app_name']}] 导出完成！")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Index Exporter')
    parser.add_argument('--bot', type=str, default='tgporncopilot', help='指定 Bot 身份')
    parser.add_argument('--progress-file', type=str, default=None, help='进度 JSON 文件路径')
    args = parser.parse_args()

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    exporter = IndexExporter(bot_name=args.bot)
    exporter.export(progress_file=args.progress_file)
