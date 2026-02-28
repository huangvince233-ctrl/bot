import os
import sys
import re
import json
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.db import Database

class SearchDatabaseUpdater:
    def __init__(self, db_path='data/copilot.db', entities_path='data/entities.json'):
        self.db = Database(db_path)
        self.entities_path = entities_path
        self.candidates = {}

    def load_entities(self):
        """加载已敲定的实体列表"""
        if os.path.exists(self.entities_path):
            with open(self.entities_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"version": "1.0", "creators": [], "actors": []}

    def extract_candidates(self, output_file='data/candidate_pool.md'):
        """遍历 is_extracted=0 的记录，提取潜在的创作者和模特候选并输出为 Markdown"""
        print("🔍 正在扫描全库以提取新实体候选...")
        # 取出所有尚未被最终索引的消息
        self.db.cursor.execute("SELECT chat_name, sender_name, file_name, text_content FROM global_messages WHERE is_extracted = 0")
        rows = self.db.cursor.fetchall()
        
        ignore_tags = {
            'BDSM', '调教', '萝莉', '国产', '黑丝', '白丝', '羞耻', '强高', 
            '木乃伊', '窒息', 'DID', 'cosplay', '洛丽塔', 'jk', '萌妹子', 
            '泳装', '吊带', '捆绑', '束缚', '绳艺', 'SM', '推特',
            '巨乳', '贫乳', '全裸', '半裸', '漏鲍', '自慰', '剧情', 
            '露出', '无码', '有码', '流出', '破解', '连裤袜', '高跟鞋',
            '网盘', '解压', '密码', '合集', '系列', '更新', '预告'
        }
        noise = {'7z', 'mp4', 'txt', 'zip', 'rar', 'http', 'https', 'com', 'org', 'net', 'Fantia', 'Second', 'Nov'}

        for row in rows:
            chat_name, sender_name, file_name, text_content = row
            text_content = text_content or ""
            file_name = file_name or ""
            
            # 1. 提取带标签的关键字
            tags = re.findall(r'#(\w+)', text_content)
            for tag in tags:
                if tag not in ignore_tags and len(tag) > 1 and not tag.isdigit():
                    self._add_candidate(tag, "Tag")

            # 2. 从特定前缀/括号结构提取
            # e.g. 【霜月shimo】
            brackets = re.findall(r'【(.*?)】|\[(.*?)\]', text_content)
            for b in brackets:
                val = b[0] or b[1]
                if val and not val.isdigit() and len(val) < 15:
                    self._add_candidate(val, "Brackets")

            # 3. 从文件名提取
            if file_name:
                match = re.search(r'^(.*?)@', file_name)
                if match:
                    self._add_candidate(match.group(1), "FilePrefix")

        # 过滤掉已经是已知实体的项
        known_entities = self.load_entities()
        known_set = set(known_entities.get('creators', []) + known_entities.get('actors', []))
        
        filtered_candidates = []
        for name, data in self.candidates.items():
            if name.lower() not in [k.lower() for k in known_set]:
                filtered_candidates.append({'name': name, 'count': data['count'], 'source': data['source']})
                
        # 按频次排序并取 Top 100
        total_list = sorted(filtered_candidates, key=lambda x: x['count'], reverse=True)[:100]
        
        # 分类
        tags_list = [c for c in total_list if 'Tag' in c['source']]
        other_list = [c for c in total_list if 'Tag' not in c['source']]
        
        # 写入候选池
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# 🎭 实体候选筛选池 (Interactive Pool)\n\n")
            f.write("此文件列出了系统在日志中发现的高频词汇。请直接在下方 **勾选** `[x]` 您认可的项，然后告知 AI。\n")
            f.write("AI 会读取此文件并将勾选项与沟通补充项一并录入 `entities.json`。\n\n")
            
            f.write("## 🏷️ 标签/关键词类 (Tags)\n")
            for c in tags_list:
                f.write(f"- [ ] `{c['name']}` (出现 {c['count']} 次)\n")
                
            f.write("\n## 📁 文件/括号提取类 (Other)\n")
            for c in other_list:
                f.write(f"- [ ] `{c['name']}` (出现 {c['count']} 次)\n")
                
        print(f"✅ 生成完毕！共提取到 {len(filtered_candidates)} 个新候选词，已输出已筛选 Top 到 {output_file}。")

    def _add_candidate(self, name, source):
        name = name.strip()
        if not name or len(name) < 2 or name.isdigit(): return
        noise = {'7z', 'mp4', 'txt', 'zip', 'rar', 'http', 'https', 'com'}
        if name in noise: return
        
        if name not in self.candidates:
            self.candidates[name] = {'count': 0, 'source': set()}
        self.candidates[name]['count'] += 1
        self.candidates[name]['source'].add(source)

    def apply_entities_and_index(self):
        """将 entities.json 的规则应用到 global_messages 中，更新 search_tags，并将 is_extracted 置为 1"""
        print("⚙️ 正在应用 entities.json 并更新数据库索引...")
        known_entities = self.load_entities()
        all_creators = known_entities.get('creators', [])
        all_actors = known_entities.get('actors', [])
        
        # 构建小写查找表以实现忽略大小写的匹配
        entity_map = {c.lower(): c for c in all_creators}
        for a in all_actors:
            entity_map[a.lower()] = a

        self.db.cursor.execute("SELECT chat_id, msg_id, file_name, text_content FROM global_messages WHERE is_extracted = 0")
        rows = self.db.cursor.fetchall()
        
        update_count = 0
        for row in rows:
            chat_id, msg_id, file_name, text_content = row
            text_content = text_content or ""
            file_name = file_name or ""
            
            combined_text = (file_name + " " + text_content).lower()
            
            found_tags = set()
            for lower_k, real_k in entity_map.items():
                if lower_k in combined_text:
                    found_tags.add(real_k)
                    
            tags_str = ",".join(found_tags) if found_tags else None
            
            self.db.cursor.execute('''
                UPDATE global_messages 
                SET search_tags = ?, is_extracted = 1 
                WHERE chat_id = ? AND msg_id = ?
            ''', (tags_str, chat_id, msg_id))
            
            update_count += 1
            
        self.db.conn.commit()
        
        # [NEW] 更新 entities.json 中的元数据：记录最后一次处理的全库总量
        self.db.cursor.execute("SELECT COUNT(*) FROM global_messages")
        total_count = self.db.cursor.fetchone()[0]
        self._update_metadata(total_count)
        
        print(f"✅ 更新完成！共打标并归档了 {update_count} 条新消息。")

    def ingest_checked_candidates(self, input_file='data/candidate_pool.md'):
        """读取 Markdown 文件，解析勾选的 [x] 项并存入 entities.json"""
        print(f"📥 正在从 {input_file} 提取勾选项...")
        if not os.path.exists(input_file):
            print("❌ 找不到候选池文件。")
            return
            
        checked_names = []
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                # 匹配 - [x] `Name`
                match = re.search(r'-\s*\[x\]\s*`(.+?)`', line)
                if match:
                    checked_names.append(match.group(1))
                    
        if not checked_names:
            print("⚠️ 未发现勾选项。请确保您使用了小写 [x] 或大写 [X]。")
            # 兼容性检查 [X]
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
                matches = re.findall(r'-\s*\[X\]\s*`(.+?)`', content)
                checked_names.extend(matches)
        
        if not checked_names:
            print("❌ 依然未发现勾选项。")
            return
            
        print(f"✨ 发现 {len(checked_names)} 个勾选项：{checked_names}")
        
        # 默认归类到 creators (或提示用户，这里我们先默认加上，AI 之后可以手动调整分类)
        data = self.load_entities()
        existing = set(data.get('creators', []) + data.get('actors', []))
        
        added = 0
        for name in checked_names:
            if name not in existing:
                # 启发式归类：如果是 2-3 个汉字且在 Tags 中提取，暂归为 Creator (或让 agent 后面改)
                data['creators'].append(name)
                existing.add(name)
                added += 1
                
        if added > 0:
            with open(self.entities_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"✅ 成功将 {added} 个新实体同步至 entities.json。")
        else:
            print("ℹ️ 勾选项均已存在于实体列表中。")

    def _update_metadata(self, total_count):
        """更新 entities.json 的元数据"""
        try:
            data = self.load_entities()
            data['last_processed_count'] = total_count
            data['version'] = str(float(data.get('version', '1.0')) + 0.1)[:3]
            with open(self.entities_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 更新元数据失败: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="增量更新搜索数据库")
    parser.add_argument('--extract', action='store_true', help='提取新增候选词到 candidate_pool.md')
    parser.add_argument('--ingest', action='store_true', help='读取 candidate_pool.md 的勾选项并更新 entities.json')
    parser.add_argument('--apply', action='store_true', help='将 entities.json 的规则应用并打标到数据库新数据中')
    args = parser.parse_args()
    
    updater = SearchDatabaseUpdater()
    
    if args.extract:
        updater.extract_candidates()
    elif args.ingest:
        updater.ingest_checked_candidates()
    elif args.apply:
        updater.apply_entities_and_index()
    else:
        print("请指定参数: --extract, --ingest 或 --apply")
