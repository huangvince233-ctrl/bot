import os
import re
import glob
from src.db import Database

class KeywordExtractor:
    def __init__(self, db_path='data/copilot.db'):
        self.db = Database(db_path)
        self.entities = []

    def extract_from_file(self, file_path):
        """从单个备份 MD 文件中提取关键字"""
        print(f"🔍 正在解析: {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"❌ 读取文件失败 {file_path}: {e}")
            return

        # 1. 提取带标签的关键字 (e.g., #窒物者)
        # 排除掉常见的非人称标签
        ignore_tags = {
            'BDSM', '调教', '萝莉', '国产', '黑丝', '白丝', '羞耻', '强高', 
            '木乃伊', '窒息', 'DID', 'cosplay', '洛丽塔', 'jk', '萌妹子', 
            '泳装', '吊带', '捆绑', '束缚', '绳艺', 'SM', '推特', 
            '历史备份归档', '全局统计汇总', '编号概览', '对话资源号范围', '发布源', '文件名'
        }
        
        tags = re.findall(r'#(\w+)', content)
        for tag in tags:
            # 排除纯数字、单字、以及忽略列表中的词
            if tag not in ignore_tags and len(tag) > 1 and not tag.isdigit():
                self.add_candidate(tag, "Tag")

        # 2. 提取表格/结构化字段
        # 编号   88
        # 名称   窒物者 我是小G呀
        # id   LoerAngela 
        fields = re.findall(r'> 名称\s+(.*)', content)
        for name in fields:
            name = name.strip()
            if name:
                # 进一步拆分，如果是 "A B" 形式，通常两个都是名字
                parts = re.split(r'[\s/／|]+', name)
                for p in parts:
                    if len(p) > 1:
                        self.add_candidate(p.strip(), "名称")

        ids = re.findall(r'> id\s+(.*)', content)
        for id_val in ids:
            id_val = id_val.strip()
            if id_val and len(id_val) > 1:
                self.add_candidate(id_val, "ID")

        # 3. 从文件名提取
        # - **文件名**: `自由之翼@ZYZY-ZDZ编号088 (1).mp4`
        filenames = re.findall(r'- \*\*文件名\*\*: `(.*?)`', content)
        for fname in filenames:
            # 提取前缀或特定模式
            # e.g. 自由之翼@ZYZY
            match = re.search(r'^(.*?)@', fname)
            if match:
                self.add_candidate(match.group(1), "文件名前缀")
            
            # e.g. 霜月shimo
            match_shimo = re.search(r'([^\w]?)([一-龥]{2,}|\w{3,})(shimo|SHIMO)', fname)
            if match_shimo:
                self.add_candidate(match_shimo.group(2) + match_shimo.group(3), "特征匹配")

    def add_candidate(self, name, source_type):
        """记录候选人到内存列表"""
        # 清理名字
        name = name.strip().replace('#', '')
        if not name or len(name) < 2 or name.isdigit(): return
        
        # 排除已知的干扰词
        noise = {'7z', 'mp4', 'txt', 'zip', 'rar', 'http', 'https', 'com', 'org', 'net', 'Fantia', 'Second', 'Nov'}
        if name in noise: return

        # 大小写不敏感去重
        name_lower = name.lower()
        for item in self.entities:
            if item['name'].lower() == name_lower:
                item['count'] += 1
                return
        
        self.entities.append({
            'name': name,
            'type': "Creator/Actor",
            'source': source_type,
            'count': 1
        })

    def scan_backups(self, base_dir='docs/archived/backups'):
        """扫描所有备份目录"""
        search_pattern = os.path.join(base_dir, '**', '*.md')
        files = glob.glob(search_pattern, recursive=True)
        
        for f in files:
            if os.path.isfile(f) and 'subscriptions.md' not in f:
                self.extract_from_file(f)

    def save_to_db(self):
        """将候选人存入数据库待审池"""
        print(f"💾 正在将 {len(self.entities)} 个候选项目存入数据库...")
        for item in self.entities:
            self.db.add_entity_candidate(item['name'], item['type'], item['count'])
        print("✅ 存储完成。")

if __name__ == "__main__":
    extractor = KeywordExtractor()
    extractor.scan_backups()
    extractor.save_to_db()
    
    # 打印前 20 个高频候选用于预览
    top_entities = sorted(extractor.entities, key=lambda x: x['count'], reverse=True)[:20]
    print("\n📊 --- 前 20 个高频候选项目 ---")
    for e in top_entities:
        print(f"[{e['count']}] {e['name']} ({e['source']})")
