import os
import sys
import json
import sqlite3
import re

# 增加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
from src.db import Database

class AITagger:
    def __init__(self, db_path='data/copilot.db', entities_path='data/entities/tgporncopilot_entities.json'):
        self.db = Database(db_path)
        self.entities_path = entities_path
        self.known_entities = self._load_entities()

    def _load_entities(self):
        if os.path.exists(self.entities_path):
            with open(self.entities_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"creators": [], "actors": []}

    def _get_canonical_name(self, name, etype):
        """将提取的名字归一化为主名"""
        entity_list = self.known_entities.get(etype, [])
        name_lower = name.lower().strip()
        for item in entity_list:
            if isinstance(item, dict):
                main_name = item['name']
                if main_name.lower() == name_lower:
                    return main_name
                for ali in item.get('aliases', []):
                    if ali.lower() == name_lower:
                        return main_name
            else:
                if item.lower() == name_lower:
                    return item
        return name # 未命中映射，保留原名

    def construct_prompt(self, messages):
        """构建 Few-shot 提示词"""
        examples = """
案例 1:
文本: #白城 #人妻 #七宗罪 #剧本杀 #剧情演绎 #短片 #调教片 第十五部《人妻惩戒所》小南篇... 白城是 创作者 小南是女m
结果: {"creator": "白城", "actor": "小南", "keywords": ["人妻", "七宗罪", "剧本杀", "剧情演绎", "短片", "调教片"]}

案例 2:
文本: #面具男 #天津 #电控装置 #初体验 #k9 #人形犬 #bondage #rope 绝色女声伪娘骚狗乸花花电击控制改造训练计划... 这一条 面具男 是创作者
结果: {"creator": "面具男", "actor": "花花", "keywords": ["天津", "电控装置", "初体验", "k9", "人形犬", "bondage", "rope", "电击控制", "改造训练"]}

案例 3:
文本: #嗷大喵 #角色扮演 #私人定制 #捆绑 -《二次元少女系》-客户定拍-第二集-刻晴cos+捂晕日式紧缚+胸缚+丝袜堵嘴... 嗷大喵是 创作者
结果: {"creator": "嗷大喵", "actor": "刻晴", "keywords": ["角色扮演", "私人定制", "捆绑", "二次元", "cos", "日式紧缚", "胸缚", "丝袜堵嘴"]}
"""
        prompt = f"你是一个专业的 Telegram 资源分类专家。请根据以下案例模式，解析待处理的消息，提取 'creator' (创作者), 'actor' (女m/模特/主角) 和 'keywords' (核心关键词)。\n{examples}\n\n待处理消息:\n"
        for i, msg in enumerate(messages):
            prompt += f"消息 {i+1} (ID: {msg['msg_id']}):\n文件名: {msg['file_name']}\n文本: {msg['text_content']}\n---\n"
        
        prompt += "\n直接以 JSON 数组形式返回结果，每个对象包含 msg_id, creator, actor, keywords。不要包含任何多余文字。"
        return prompt

    def process_pending_messages(self, limit=10):
        """读取未处理消息并调用 AI (这里预留接口)"""
        self.db.cursor.execute("SELECT chat_id, msg_id, file_name, text_content, creator, actor FROM global_messages WHERE is_extracted = 0 LIMIT ?", (limit,))
        rows = self.db.cursor.fetchall()
        if not rows:
            print("没有待处理的消息。")
            return
        
        messages = []
        for row in rows:
            messages.append({
                'chat_id': row[0],
                'msg_id': row[1],
                'file_name': row[2] or "",
                'text_content': row[3] or "",
                'existing_creator': row[4],
                'existing_actor': row[5]
            })
            
        prompt = self.construct_prompt(messages)
        print("--- 已生成 AI 提示词 ---")
        print(prompt)
        print("--- 提示词结束 ---")
        
        # TODO: 集成真正的 API 调用
        # print("正在调用 AI API...")
        # response = call_llm(prompt) 
        # self.apply_results(json.loads(response))
        
        print("\n[TIP] 请将上述提示词发送给 AI，并将返回的 JSON 结果告诉我，我来为您批量写入数据库并同步实体库。")

    def apply_results(self, results):
        """将 AI 结果写回数据库并归一化"""
        for res in results:
            msg_id = res['msg_id']
            creator = self._get_canonical_name(res.get('creator', ''), 'creators')
            actor = self._get_canonical_name(res.get('actor', ''), 'actors')
            keywords = ",".join(res.get('keywords', []))
            
            # TODO: 更新数据库并设置 is_extracted = 1
            print(f"✔️ 已处理消息 {msg_id}: {creator} / {actor} / [{keywords}]")

if __name__ == "__main__":
    tagger = AITagger()
    tagger.process_pending_messages(limit=5)
