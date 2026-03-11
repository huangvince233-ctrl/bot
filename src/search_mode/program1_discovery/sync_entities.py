import os
import sys
import re
import json

# 工作流 Program 1c: 实体同步工具 (UI 2.0 适配版)
# 逻辑：读取 8 列表格中的多列复选框，同步至对应分类

class EntitySyncer:
    def __init__(self, pool_dir, entities_path, current_md):
        self.pool_dir = pool_dir
        self.entities_path = entities_path
        self.current_md = current_md

    def load_entities(self):
        if os.path.exists(self.entities_path):
            with open(self.entities_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 自动迁移旧版关键词列表 -> 字典结构 [Migrate v1.2 -> v2.0]
                if isinstance(data.get('keywords'), list):
                    print("🔄 检测到旧版关键词列表，正在迁移到「未分类」组...")
                    data['keywords'] = {"未分类": data['keywords']}
                return data
        return {"version": "2.0", "creators": [], "actors": [], "keywords": {"未分类": []}, "noise": []}

    def save_entities(self, data):
        os.makedirs(os.path.dirname(self.entities_path), exist_ok=True)
        with open(self.entities_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def sync_from_pool(self):
        if not os.path.exists(self.pool_dir):
            print(f"❌ 找不到候选池目录: {self.pool_dir}")
            return
            
        # 使用与 server.py 对齐的增强版正则
        # 格式: 1. ` Word ` —— [ ] CREATOR | [ ] ACTOR | [ ] TAG(Category) | [ ] NOISE
        pattern = re.compile(
            r'\d+\.\s*`\s*(.*?)\s*`\s*——\s*\[(.)\]\s*CREATOR\s*\|\s*\[(.)\]\s*ACTOR\s*\|\s*\[(.)\]\s*TAG(?:\((.*?)\))?\s*\|\s*\[(.)\]\s*NOISE'
        )
        
        extracted_data = []
        # 遍历所有分卷文件
        for filename in sorted(os.listdir(self.pool_dir)):
            if filename.startswith("candidate_pool_part_") and filename.endswith(".md"):
                file_path = os.path.join(self.pool_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    matches = pattern.findall(content)
                    for name, c, a, t, cat, n in matches:
                        # 检查是否有任何一项被勾选 (x)
                        chosen_cat = cat.strip() if cat and cat.strip() else "未分类"
                        if 'x' in (c + a + t + n).lower():
                            extracted_data.append((
                                name.strip(), 
                                c.lower() == 'x', 
                                a.lower() == 'x', 
                                t.lower() == 'x', 
                                n.lower() == 'x',
                                chosen_cat
                            ))
        
        entities = self.load_entities()
        if not extracted_data:
            print("⚠️ 未发现已勾选 [x] 的新项目。")
            self.export_markdown(entities)
            return

        added_count = 0
        for name, is_creator, is_actor, is_tag, is_noise, category in extracted_data:
            # 逻辑：Noise 优先，且互斥
            if is_noise:
                if 'noise' not in entities: entities['noise'] = []
                if name not in entities['noise']:
                    entities['noise'].append(name)
                    added_count += 1
                    print(f"🚫 已拉黑 (Noise): {name}")
                continue
            
            # 非 Noise：处理 C/A/T (支持身兼数职)
            mapping = [
                (is_creator, 'creators', 'CREATOR'),
                (is_actor, 'actors', 'ACTOR'),
                (is_tag, 'keywords', 'TAG')
            ]
            
            for is_selected, key, label in mapping:
                if is_selected:
                    # 查重逻辑适配
                    if key == 'keywords':
                        # 优先使用分拣时带入的分类，否则使用“未分类”
                        if category not in entities['keywords']: entities['keywords'][category] = []
                        target_list = entities['keywords'][category]
                    else:
                        if key not in entities: entities[key] = []
                        target_list = entities[key]
                    
                    exists = False
                    for item in target_list:
                        check_name = item['name'] if isinstance(item, dict) else item
                        if check_name == name:
                            exists = True
                            break
                    
                    if not exists:
                        target_list.append({"name": name, "aliases": []})
                        added_count += 1
                        print(f"➕ 已录入: [{label}] {name}")

        if added_count > 0:
            self.save_entities(entities)
            print(f"✅ 同步完成！共新增 {added_count} 条规则到 entities.json")
        else:
            print("ℹ️ 所有勾选项已在实体库中，无需更新。")
            
        # 每次运行结束都重新生成全景视图
        self.export_markdown(entities)

    def export_markdown(self, entities):
        """将 JSON 字典导出为易于阅读的 Markdown 可视化页面"""
        md_path = self.current_md
        os.makedirs(os.path.dirname(md_path), exist_ok=True)
        
        md_content = "# 📚 全局已确认实体与噪声字典 (entities.json 可视化)\n\n"
        md_content += "这是当前系统正在使用的核心标签库与拦截名单。所有列在这里的条目都不会再向候选池推送。\n\n"
        
        categories = [
            ("🎬 创作者体系 (Creators)", entities.get('creators', [])),
            ("👠 主要人物 (Actors)", entities.get('actors', []))
        ]
        
        for title, items in categories:
            md_content += f"## {title}\n"
            if not items:
                md_content += "> 暂无数据\n\n"
            else:
                md_content += "| 核心名称 | 同义词/别名 |\n| :--- | :--- |\n"
                for item in items:
                    name = item.get('name', item) if isinstance(item, dict) else item
                    aliases = ", ".join(item.get('aliases', [])) if isinstance(item, dict) else ""
                    md_content += f"| `{name}` | {aliases} |\n"
                md_content += "\n"

        # Keywords 按分组展示 [NEW]
        md_content += "## 🏷️ 关键词分类库 (Keywords)\n"
        kw_data = entities.get('keywords', {})
        if not kw_data:
            md_content += "> 暂无数据\n\n"
        else:
            for group_name, items in kw_data.items():
                md_content += f"### 📁 {group_name}\n"
                md_content += "| 关键词 | 别名 |\n| :--- | :--- |\n"
                for item in items:
                    name = item.get('name', item) if isinstance(item, dict) else item
                    aliases = ", ".join(item.get('aliases', [])) if isinstance(item, dict) else ""
                    md_content += f"| `{name}` | {aliases} |\n"
                md_content += "\n"
                
        md_content += "## 🚫 噪声与拉黑词库 (Noise)\n"
        noise_list = entities.get('noise', [])
        if not noise_list:
            md_content += "> 暂无数据\n"
        else:
            md_content += "> 包含被归类为无效、广告群组等无检索意义的词汇。\n\n"
            noise_display = "`, `".join(noise_list)
            md_content += f"`{noise_display}`\n"
            
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        print(f"📄 已同步生成字典可视化视图：{md_path}")


if __name__ == "__main__":
    import argparse
    import sys
    
    # 路径修复逻辑
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    sys.path.append(project_root)
    import src.utils.config as cfg
    
    parser = argparse.ArgumentParser(description="同步分拣结论")
    parser.add_argument('--bot', type=str, default='tgporncopilot', help='指定触发的 Bot 配置')
    args = parser.parse_args()
    
    CONFIG = cfg.get_bot_config(args.bot)
    pool_dir = os.path.join(project_root, CONFIG['candidates_dir_docs'])
    entities_path = os.path.join(project_root, CONFIG['currententities_dir_data'], 'entities.json')
    current_md = os.path.join(project_root, CONFIG['currententities_dir_docs'], 'current_entities.md')
    
    print(f"[{CONFIG['app_name']}] 正在解析并同步分拣结论...")
    syncer = EntitySyncer(pool_dir=pool_dir, entities_path=entities_path, current_md=current_md)
    syncer.sync_from_pool()
