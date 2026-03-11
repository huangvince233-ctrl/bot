import os
import sys
import re

# 工作流 Program 1 Part 2: AI 建议分类脚本
# 逻辑：读取 candidate_pool.md -> 调用 AI 进行初步分类建议 -> 写回表格

class AICandidateClassifier:
    def __init__(self, pool_path='docs/entities/candidate_pool.md'):
        self.pool_path = pool_path

    def parse_pool(self):
        if not os.path.exists(self.pool_path):
            return []
        
        with open(self.pool_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        candidates = []
        for line in lines:
            # 匹配表格行: | 排名 | 候选词 | 频次 | 类型 | 建议 | 确认 |
            match = re.search(r'\|\s*(\d+)\s*\|\s*`(.*?)`\s*\|\s*(\d+)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|', line)
            if match:
                candidates.append({
                    'rank': match.group(1),
                    'name': match.group(2),
                    'count': match.group(3),
                    'type': match.group(4),
                    'suggestion': match.group(5).strip(),
                    'confirmed': match.group(6).strip()
                })
        return candidates

    def generate_prompt(self, batch):
        """生成发送给 AI 的分类指令"""
        names = [c['name'] for c in batch]
        prompt = f"你是一个专业的分类助手。请根据以下词汇列表，判断它们在「成人/绳艺/模特资源」上下文中的分类：\n"
        prompt += "分类选项：\n"
        prompt += "- Creator (创作者/工作室/机构)\n"
        prompt += "- Actor (女m/模特/演员)\n"
        prompt += "- Tag (关键词/玩法/特征，如: 束缚, 黑丝)\n"
        prompt += "- Noise (无效词/广告/网络术语)\n\n"
        prompt += "待处理列表: " + ", ".join(names) + "\n\n"
        prompt += "请仅返回 JSON 格式，如: {\"词汇\": \"分类\"}"
        return prompt

    def update_pool(self, suggestions):
        """将 AI 建议写回文件"""
        if not os.path.exists(self.pool_path): return
        
        with open(self.pool_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        new_lines = []
        for line in lines:
            match = re.search(r'^\|\s*(\d+)\s*\|\s*`(.*?)`\s*\|', line)
            if match:
                rank = match.group(1)
                name = match.group(2)
                if name in suggestions:
                    # 替换建议列 (倒数第二列)
                    parts = line.split('|')
                    if len(parts) >= 7:
                        parts[5] = f" {suggestions[name]} "
                        line = "|".join(parts)
            new_lines.append(line)
            
        with open(self.pool_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        print(f"✅ 已更新 {len(suggestions)} 条分类建议。")

if __name__ == "__main__":
    classifier = AICandidateClassifier()
    # 示例用法：
    # batch = classifier.parse_pool()[:50]
    # prompt = classifier.generate_prompt(batch)
    # response = call_ai(prompt) 
    # classifier.update_pool(response_json)
    print("🚀 AI 分类脚本已就位。请配置 API 或由 Agent 直接处理候选池。")
