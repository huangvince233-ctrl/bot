import os
import sys
import re
import json
import time
from collections import Counter

import jieba
import jieba.posseg as pseg

# 工作流 Program 1: 全局文本分词与频次统计 (NLP 引擎升级版)
# 逻辑：NLP分词提取高价值词性 -> 聚类统计 -> 生成候选池 -> (后续) 人工/AI 分类

def initialize_jieba_dict(entities_path=None):
    """将现有的已知实体加入词典，防止被错误切分"""
    if entities_path is None:
        entities_path = 'data/entities/tgporncopilot_entities.json'
    print("🧠 正在初始化 NLP 引擎字典...")
    if os.path.exists(entities_path):
        try:
            with open(entities_path, 'r', encoding='utf-8') as f:
                edata = json.load(f)
                for cat in ['creators', 'actors', 'keywords', 'noise']:
                    for item in edata.get(cat, []):
                        if isinstance(item, dict):
                            jieba.add_word(item['name'])
                            for alias in item.get('aliases', []):
                                jieba.add_word(alias)
                        else:
                            jieba.add_word(item)
            print("✅ NLP 字典加载完成 (已同步本地已知实体与黑名单)")
        except:
            print("⚠️ NLP 字典加载失败，将使用默认分词模型")

def tokenize(text):
    """
    NLP 深层特征提取逻辑：
    1. 提取 #标签 和 【括号内容】(最高优先级)
    2. 使用 Jieba 进行词性标注分词
    3. 仅保留名词(n/nr/ns/nt/nz)、动名词(vn)、形容词(a)、外文符号(eng) 等具有业务价值的词汇
    """
    tokens = []
    
    # 提取强特征词
    tags = re.findall(r'#(\w+)', text)
    tokens.extend([('Tag', t) for t in tags])
    
    brackets = re.findall(r'【(.*?)】|\[(.*?)\]', text)
    for b in brackets:
        val = (b[0] or b[1]).strip()
        if val: tokens.append(('Bracket', val))
        
    # 移除已提取的部分以及网址链接等噪声，避免干扰 NLP
    clean_text = re.sub(r'#\w+|【.*?】|\[.*?\]|http[s]?://\S+', ' ', text)
    # 进一步移除所有标点符号，防止 jieba 切分出无意义标点
    clean_text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', ' ', clean_text)
    
    # 使用 jieba.posseg 标注词性
    words = pseg.cut(clean_text)
    
    # 定义有价值的词性集合 (排除连词、介词、标点、助词等虚词)
    # n: 名词, nr: 人名, ns: 地名, nt: 机构团体, nz: 其他专名
    # a: 形容词, vn: 名动词, eng: 英文字符串 (很多厂牌是英文)
    valid_flags = {'n', 'nr', 'ns', 'nt', 'nz', 'a', 'vn', 'eng', 'i'}
    
    for word, flag in words:
        word = word.strip()
        # 过滤单字词或过长无意义词
        if len(word) < 2 or len(word) > 10: continue
        
        # 英文要求至少长于2
        if flag == 'eng' and len(word) < 3: continue
        
        # 判断词性
        if any(flag.startswith(f) for f in valid_flags):
            tokens.append(('NLP_Term', word))
            
    return tokens

def scan_backups(backup_dir, managed_folders=None, progress_file=None):
    counter = Counter()
    word_samples = {}  # word -> [sample_text, ...]  最多保留 10 条原文
    total_msgs = 0
    
    print(f"📂 正在递归扫描目录: {backup_dir} ...")
    if managed_folders:
        print(f"🎯 限制管辖范围: {managed_folders}")
    
    # [NEW] 预扫描文件列表，用于计算进度
    all_json_files = []
    for root, dirs, files in os.walk(backup_dir):
        if managed_folders:
            rel_path = os.path.relpath(root, backup_dir)
            if rel_path != '.':
                top_folder = rel_path.split(os.sep)[0]
                if top_folder not in managed_folders:
                    continue
        for file in files:
            if file.endswith('.json') and not file.startswith('metadata'):
                all_json_files.append(os.path.join(root, file))
    
    import time
    last_update = 0
    total_files = len(all_json_files)
    
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    # progress_file 现由外部传入
    if progress_file:
        os.makedirs(os.path.dirname(progress_file), exist_ok=True)

    max_mtime = 0
    for idx, path in enumerate(all_json_files):
        # 记录最新文件的时间戳
        try:
            mtime = os.path.getmtime(path)
            if mtime > max_mtime:
                max_mtime = mtime
        except: pass

        # [FIX] 限制写入频率，避免磁盘负载过大导致锁竞争
        now = time.time()
        if idx % 50 == 0 or now - last_update > 2:
            try:
                with open(progress_file, 'w', encoding='utf-8') as pf:
                    json.dump({
                        'current_file': os.path.basename(path),
                        'files_done': idx,
                        'total_files': total_files,
                        'total_msgs': total_msgs,
                        'status': 'scanning',
                        'timestamp': now,
                        'max_mtime': max_mtime
                    }, pf)
                last_update = now
            except: pass

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                messages = data if isinstance(data, list) else data.get('messages', [])
                
                for msg in messages:
                    if not isinstance(msg, dict): continue
                    total_msgs += 1
                    text = f"{msg.get('text', '')} {msg.get('caption', '')} {msg.get('file_name', '')}"
                    
                    # 进行分词
                    msg_tokens = tokenize(text)
                    for t_type, val in msg_tokens:
                        counter[f"{val}|{t_type}"] += 1
                        # 采集原文样本 (每词最多 10 条)
                        if val not in word_samples:
                            word_samples[val] = []
                        if len(word_samples[val]) < 10:
                            sample = text.strip()[:300]
                            if sample and sample not in word_samples[val]:
                                word_samples[val].append(sample)
        except:
            pass
    
    # 扫描完成标志
    if progress_file:
        try:
            with open(progress_file, 'w', encoding='utf-8') as pf:
                json.dump({'status': 'completed', 'total_msgs': total_msgs, 'max_mtime': max_mtime}, pf)
        except: pass

    return counter, total_msgs, word_samples, max_mtime

def save_candidates(counter, output_dir, entities_path=None, word_samples=None, metadata=None, json_output_dir=None):
    if entities_path is None:
        entities_path = 'data/entities/tgporncopilot_entities.json'
    # 加载已分类实体和黑名单以便排除
    exclude_set = set()
    if os.path.exists(entities_path):
        try:
            with open(entities_path, 'r', encoding='utf-8') as f:
                edata = json.load(f)
                # 收集所有已存在的名称和别名
                for cat in ['creators', 'actors', 'keywords']:
                    for item in edata.get(cat, []):
                        if isinstance(item, dict):
                            exclude_set.add(item['name'].lower())
                            for alias in item.get('aliases', []):
                                exclude_set.add(alias.lower())
                        else:
                            exclude_set.add(item.lower())
                # 收集噪点/黑名单
                for n in edata.get('noise', []):
                    exclude_set.add(n.lower())
        except:
            pass

    # 过滤硬件静态噪声 (大幅扩充)
    ignore = {
        '频道', '加入', '讨论', '关注', '来自', '客服', '联系', '招商', '免费', '试看', 
        '系列', '更新', '视频', '文件', 'https', 'http', 'com', 'html', 'php', 'aspx',
        'news', 'instant', 'chat', 'good', 'bad', 'out', 'www', 'net', 'None', 'mp', 'none',
        '00', '000', '008', '0元', '一个', '更多', '自己', '第一', '可以', '支持', 
        '收藏', '喜欢', '下载', '搜索', '聚合', '资讯', '讨论群', '热门', '点击', '跳转', 
        '预览', '最新', '联系客服', '最新联系', '奖励', '有效', '分享', '参考', '仅需', 
        '可能', '存在', '回国', '专线', '体育', '足球', '推荐', '指数', '本站', '永久', 
        '公告', '提醒', '通知', '点击查看', '立即加入', '置顶', '推荐关注', '转发', '赞助',
        '广告', '全网', '独家', '资源', '高清', '画质', '速度', '稳定', '流畅', '节点',
        '会员', '充值', '余额', '提现', '赚钱', '官方', '直录', '自拍', '探花', '模特',
        'me', 'IMG', 'txt', 'TG频道', '点击关注', '来自', '链接', '详情', '点击', '跳转',
        '免费试看', '分类列表', '代理部', '有效新', '效新增', '教视频', '调教视', '效新',
        'kyty', 'TBBAD', 'chigua', 'bdzs0', 'bzha', 'HTHUB', 'jnvip00', 'azv', 'mp4',
        'Lvv2聚合资讯频道', 'Lvv2聚合资讯讨论群', '我的', '没有', '体验', '一个', '很多',
        '演员', '作品', '合集', '整理', '发布', '原创', '投稿', '精选', '重磅', '整理'
    }
    
    # 整理结果库
    seen_names = {}
    
    # 排序并应用排除逻辑 (处理所有词汇)
    for key, count in counter.most_common():
        if count < 5: continue # 过滤低频词
        if '|' not in key: continue
        
        name, t_type = key.rsplit('|', 1)
        name_lower = name.lower()
        
        # 核心排除逻辑：忽略列表 OR 已分类/噪点库 OR 长度过滤 OR 纯数字/符号
        if name_lower in ignore or name_lower in exclude_set or len(name) < 2:
            continue
        if re.match(r'^[0-9\W_]+$', name) or name.startswith('['):
            continue
        if re.match(r'^[a-z0-0]{5,}$', name_lower) and not any(c in 'aeiou' for c in name_lower):
            continue
        
        if name not in seen_names:
            seen_names[name] = {'count': count, 'types': {t_type}}
        else:
            seen_names[name]['count'] += count
            seen_names[name]['types'].add(t_type)

    # 再次排序输出
    sorted_final = sorted(seen_names.items(), key=lambda x: x[1]['count'], reverse=True)

    # 分卷输出
    items_per_file = 2000
    total_items = len(sorted_final)
    
    # MD 输出目录处理
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    else:
        # 仅清理特定 Bot 的候选文件
        for f in os.listdir(output_dir):
            if f.startswith("candidate_pool_part_") and f.endswith(".md"):
                os.remove(os.path.join(output_dir, f))

    # JSON 输出目录处理 [NEW]
    if json_output_dir:
        if not os.path.exists(json_output_dir):
            os.makedirs(json_output_dir, exist_ok=True)
        else:
            for f in os.listdir(json_output_dir):
                if f.startswith("candidate_pool_part_") and f.endswith(".json"):
                    os.remove(os.path.join(json_output_dir, f))

    total_files = (total_items + items_per_file - 1) // items_per_file

    for file_index in range(total_files):
        start_idx = file_index * items_per_file
        chunk = sorted_final[start_idx:min(start_idx + items_per_file, total_items)]
        
        part_path_md = os.path.join(output_dir, f"candidate_pool_part_{file_index+1}.md")
        part_data = []
        
        with open(part_path_md, 'w', encoding='utf-8') as f_md:
            f_md.write(f"# 🔬 新发现实体池 (Program 1B) - Part {file_index+1}/{total_files}\n\n")
            f_md.write(f"此池已过滤掉 `entities.json` (v{_DICT_VERSION}) 中已存在的词条。每卷 {items_per_file} 条。\n\n")
            f_md.write("## 📥 待分拣词汇列表\n\n")
            f_md.write("| ID | 候选词 (Candidate Word) | 选择操作 (Creator | Actor | Tag | Noise) | 原始数据参考 (频次/类型) |\n")
            f_md.write("| :--- | :--- | :--- | :--- |\n")
            
            for i, (name, info) in enumerate(chunk):
                item_id = start_idx + i + 1
                types_str = ", ".join(info['types'])
                f_md.write(f"| {item_id} | ` {name} ` | [ ] CREATOR \| [ ] ACTOR \| [ ] TAG \| [ ] NOISE | (频次: {info['count']}, 来源: {types_str}) |\n")
                
                # 记录 JSON 结构
                part_data.append({
                    'id': item_id,
                    'word': name,
                    'count': info['count'],
                    'types': list(info['types'])
                })

        if json_output_dir:
            part_path_json = os.path.join(json_output_dir, f"candidate_pool_part_{file_index+1}.json")
            with open(part_path_json, 'w', encoding='utf-8') as f_json:
                json.dump(part_data, f_json, ensure_ascii=False, indent=2)

    # 保存 Samples 和 Metadata 到 docs (维持 UI 兼容性)
    if word_samples:
        samples_path = os.path.join(output_dir, 'candidate_samples.json')
        with open(samples_path, 'w', encoding='utf-8') as f:
            json.dump(word_samples, f, ensure_ascii=False)
        print(f"  Saved {len(word_samples)} word samples to candidate_samples.json")

    if metadata:
        meta_path = os.path.join(output_dir, 'candidate_metadata.json')
        try:
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            print(f"  Saved metadata to candidate_metadata.json")
            if json_output_dir:
                with open(os.path.join(json_output_dir, 'candidate_metadata.json'), 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
        except: pass

    print(f"✅ 处理完成。共计提取 {total_items} 个新词。")

_DICT_VERSION = "0.0"

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--bot', type=str, default='tgporncopilot')
    parser.add_argument('--backup-id', type=str, default='NONE')
    parser.add_argument('--progress-file', type=str, default=None)
    parser.add_argument('--test', action='store_true')
    args = parser.parse_args()

    # 修正路径
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    sys.path.append(project_root)
    from src.utils.config import get_bot_config
    
    CONFIG = get_bot_config(args.bot)
    BACKUP_DIR = os.path.join(project_root, 'data/archived/backups')
    OUTPUT_DIR = os.path.join(project_root, CONFIG.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates'))
    DATA_OUTPUT_DIR = os.path.join(project_root, CONFIG.get('candidates_dir_data', 'data/entities/tgporncopilot/candidates'))
    ENTITIES_PATH = os.path.join(project_root, CONFIG.get('currententities_dir_data', 'data/entities/tgporncopilot/currententities'), 'entities.json')
    MANAGED_FOLDERS = CONFIG.get('managed_folders', [])

    if os.path.exists(ENTITIES_PATH):
        try:
            with open(ENTITIES_PATH, 'r', encoding='utf-8') as f:
                _DICT_VERSION = json.load(f).get('version', '1.0')
        except: pass

    print(f"[*] Bot: {CONFIG['app_name']} | Dict Version: {_DICT_VERSION}")
    
    # [NEW] 及早创建进度文件，避免加载词典时 Bot 认为无响应
    progress_file = args.progress_file or os.path.join(project_root, 'data/temp/extractor_progress.json')
    os.makedirs(os.path.dirname(progress_file), exist_ok=True)
    try:
        with open(progress_file, 'w', encoding='utf-8') as pf:
            json.dump({'status': 'initializing', 'bot': args.bot}, pf)
    except: pass

    initialize_jieba_dict(ENTITIES_PATH)
    counts, total, samples, max_mtime = scan_backups(BACKUP_DIR, managed_folders=MANAGED_FOLDERS, progress_file=progress_file)
    
    metadata = {
        'bot_name': args.bot,
        'latest_backup_id': args.backup_id,
        'total_msgs': total,
        'max_mtime': max_mtime,
        'scan_time': time.time(),
        'candidate_count': len(counts)
    }
    save_candidates(counts, OUTPUT_DIR, ENTITIES_PATH, word_samples=samples, metadata=metadata, json_output_dir=DATA_OUTPUT_DIR)
