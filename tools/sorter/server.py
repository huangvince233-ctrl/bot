#!/usr/bin/env python3
"""
P1.5 候选词分拣工具 - 本地 Flask 服务
用法: python tools/sorter/server.py --bot tgporncopilot
访问: http://localhost:8765
"""
import os
import sys
import json
import re
import subprocess
import threading
import time
import webbrowser
import argparse
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory, Response

# 路径修复
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.utils.config import get_bot_config

app = Flask(__name__)

# ─── 全局状态 ───────────────────────────────────────────────
CONFIG = {}
CANDIDATES_CACHE = []       # [{word, count, types, decided: [], category: ""}]
SAMPLES_CACHE = {}          # {word: [sample_text, ...]}
CATEGORIES = ["未分类"]      # 预置分类
PIPELINE_LOG = []           # 流水线日志（SSE 推送）
pipeline_lock = threading.Lock()
pipeline_active_lock = threading.Lock() # 流水线互斥锁
CANDIDATES_CACHE_DECISIONS = {} # 暂存用户的分拣决策 {word: {cats, category}}
PIPELINE_LOG_FILE = PROJECT_ROOT / 'data/temp' / f"pipeline_status_{CONFIG.get('app_name', 'tgporncopilot')}.json"

def _save_pipeline_log():
    """将流水线日志持久化到磁盘，供 Bot 进程监听"""
    try:
        os.makedirs(PIPELINE_LOG_FILE.parent, exist_ok=True)
        PIPELINE_LOG_FILE.write_text(json.dumps(PIPELINE_LOG, ensure_ascii=False, indent=2), encoding='utf-8')
    except: pass

def log_pipeline(data):
    """记录流水线日志并固化到磁盘"""
    with pipeline_lock:
        PIPELINE_LOG.append(data)
        _save_pipeline_log()

# 移除旧的 Staging 函数

def update_md():
    """Placeholder: 实体变更后的 MD 同步（当前为空实现）"""
    pass

def get_all_lexicon_words():
    """从 entities.json 中提取所有已录入的词汇（包括别名）用于去重"""
    entities_path = PROJECT_ROOT / CONFIG.get('currententities_dir_data', 'data/entities/tgporncopilot/currententities') / 'entities.json'
    words = set()
    if not entities_path.exists(): return words
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        for cat in ['creators', 'actors']:
            for item in data.get(cat, []):
                name = item.get('name') if isinstance(item, dict) else item
                if name: words.add(name.lower())
                if isinstance(item, dict):
                    for alias in item.get('aliases', []):
                        words.add(alias.lower())
        
        kws = data.get('keywords', {})
        if isinstance(kws, list): kws = {"未分类": kws}
        for cat_list in kws.values():
            for item in cat_list:
                name = item.get('name') if isinstance(item, dict) else item
                if name: words.add(name.lower())
                if isinstance(item, dict):
                    for alias in item.get('aliases', []):
                        words.add(alias.lower())
        
        for n in data.get('noise', []):
            words.add(n.lower())
    except: pass
    return words

# ─── 启动时加载候选词 ────────────────────────────────────────
def load_candidates(config):
    """从 candidates_dir 下的 MD 文件加载候选词列表"""
    candidates_dir = PROJECT_ROOT / config['candidates_dir_docs']
    results = []
    seen = set()
    
    # 增强正则，支持表格格式 (P1B v2.0)
    # 格式: | ID | ` word ` | [ ] CREATOR \| [ ] ACTOR \| [ ] TAG \| [ ] NOISE | (频次: N, 来源: ...) |
    pattern_table = re.compile(
        r'\| \d+ \| `\s*(.*?)\s*` \| \[(.)\] CREATOR\s*\\\|\s*\[(.)\] ACTOR\s*\\\|\s*\[(.)\] TAG(?:\s*\((.*?)\))?\s*\\\|\s*\[(.)\] NOISE\s*\|'
        r'\s*\(频次:\s*(\d+),\s*来源:\s*(.*?)\)'
    )
    
    # 兼容旧单行列表格式
    pattern_list = re.compile(
        r'\d+\.\s*`\s*(.*?)\s*`\s*——\s*\[(.)\]\s*CREATOR\s*\|\s*\[(.)\]\s*ACTOR\s*\|\s*\[(.)\]\s*TAG(?:\s*\((.*?)\))?\s*\|\s*\[(.)\]\s*NOISE'
        r'.*?频次:\s*(\d+),\s*来源:\s*(.*?)\)'
    )
    
    if not candidates_dir.exists():
        return []

    for md_file in sorted(candidates_dir.glob("candidate_pool_part_*.md")):
        try:
            content = md_file.read_text(encoding='utf-8')
            # 尝试表格模式
            found_any = False
            for m in pattern_table.finditer(content):
                found_any = True
                word = m.group(1).strip()
                c, a, t, cat, n = m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)
                count = int(m.group(7))
                types = m.group(8).strip()
                if word not in seen:
                    seen.add(word)
                    decided = []
                    if c.lower() == 'x': decided.append('creator')
                    if a.lower() == 'x': decided.append('actor')
                    if t.lower() == 'x': decided.append('tag')
                    if n.lower() == 'x': decided.append('noise')
                    results.append({
                        'word': word, 'count': count,
                        'types': types, 'decided': decided,
                        'category': cat or "未分类",
                        '_file': md_file.name,
                        'samples': [],
                        'added_at': os.path.getmtime(md_file)
                    })
            
            # 如果表格模式没匹配到任何内容，回退到列表模式尝试
            if not found_any:
                for m in pattern_list.finditer(content):
                    word = m.group(1).strip()
                    c, a, t, cat, n = m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)
                    count = int(m.group(7))
                    types = m.group(8).strip()
                    if word not in seen:
                        seen.add(word)
                        decided = []
                        if c.lower() == 'x': decided.append('creator')
                        if a.lower() == 'x': decided.append('actor')
                        if t.lower() == 'x': decided.append('tag')
                        if n.lower() == 'x': decided.append('noise')
                        results.append({
                            'word': word, 'count': count,
                            'types': types, 'decided': decided,
                            'category': cat or "未分类",
                            '_file': md_file.name,
                            'samples': [],
                            'added_at': os.path.getmtime(md_file)
                        })
        except Exception as e:
            print(f"Error loading {md_file}: {e}")
            print(f"⚠️ 无法读取 {md_file}: {e}")
    
    # [NEW] 启动即过滤：如果词汇已在库中，自动剔除
    lexicon = get_all_lexicon_words()
    filtered = []
    for item in results:
        if item['word'].lower() in lexicon:
            continue
        filtered.append(item)
    
    if len(results) != len(filtered):
        print(f"🧹 已自动从候选池中剔除 {len(results) - len(filtered)} 个已存在于词库的词汇。")
        
    return filtered

def save_decision_to_md(file_path, word, decided, category):
    """实时将决策写入本地 MD 文件"""
    p = Path(file_path)
    if not p.exists(): return
    try:
        content = p.read_text(encoding='utf-8')
        lines = content.splitlines()
        new_lines = []
        found = False
        for line in lines:
            if not found and f"` {word} `" in line:
                c_box = "[x]" if "creator" in decided else "[ ]"
                a_box = "[x]" if "actor" in decided else "[ ]"
                t_box = "[x]" if "tag" in decided else "[ ]"
                n_box = "[x]" if "noise" in decided else "[ ]"
                tag_label = f"TAG({category})" if category and category != "未分类" else "TAG"
                
                line = re.sub(r'\[.\]\s*CREATOR', f'{c_box} CREATOR', line)
                line = re.sub(r'\[.\]\s*ACTOR', f'{a_box} ACTOR', line)
                line = re.sub(r'\[.\]\s*TAG(\(.*?\))?', f'{t_box} {tag_label}', line)
                line = re.sub(r'\[.\]\s*NOISE', f'{n_box} NOISE', line)
                found = True
            new_lines.append(line)
        if found:
            p.write_text("\n".join(new_lines) + "\n", encoding='utf-8')
    except Exception as e:
        print(f"Error saving to MD: {e}")


def load_categories(config):
    """从 entities.json 加载现有的关键词分类"""
    entities_path = PROJECT_ROOT / config['currententities_dir_data'] / 'entities.json'
    if entities_path.exists():
        try:
            data = json.loads(entities_path.read_text(encoding='utf-8'))
            kws = data.get('keywords', {})
            # 自动迁移旧版关键词列表 -> 字典结构 [Migrate v1.2 -> v2.0]
            if isinstance(kws, list):
                kws = {"未分类": kws}
            if isinstance(kws, dict):
                return sorted(list(kws.keys()))
        except:
            pass
    return ["未分类"]

def load_samples(config):
    """加载候选词的原文样本"""
    candidates_dir = PROJECT_ROOT / config['candidates_dir_docs']
    samples_path = candidates_dir / 'candidate_samples.json'
    if samples_path.exists():
        try:
            import json as _json
            return _json.loads(samples_path.read_text(encoding='utf-8'))
        except:
            pass
    return {}


def load_staging(config):
    """从 staging_decisions.json 加载暂存决策"""
    candidates_dir = PROJECT_ROOT / config.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates')
    staging_path = candidates_dir / 'staging_decisions.json'
    if staging_path.exists():
        try:
            return json.loads(staging_path.read_text(encoding='utf-8')) or []
        except:
            pass
    return []

def save_staging(config, entries):
    """保存暂存决策"""
    candidates_dir = PROJECT_ROOT / config.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates')
    staging_path = candidates_dir / 'staging_decisions.json'
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')


# ─── API 路由 ────────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory(Path(__file__).parent, 'index.html')

@app.route('/api/config')
def api_config():
    return jsonify({
        'bot': CONFIG.get('app_name', ''),
        'total': len(CANDIDATES_CACHE),
        'categories': CATEGORIES
    })

@app.route('/api/candidates')
def api_candidates():
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 50))
    q = request.args.get('q', '').strip().lower()
    
    data = CANDIDATES_CACHE
    if q:
        data = [c for c in data if q in c['word'].lower() or q in c['types'].lower()]
    
    # [NEW] 排序逻辑
    sort_by = request.args.get('sort', 'default')
    if sort_by == 'freq':
        data = sorted(data, key=lambda x: x.get('count', 0), reverse=True)
    elif sort_by == 'ascii':
        data = sorted(data, key=lambda x: x.get('word', '').lower())
    
    total = len(data)
    start = (page - 1) * page_size
    end = start + page_size
    
    return jsonify({
        'total': total,
        'page': page,
        'page_size': page_size,
        'data': data[start:end]
    })

@app.route('/api/samples')
def api_samples():
    """返回指定词的原文样本"""
    word = request.args.get('word', '').strip()
    samples = SAMPLES_CACHE.get(word, [])
    return jsonify({'word': word, 'samples': samples})

@app.route('/api/decide', methods=['POST'])
def api_decide():
    body = request.json
    word = body.get('word')
    decided = body.get('decided', [])
    category = body.get('category', '未分类')
    
    for item in CANDIDATES_CACHE:
        if item['word'] == word:
            item['decided'] = decided
            item['category'] = category
            save_decision_to_md(item['_file'], word, decided, category)
            break

    
    return jsonify({'ok': True})

@app.route('/api/batch_decide', methods=['POST'])
def api_batch_decide():
    """批量设置（当前页全选 Tag / 全部标为 Noise）"""
    body = request.json
    words = body.get('words', [])   # 词列表
    decided = body.get('decided', [])
    word_set = set(words)
    file_groups = {} # file_path -> [(word, decided, category)]
    
    for item in CANDIDATES_CACHE:
        if item['word'] in word_set:
            item['decided'] = decided
            # 批量操作通常是设置 TAG 或 NOISE，这里假设 category 不变（除非是 TAG 批量设置，但 api_batch_decide 目前只传 decided）
            # 实际上 api_batch_decide 应该也支持 category。
            # 检查 body 是否有 category
            category = body.get('category', item.get('category', '未分类'))
            item['category'] = category
            
            f = item['_file']
            if f not in file_groups: file_groups[f] = []
            file_groups[f].append((item['word'], decided, category))
            
    # 执行文件批量更新
    for f_path, changes in file_groups.items():
        p = Path(f_path)
        if not p.exists(): continue
        try:
            content = p.read_text(encoding='utf-8')
            lines = content.splitlines()
            new_lines = []
            # change_map = {word: (decided, category)}
            cmap = {w: (d, c) for w, d, c in changes}
            
            for line in lines:
                m_word = re.search(r'` (.*?) `', line)
                if m_word:
                    word = m_word.group(1).strip()
                    if word in cmap:
                        d, cat = cmap[word]
                        c_box = "[x]" if "creator" in d else "[ ]"
                        a_box = "[x]" if "actor" in d else "[ ]"
                        t_box = "[x]" if "tag" in d else "[ ]"
                        n_box = "[x]" if "noise" in d else "[ ]"
                        tag_label = f"TAG({cat})" if cat and cat != "未分类" else "TAG"
                        
                        line = re.sub(r'\[.\]\s*CREATOR', f'{c_box} CREATOR', line)
                        line = re.sub(r'\[.\]\s*ACTOR', f'{a_box} ACTOR', line)
                        line = re.sub(r'\[.\]\s*TAG(\(.*?\))?', f'{t_box} {tag_label}', line)
                        line = re.sub(r'\[.\]\s*NOISE', f'{n_box} NOISE', line)
                new_lines.append(line)
            p.write_text("\n".join(new_lines) + "\n", encoding='utf-8')
        except: pass
        
    return jsonify({'ok': True, 'count': len(word_set)})


@app.route('/api/categories')
def api_get_categories():
    """获取所有类目列表"""
    return jsonify(CATEGORIES)

@app.route('/api/categories/add', methods=['POST'])
def api_add_category():
    body = request.json
    name = body.get('name', '').strip()
    if not name:
        return jsonify({'ok': False, 'error': '分类名称不能为空'})

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        if name in kws:
            return jsonify({'ok': False, 'error': '该分类已存在'})
        
        kws[name] = []
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        global CATEGORIES
        CATEGORIES = load_categories(CONFIG)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/categories/rename', methods=['POST'])
def api_rename_category():
    body = request.json
    old_name = body.get('old_name')
    new_name = body.get('new_name')
    if not old_name or not new_name or old_name == new_name:
        return jsonify({'ok': False, 'error': '无效名称'})

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        
        # 递归找以此为前缀的所有类目
        to_rename = []
        for k in list(kws.keys()): # 使用 list 保证安全
            if k == old_name or k.startswith(old_name + "/"):
                to_rename.append(k)
        
        if not to_rename:
            return jsonify({'ok': False, 'error': '该路径下未发现任何有效类目'})
        
        # 按照长度降序排序，先重命名子级 (如果是复杂结构)
        # 但在 flat dict 结构中，先 rename 父级再 rename 子级也可以，
        # 只要 replace 逻辑对。
        to_rename.sort(key=len, reverse=True)
        
        for k in to_rename:
            # 仅替换最前面的匹配，确保层级正确
            new_k = re.sub(f"^{re.escape(old_name)}", new_name, k)
            kws[new_k] = kws.pop(k)
            
        data['keywords'] = kws
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        
        # [NEW] 同时更新内存缓存中的候选词分类，防止重命名后提交导致旧名称回流
        if CANDIDATES_CACHE:
            for item in CANDIDATES_CACHE:
                c = item.get('category', '未分类')
                if c == old_name:
                    item['category'] = new_name
                elif c.startswith(old_name + "/"):
                    item['category'] = re.sub(f"^{re.escape(old_name)}", new_name, c)
        
        # [NEW] 同时更新物理 MD 文件中的 TAG 标记，防止 P1/P2/P3 链路读回旧名称
        candidates_dir = PROJECT_ROOT / CONFIG.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates')
        if candidates_dir.exists():
            # 正则匹配形如 TAG(OldName) 或 [ ] TAG(OldName) |
            # 注意：TAG 后面可能带括号。
            tag_pattern = re.compile(rf'TAG\({re.escape(old_name)}\)')
            new_tag = f"TAG({new_name})"
            
            # 同样考虑子类目情况 A/B -> A1/B
            # 如果是子类目重命名，old_name 已经是 A/B，new_name 是 A1/B，直接替换即可
            
            for md_file in candidates_dir.glob("candidate_pool_part_*.md"):
                try:
                    content = md_file.read_text(encoding='utf-8')
                    if tag_pattern.search(content):
                        new_content = tag_pattern.sub(new_tag, content)
                        # 如果是父目录重命名，还需要处理子目录的匹配，例如 TAG(A/B) 当 A 变为 A1 时变为 TAG(A1/B)
                        # 上面的 tag_pattern 只匹配精确的 old_name。
                        # 我们增加一个匹配子类目的正则
                        sub_tag_pattern = re.compile(rf'TAG\({re.escape(old_name)}/')
                        new_sub_tag = f"TAG({new_name}/"
                        new_content = sub_tag_pattern.sub(new_sub_tag, new_content)
                        
                        md_file.write_text(new_content, encoding='utf-8')
                        print(f"📄 已同步更新 MD 文件: {md_file.name}")
                except Exception as e:
                    print(f"⚠️ 同步更新 MD 文件失败 {md_file.name}: {e}")

        # 触发全景视图更新
        threading.Thread(target=update_md).start()
        
        # 刷新内存缓存
        global CATEGORIES
        CATEGORIES = load_categories(CONFIG)
        
        return jsonify({'ok': True, 'count': len(to_rename)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/categories/delete', methods=['POST'])
def api_delete_category():
    body = request.json
    name = body.get('name')
    if not name or name == "未分类":
        return jsonify({'ok': False, 'error': '不能删除基础类目'})

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        if name in kws:
            items = kws.pop(name)
            # 移动到未分类
            if "未分类" not in kws: kws["未分类"] = []
            kws["未分类"].extend(items)
            entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
            global CATEGORIES
            CATEGORIES = load_categories(CONFIG)
            return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})
    return jsonify({'ok': False, 'error': '类目不存在'})

@app.route('/api/categories/move', methods=['POST'])
def api_move_category():
    """将整个分类及其子分类移动到新父节点下"""
    body = request.json
    old_path = body.get('old_path') # e.g. "服饰/上身服饰"
    new_parent = body.get('new_parent') # e.g. "杂1"
    
    if not old_path or new_parent is None:
        return jsonify({'ok': False, 'error': '缺失必要参数'})
        
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        
        # 计算新路径基础
        basename = old_path.split('/')[-1]
        new_path_base = f"{new_parent}/{basename}" if new_parent else basename
        
        if new_path_base == old_path:
            return jsonify({'ok': False, 'error': '新旧路径一致'})
        if new_path_base.startswith(old_path + "/"):
            return jsonify({'ok': False, 'error': '不能移动到自己的子目录下'})

        new_kws = {}
        moved_count = 0
        # 必须先处理所有 keys，避免 rename 过程中字典变化
        for cat in list(kws.keys()):
            if cat == old_path or cat.startswith(old_path + "/"):
                new_cat = cat.replace(old_path, new_path_base, 1)
                new_kws[new_cat] = kws.pop(cat)
                moved_count += 1
            else:
                new_kws[cat] = kws.pop(cat)
        
        data['keywords'] = new_kws
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        global CATEGORIES
        CATEGORIES = load_categories(CONFIG)
        threading.Thread(target=update_md).start()
        return jsonify({'ok': True, 'moved': moved_count})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/entities')
def api_entities():
    """获取已分拣的全量实体数据"""
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        # 实时自动迁移
        if isinstance(kws, list):
            kws = {"未分类": kws}
            data['keywords'] = kws
        
        # 提取暂存中的标签：优先读持久化 staging 文件，再兼容内存中尚未提交的勾选
        staging_map = {}
        for st in load_staging(CONFIG):
            name = st.get('name')
            if not name:
                continue
            staging_map[str(name).strip().lower()] = {
                'name': name,
                'category': st.get('category', '未分类'),
                'is_staging': True,
                'type': st.get('type', 'tag')
            }

        for c in CANDIDATES_CACHE:
            decided = c.get('decided') or []
            decided_norm = {str(x).strip().lower() for x in decided}
            if 'tag' in decided_norm:
                staging_map[str(c['word']).strip().lower()] = {
                    'name': c['word'],
                    'category': c.get('category', '未分类'),
                    'is_staging': True
                }

        staging = list(staging_map.values())

        # 将暂存的 creators 合并到 creators 列表中，前端可显示为暂存项
        creators_out = data.get('creators', []) or []
        # 标准化 creators_out 为 dict entries
        norm_creators = []
        for c in creators_out:
            if isinstance(c, dict):
                norm_creators.append(c)
            else:
                norm_creators.append({'name': c, 'aliases': []})

        for st in staging:
            if st.get('type') == 'creator':
                # 如果已在正式创作者里，不重复；否则追加一个带 is_staging 标记的项
                exists = any((item.get('name') if isinstance(item, dict) else item) == st['name'] for item in norm_creators)
                if not exists:
                    norm_creators.append({'name': st['name'], 'aliases': [], 'is_staging': True})

        creators_final = norm_creators
        
        return jsonify({
            'keywords': kws,
            'creators': creators_final,
            'actors': data.get('actors', []),
            'noise': data.get('noise', []),
            'staging': staging
        })
    except:
        return jsonify({'keywords': {}, 'creators': [], 'actors': [], 'noise': []})

@app.route('/api/entities/add_direct', methods=['POST'])
def api_add_entity_direct():
    global CANDIDATES_CACHE
    body = request.json
    word = body.get('word', '').strip()
    e_type = body.get('type', 'tag') # creator, actor, tag, noise
    category = body.get('category', '未分类')
    
    if not word: return jsonify({'ok': False, 'error': '词汇不能为空'})
    
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        
        # 统一查重函数
        def exists(word, target_list):
            return any((item.get('name') if isinstance(item, dict) else item) == word for item in target_list)

        if e_type == 'creator':
            if 'creators' not in data: data['creators'] = []
            if exists(word, data['creators']): return jsonify({'ok': False, 'error': '该创作者已存在'})
            data['creators'].append({"name": word, "aliases": []})
        elif e_type == 'actor':
            if 'actors' not in data: data['actors'] = []
            if exists(word, data['actors']): return jsonify({'ok': False, 'error': '该人物已存在'})
            data['actors'].append({"name": word, "aliases": []})
        elif e_type == 'noise':
            if 'noise' not in data: data['noise'] = []
            if word in data['noise']: return jsonify({'ok': False, 'error': '该噪声已存在'})
            data['noise'].append(word)
        else: # tag
            if 'keywords' not in data: data['keywords'] = {}
            if category not in data['keywords']: data['keywords'][category] = []
            if exists(word, data['keywords'][category]): return jsonify({'ok': False, 'error': '该关键词已存在'})
            data['keywords'][category].append({"name": word, "aliases": []})
            
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        
        threading.Thread(target=update_md).start()

        
        # [NEW] 动态去重：如果此词在候选池中，立即移除
        CANDIDATES_CACHE = [item for item in CANDIDATES_CACHE if (item['word'].lower() != word.lower())]
        
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/entities/remove', methods=['POST'])
def api_remove_entity():
    global CANDIDATES_CACHE
    body = request.json
    name = body.get('name')
    e_type = body.get('type') # creator, actor, noise, tag
    category = body.get('category')
    
    if not name or not e_type: return jsonify({'ok': False, 'error': '缺失必要参数'})
    
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        found = False
        
        if e_type == 'creator':
            data['creators'] = [e for e in data.get('creators', []) if (e['name'] if isinstance(e, dict) else e) != name]
            found = True
        elif e_type == 'actor':
            data['actors'] = [e for e in data.get('actors', []) if (e['name'] if isinstance(e, dict) else e) != name]
            found = True
        elif e_type == 'noise':
            if name in data.get('noise', []):
                data['noise'].remove(name)
                found = True
                # [NEW] 从噪声词删除后，自动放回候选池
                if not any(c['word'] == name for c in CANDIDATES_CACHE):
                    CANDIDATES_CACHE.append({
                        'word': name,
                        'freq': 1,
                        'types': 'restored',
                        'decided': None,
                        'category': '未分类',
                        '_file': 'restored_from_noise',
                        'samples': [],
                        'added_at': time.time()
                    })
            if category in data.get('keywords', {}):
                data['keywords'][category] = [e for e in data['keywords'][category] if (e['name'] if isinstance(e, dict) else e) != name]
                found = True
                
        if found:
            entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
            threading.Thread(target=update_md).start()

            return jsonify({'ok': True})
        return jsonify({'ok': False, 'error': '未找到对应实体'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/entities/move', methods=['POST'])
def api_move_entity():
    global CATEGORIES
    body = request.json or {}
    word = body.get('word')
    old_cat = body.get('old_category')
    new_cat = body.get('new_category')

    if not word or not new_cat:
        return jsonify({'ok': False, 'error': '缺少必要参数'}), 400

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        if isinstance(kws, list):
            kws = {'未分类': kws}
            data['keywords'] = kws

        source_cat = old_cat
        target = None

        if old_cat and old_cat in kws:
            new_list = []
            for item in kws.get(old_cat, []):
                curr_name = item.get('name') if isinstance(item, dict) else item
                if curr_name == word and target is None:
                    target = item
                else:
                    new_list.append(item)
            kws[old_cat] = new_list

        if target is None:
            for cat, lst in list(kws.items()):
                new_list = []
                found_here = False
                for item in lst:
                    curr_name = item.get('name') if isinstance(item, dict) else item
                    if curr_name == word and target is None:
                        target = item
                        source_cat = cat
                        found_here = True
                    else:
                        new_list.append(item)
                if found_here:
                    kws[cat] = new_list
                    break

        if target is None:
            return jsonify({'ok': False, 'error': '未找到该关键词'}), 404

        if not isinstance(target, dict):
            target = {'name': target, 'aliases': []}

        if new_cat not in kws:
            kws[new_cat] = []

        kws[new_cat].append(target)
        data['keywords'] = kws
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


        threading.Thread(target=update_md).start()
        CATEGORIES = load_categories(CONFIG)

        return jsonify({'ok': True, 'moved_from': source_cat, 'moved_to': new_cat})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/entities/alias/add', methods=['POST'])
def api_add_entity_alias():
    body = request.json
    e_type = body.get('type') # creator / actor / tag
    name = body.get('name')
    alias = body.get('alias', '').strip()
    category = body.get('category') # For tag
    if not name or not alias: return jsonify({'ok': False, 'error': '参数不足'})

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        
        target_list = []
        if e_type == 'creator': target_list = data.get('creators', [])
        elif e_type == 'actor': target_list = data.get('actors', [])
        elif e_type == 'tag' and category: target_list = data.get('keywords', {}).get(category, [])
        else: return jsonify({'ok': False, 'error': '不支持的类型或缺失分类信息'})
        
        found = False
        for i, item in enumerate(target_list):
            curr_name = item.get('name') if isinstance(item, dict) else item
            if curr_name == name:
                if isinstance(item, str): 
                    # 升级为 dict
                    item = {"name": item, "aliases": []}
                    target_list[i] = item
                
                if 'aliases' not in item: item['aliases'] = []
                if alias not in item['aliases']:
                    item['aliases'].append(alias)
                found = True
                break
        
        if not found: return jsonify({'ok': False, 'error': '实体不存在'})
        
        # 回写
        if e_type == 'creator': data['creators'] = target_list
        elif e_type == 'actor': data['actors'] = target_list
        elif e_type == 'tag': data['keywords'][category] = target_list

        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/entities/alias/remove', methods=['POST'])
def api_remove_entity_alias():
    body = request.json
    e_type = body.get('type')
    name = body.get('name')
    alias = body.get('alias')
    category = body.get('category')
    if not name or not alias: return jsonify({'ok': False, 'error': '参数不足'})

    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        
        target_list = []
        if e_type == 'creator': target_list = data.get('creators', [])
        elif e_type == 'actor': target_list = data.get('actors', [])
        elif e_type == 'tag' and category: target_list = data.get('keywords', {}).get(category, [])
        else: return jsonify({'ok': False, 'error': '不支持的类型或缺失分类信息'})
        
        found = False
        for item in target_list:
            curr_name = item.get('name') if isinstance(item, dict) else item
            if curr_name == name:
                if isinstance(item, dict) and alias in item.get('aliases', []):
                    item['aliases'].remove(alias)
                    found = True
                    break
        
        if not found: return jsonify({'ok': False, 'error': '别名或实体不存在'})
        
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/entities/merge', methods=['POST'])
def api_entities_merge():
    """合并两个实体：将 to_merge 合并到 primary"""
    body = request.json
    e_type = body.get('type') # creator, actor, tag
    primary_name = body.get('primary_name')
    to_merge_name = body.get('to_merge_name')
    category = body.get('category') # For tags
    
    if not all([e_type, primary_name, to_merge_name]):
        return jsonify({'ok': False, 'error': '缺失必要参数'})
        
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        
        def find_entity(entity_list, name):
            name_l = name.lower()
            for i, e in enumerate(entity_list):
                curr_name = (e['name'] if isinstance(e, dict) else e).lower()
                if curr_name == name_l:
                    return i, e
            return -1, None
            
        target_list = []
        if e_type == 'creator':
            target_list = data.get('creators', [])
        elif e_type == 'actor':
            target_list = data.get('actors', [])
        elif e_type == 'tag' and category:
            target_list = data.get('keywords', {}).get(category, [])
        else:
            return jsonify({'ok': False, 'error': '不支持的类型或缺失分类信息'})
            
        idx_p, e_p = find_entity(target_list, primary_name)
        idx_m, e_m = find_entity(target_list, to_merge_name)
        
        if idx_p == -1 or idx_m == -1:
            return jsonify({'ok': False, 'error': f'未找到实体: {primary_name if idx_p==-1 else to_merge_name}'})
            
        # 归一化为字典
        if not isinstance(e_p, dict): e_p = {"name": e_p, "aliases": []}
        if not isinstance(e_m, dict): e_m = {"name": e_m, "aliases": []}
        
        # 合并别名
        merged_aliases = set(e_p.get('aliases', []))
        merged_aliases.update(e_m.get('aliases', []))
        merged_aliases.add(e_m['name']) # 被合并的主名变更为别名
        
        # 确保 primary_name 不在别名列表中
        if primary_name in merged_aliases:
            merged_aliases.remove(primary_name)
            
        e_p['aliases'] = sorted(list(merged_aliases))
        target_list[idx_p] = e_p
        
        # 删除被合并的项
        target_list.pop(idx_m)
        
        # 回写
        if e_type == 'creator': data['creators'] = target_list
        elif e_type == 'actor': data['actors'] = target_list
        elif e_type == 'tag': data['keywords'][category] = target_list
        
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        threading.Thread(target=update_md).start()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/commit', methods=['POST'])
def api_commit():
    """
    确认提交：
    4. 调用 index_exporter.py（P3 生成 tags/）
    """
    if not pipeline_active_lock.acquire(blocking=False):
        return jsonify({'ok': False, 'msg': '流水线已在运行中，请等结束后再试。'}), 429

    is_partial = request.args.get('partial') == '1'
    
    decided_words = {
        item['word']: item['decided']
        for item in CANDIDATES_CACHE
        if item['decided']
    }
    
    if not decided_words:
        return jsonify({'ok': False, 'error': '未做任何分拣决策'})
    
    # 1. 更新 candidate MD 文件：移除已分拣行并重排序号
    candidates_dir = PROJECT_ROOT / CONFIG['candidates_dir_docs']
    # [FIX] 增强正则，允许分类括号且对空格更宽容
    pattern = re.compile(
        r'(\d+\.\s*`\s*)(.*?)(\s*`\s*——\s*)\[(.)\]\s*(CREATOR.*?)\s*\|\s*\[(.)\]\s*(ACTOR.*?)\s*\|\s*\[(.)\]\s*(TAG(?:\(.*?\))?.*?)\s*\|\s*\[(.)\]\s*(NOISE)'
    )
    
    # [ACTION] 我们需要先同步到 entities.json，再从池中删除
    # 所以我们把删除逻辑放到 run_pipeline 的第一步
    
    def run_pipeline():
        global CANDIDATES_CACHE, SAMPLES_CACHE
        py = sys.executable
        bot_name = CONFIG.get('app_name', 'tgporncopilot')
        decided_word_list = list(decided_words.keys())

        if is_partial:
            # [NEW] 暂存 = 直接写入 currententities/entities.json
            entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
            entities_path.parent.mkdir(parents=True, exist_ok=True)
            
            if entities_path.exists():
                try:
                    ent_data = json.loads(entities_path.read_text(encoding='utf-8'))
                except Exception:
                    ent_data = {'creators': [], 'actors': [], 'keywords': {}, 'noise': []}
            else:
                ent_data = {'creators': [], 'actors': [], 'keywords': {}, 'noise': []}
            
            for word, d in decided_words.items():
                d_norm = {str(x).strip().lower() for x in (d or [])}
                cat = next((item['category'] for item in CANDIDATES_CACHE if item['word'] == word), '未分类')
                
                def exists_in(lst, w):
                    return any((e.get('name') if isinstance(e, dict) else e) == w for e in lst)
                
                if 'creator' in d_norm:
                    if 'creators' not in ent_data: ent_data['creators'] = []
                    if not exists_in(ent_data['creators'], word):
                        ent_data['creators'].append({'name': word, 'aliases': []})
                if 'actor' in d_norm:
                    if 'actors' not in ent_data: ent_data['actors'] = []
                    if not exists_in(ent_data['actors'], word):
                        ent_data['actors'].append({'name': word, 'aliases': []})
                if 'tag' in d_norm:
                    if 'keywords' not in ent_data: ent_data['keywords'] = {}
                    if cat not in ent_data['keywords']: ent_data['keywords'][cat] = []
                    if not exists_in(ent_data['keywords'][cat], word):
                        ent_data['keywords'][cat].append({'name': word, 'aliases': []})
                if 'noise' in d_norm:
                    if 'noise' not in ent_data: ent_data['noise'] = []
                    if word not in ent_data['noise']:
                        ent_data['noise'].append(word)
            
            entities_path.write_text(json.dumps(ent_data, ensure_ascii=False, indent=2), encoding='utf-8')
        # else: full commit path continues below with sync_entities
        
        # 第一步：物理写回 MD 文件（带上 [x] 标志）
        # 这是为了让 sync_entities.py 能够正确识别并同步到 JSON
        with pipeline_lock:
            PIPELINE_LOG.append({'step': 'cleanup_pool', 'status': 'running', 'msg': '▶ 1/4 正在更新候选词池状态 (MD)...'})
        
        current_rank = 1
        for md_file in sorted(candidates_dir.glob("candidate_pool_part_*.md")):
            lines = md_file.read_text(encoding='utf-8').splitlines()
            header_lines = []
            word_lines = []
            for line in lines:
                if line.strip() and re.match(r'^\d+\.\s*`', line):
                    m = pattern.search(line)
                    if m:
                        word = m.group(2).strip()
                        if word not in decided_words:
                            # 没分拣的重排号保留
                            box_content = line.split(' —— ')[1]
                            new_line = f"{current_rank}. ` {word} ` —— {box_content}"
                            word_lines.append(new_line)
                            current_rank += 1
                        else:
                            # 已分拣的，根据决策打标 [x]
                            d = decided_words[word]
                            cat = next((item['category'] for item in CANDIDATES_CACHE if item['word'] == word), "未分类")
                            
                            c_box = "[x]" if "creator" in d else "[ ]"
                            a_box = "[x]" if "actor" in d else "[ ]"
                            t_box = "[x]" if "tag" in d else "[ ]"
                            tag_label = f"TAG({cat})" if cat and cat != "未分类" else "TAG"
                            n_box = "[x]" if "noise" in d else "[ ]"
                            
                            m_rest = re.search(r'\(频次:.*?\)', line)
                            rest_info = m_rest.group(0) if m_rest else ""
                            new_line = f"{current_rank}. ` {word} ` —— {c_box} CREATOR | {a_box} ACTOR | {t_box} {tag_label} | {n_box} NOISE {rest_info}"
                            
                            if is_partial:
                                # 暂存模式：保留在 MD 中以便后续继续查阅或二次提交
                                word_lines.append(new_line)
                                current_rank += 1
                            else:
                                # 正式提交模式：从 pool 中移除（不加入 word_lines）
                                pass
                elif not word_lines:
                    header_lines.append(line)
            
            if word_lines or header_lines:
                md_file.write_text("\n".join(header_lines + word_lines) + "\n", encoding='utf-8')
            elif not is_partial:
                # 仅在正式提交且文件全空时删除
                try: os.remove(md_file)
                except: pass

        # [NEW] 第 1.5 步：物理写回 JSON 文件 (Data 目录)
        with pipeline_lock:
            PIPELINE_LOG.append({'step': 'sync_json_pool', 'status': 'running', 'msg': '▶ 1.5/4 正在同步候选词池状态 (JSON)...'})
        
        candidates_data_dir = PROJECT_ROOT / CONFIG.get('candidates_dir_data', 'data/entities/tgporncopilot/candidates')
        if candidates_data_dir.exists():
            for json_file in sorted(candidates_data_dir.glob("candidate_pool_part_*.json")):
                try:
                    pool_data = json.loads(json_file.read_text(encoding='utf-8'))
                    new_pool_data = []
                    for item in pool_data:
                        w = item.get('word')
                        if w not in decided_words:
                            new_pool_data.append(item)
                        elif is_partial:
                            # 暂存模式：打上标记保留
                            item['decided'] = decided_words[w]
                            new_pool_data.append(item)
                    
                    if new_pool_data:
                        json_file.write_text(json.dumps(new_pool_data, ensure_ascii=False, indent=2), encoding='utf-8')
                    elif not is_partial:
                        try: os.remove(json_file)
                        except: pass
                except: pass

        # 第二步：同步到 entities.json (现在 MD 已经是 [x] 状态了)
        with pipeline_lock:
            PIPELINE_LOG.append({'step': 'sync_entities', 'status': 'running', 'msg': '▶ 2/4 正在同步词库 (MD -> JSON)...'})
        sync_cmd = f'"{py}" src/search_mode/program1_discovery/sync_entities.py --bot "{bot_name}"'
        res = subprocess.run(sync_cmd, shell=True, capture_output=True, text=True, encoding='utf-8', cwd=str(PROJECT_ROOT))
        if res.returncode != 0:
            with pipeline_lock: PIPELINE_LOG.append({'step': 'sync_entities', 'status': 'error', 'msg': res.stderr[-500:]})
            return

        # 更新 Samples JSON
        samples_path = candidates_dir / 'candidate_samples.json'
        if samples_path.exists():
            try:
                samples = json.loads(samples_path.read_text(encoding='utf-8'))
                for w in decided_words:
                    if w in samples: del samples[w]
                samples_path.write_text(json.dumps(samples, ensure_ascii=False), encoding='utf-8')
                SAMPLES_CACHE = samples
            except: pass

        # 更新 Metadata
        meta_path = candidates_dir / 'candidate_metadata.json'
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding='utf-8'))
                meta['candidate_count'] = current_rank - 1
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
            except: pass

        # 刷新内存缓存
        CANDIDATES_CACHE = [item for item in CANDIDATES_CACHE if item['word'] not in decided_words]

        if is_partial:
            log_pipeline({'step': 'done', 'status': 'done', 'msg': f'💾 暂存完成！已处理 {len(decided_words)} 个词，候选池已同步。'})
            return

        # 第三步：P2 打标
        log_pipeline({'step': 'global_tagger', 'status': 'running', 'msg': '▶ 3/4 正在全量打标 (DB)...'})
        tag_cmd = f'"{py}" src/search_mode/program2_tagging/global_tagger.py --bot "{bot_name}"'
        subprocess.run(tag_cmd, shell=True, capture_output=True, text=True, encoding='utf-8', cwd=str(PROJECT_ROOT))

        # 第四步：P3 导出
        log_pipeline({'step': 'index_exporter', 'status': 'running', 'msg': '▶ 4/4 正在生成预览预览 (Markdown)...'})
        exp_cmd = f'"{py}" src/search_mode/program3_export/index_exporter.py --bot "{bot_name}"'
        subprocess.run(exp_cmd, shell=True, capture_output=True, text=True, encoding='utf-8', cwd=str(PROJECT_ROOT))

        log_pipeline({'step': 'done', 'status': 'done', 'msg': f'✅ 分拣完成！已成功处理 {len(decided_words)} 个词，池中剩余 {current_rank-1} 个词。'})
    
    def run_pipeline_wrapper():
        try:
            run_pipeline()
        finally:
            pipeline_active_lock.release()

    thread = threading.Thread(target=run_pipeline_wrapper, daemon=True)
    thread.start()
    
    return jsonify({'ok': True, 'committed': len(decided_words)})
    
@app.route('/api/tag_back', methods=['POST'])
def api_tag_back():
    """
    Tag Back 功能：将选定的词移出词库（标记为需要重新打标/分拣）
    此处实现为从 entities.json 中删除并触发 P2/P3。
    """
    body = request.json
    words = body.get('words', [])
    if not words:
        return jsonify({'ok': False, 'error': '未选择词汇'})
    
    entities_path = PROJECT_ROOT / CONFIG['currententities_dir_data'] / 'entities.json'
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        word_set = {w.lower() for w in words}
        removed_count = 0
        
        # 从 creators, actors, noise 中移除
        for key in ['creators', 'actors', 'noise']:
            old_list = data.get(key, [])
            new_list = []
            for item in old_list:
                name = (item.get('name') if isinstance(item, dict) else item).lower()
                if name not in word_set:
                    new_list.append(item)
                else:
                    removed_count += 1
            data[key] = new_list
            
        # 从 keywords 中移除
        kws = data.get('keywords', {})
        for cat, items in kws.items():
            new_items = []
            for item in items:
                name = (item.get('name') if isinstance(item, dict) else item).lower()
                if name not in word_set:
                    new_items.append(item)
                else:
                    removed_count += 1
            kws[cat] = new_items
            
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        
        # [NEW] 同时将这些词在候选池 MD 中“解锁” (打标回去)
        candidates_dir = PROJECT_ROOT / CONFIG['candidates_dir_docs']
        if candidates_dir.exists():
            for md_file in candidates_dir.glob("candidate_pool_part_*.md"):
                try:
                    content = md_file.read_text(encoding='utf-8')
                    lines = content.splitlines()
                    new_lines = []
                    changed = False
                    for line in lines:
                        # 查找格式如 ` Word `
                        m = re.search(r'` (.*?) `', line)
                        if m:
                            w = m.group(1).strip().lower()
                            if w in word_set:
                                # 将 [x] 变为 [ ], 清除 TAG(...)
                                line = line.replace('[x]', '[ ]')
                                line = re.sub(r'TAG\(.*?\)', 'TAG', line)
                                # 强制清除所有复选框状态
                                line = re.sub(r'\[x\]', '[ ]', line) 
                                changed = True
                        new_lines.append(line)
                    if changed:
                        md_file.write_text("\n".join(new_lines) + "\n", encoding='utf-8')
                except: pass

        # [NEW] 同时将这些词在 JSON 候选池中打标回去
        candidates_data_dir = PROJECT_ROOT / CONFIG.get('candidates_dir_data', 'data/entities/tgporncopilot/candidates')
        if candidates_data_dir.exists():
            for json_file in candidates_data_dir.glob("candidate_pool_part_*.json"):
                try:
                    pool_data = json.loads(json_file.read_text(encoding='utf-8'))
                    changed = False
                    for item in pool_data:
                        if str(item.get('word', '')).strip().lower() in word_set:
                            item['decided'] = [] # 清空决策
                            changed = True
                    if changed:
                        json_file.write_text(json.dumps(pool_data, ensure_ascii=False, indent=2), encoding='utf-8')
                except: pass

        # 启动后台更新
        threading.Thread(target=run_p2p3_worker, daemon=True).start()
        
        # 刷新内存缓存以便去重失效，使这些词能重新显示在分拣页
        global CANDIDATES_CACHE
        CANDIDATES_CACHE = load_candidates(CONFIG)

        return jsonify({'ok': True, 'removed': removed_count, 'msg': '已移出词库并触发后台更新'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/progress')
def api_progress():
    """SSE 流推送流水线进度"""
    def stream():
        sent = 0
        while True:
            with pipeline_lock:
                # [FIX] 如果日志被清空了，将 sent 重置为 0，防止索引越位或死等
                if sent > len(PIPELINE_LOG):
                    sent = 0
                new_logs = PIPELINE_LOG[sent:]
            for log in new_logs:
                yield f"data: {json.dumps(log, ensure_ascii=False)}\n\n"
                sent += 1
                if log.get('step') == 'done':
                    return
            time.sleep(0.5)
    return Response(stream(), content_type='text/event-stream')


def get_latest_backup_info_for_pipeline():
    """核对数据库中最新的备份 Run ID 和 Label"""
    from src.db import Database
    db = Database(str(PROJECT_ROOT / 'data/copilot.db'))
    info = db.get_latest_backup_info(is_test=False)
    db.close()
    
    # 还需要获取备份文件的最新修改时间 (MTime)
    backup_dir = PROJECT_ROOT / 'data/archived/backups'
    max_mtime = 0
    if backup_dir.exists():
        for root, dirs, files in os.walk(backup_dir):
            for f in files:
                if f.endswith('.json'):
                    try:
                        mt = os.path.getmtime(os.path.join(root, f))
                        if mt > max_mtime: max_mtime = mt
                    except: pass
    return info, max_mtime

def run_full_pipeline_worker():
    """
    智能流水线工人：
    1. 检查新鲜度：如果备份已更新但 P0/P1 未跑，则先跑 P1。
    2. 跑 P1.2 (Sync MD)
    3. 跑 P2 (Global Tagger) + 进度采集
    4. 跑 P3 (Index Exporter) + 进度采集
    5. 生成全量总结报告
    """
    py = sys.executable
    project_root_str = str(PROJECT_ROOT)
    bot_name = CONFIG.get('app_name', 'tgporncopilot')
    temp_dir = PROJECT_ROOT / 'data/temp'
    os.makedirs(temp_dir, exist_ok=True)
    
    # 生成当前任务的唯一 ID，并透传给所有日志消息
    current_run_id = f"run_{int(time.time())}"
    
    # 清空之前的日志状态
    with pipeline_lock:
        PIPELINE_LOG.clear()
    log_pipeline({
        'run_id': current_run_id,
        'step': 'init', 
        'status': 'running', 
        'msg': '🔍 正在检查备份同步状态...'
    })

    # 1. 检查新鲜度 (Step 0)
    info, current_max_mtime = get_latest_backup_info_for_pipeline()
    backup_id = info['label'] if info else 'NONE'
    
    candidates_docs_dir = PROJECT_ROOT / CONFIG.get('candidates_dir_docs', 'docs/entities/tgporncopilot/candidates')
    meta_path = candidates_docs_dir / 'candidate_metadata.json'
    
    should_run_p1 = True
    reason = "没有发现现有候选词元数据"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
            if meta.get('latest_backup_id') == backup_id and abs(meta.get('max_mtime', 0) - current_max_mtime) < 1:
                should_run_p1 = False
            else:
                reason = f"备份编号不匹配 ({meta.get('latest_backup_id')} vs {backup_id}) 或时间戳更新"
        except: pass

    mission_stats = {
        'p1': None,
        'p2': None,
        'p3': None,
        'backup_id': backup_id,
        'start_time': time.strftime("%H:%M:%S")
    }
    start_all = time.time()

    if should_run_p1:
        log_pipeline({'step': 'p1_discovery', 'status': 'running', 'msg': f'▶ P1 (Discovery): 正在挖掘新候选词... ({reason})'})
        
        p1_prog = temp_dir / f"p1_prog_{int(time.time())}.json"
        p1_cmd = f'"{py}" src/search_mode/program1_discovery/entity_extractor.py --bot "{bot_name}" --backup-id "{backup_id}" --progress-file "{str(p1_prog)}"'
        
        proc = subprocess.Popen(p1_cmd, shell=True, cwd=project_root_str)
        while True:
            poll = proc.poll()
            if p1_prog.exists():
                try:
                    data = json.loads(p1_prog.read_text(encoding='utf-8'))
                    if data.get('status') in ['scanning', 'completed']:
                        files_done = data.get('files_done', 0)
                        total_files = max(1, data.get('total_files', 1))
                        percent = 100 if data.get('status') == 'completed' else int(files_done * 100 / total_files)
                        msg = f"正在扫描: {files_done}/{total_files} 文件 (发现 {data.get('total_msgs', 0)} 条消息)"
                        if data.get('status') == 'completed': msg = "✅ P1 扫描完成"
                        log_pipeline({
                            'run_id': current_run_id,
                            'step': 'p1_discovery', 
                            'status': 'running', 
                            'msg': msg, 
                            'percent': percent
                        })
                except: pass
            if poll is not None: break
            time.sleep(1)
            
        # [ROBUSTNESS] 强制打满进展消息
        if p1_prog.exists():
            try:
                data = json.loads(p1_prog.read_text(encoding='utf-8'))
                log_pipeline({
                    'run_id': current_run_id,
                    'step': 'p1_discovery', 
                    'status': 'running', 
                    'msg': "✅ P1 扫描完成", 
                    'percent': 100
                })
            except: pass
        
        # 结果采集
        if p1_prog.exists():
            try:
                # 重新读取最终 meta
                meta_json = candidates_docs_dir / 'candidate_metadata.json'
                if meta_json.exists():
                    mission_stats['p1'] = json.loads(meta_json.read_text(encoding='utf-8'))
            except: pass
    else:
        log_pipeline({
            'run_id': current_run_id,
            'step': 'p1_discovery', 
            'status': 'skip', 
            'msg': '⏭️ P1 (Discovery): 候选池已是最新，跳过挖掘'
        })

    # 2. P1.2 Sync MD
    log_pipeline({
        'run_id': current_run_id,
        'step': 'sync_entities', 
        'status': 'running', 
        'msg': '▶ P1.2: 正在更新词库可视化预览 (MD)...'
    })
    sync_cmd = f'"{py}" src/search_mode/program1_discovery/sync_entities.py --bot "{bot_name}"'
    subprocess.run(sync_cmd, shell=True, capture_output=True, text=True, encoding='utf-8', cwd=project_root_str)

    # 3. P2 Global Tagger
    log_pipeline({
        'run_id': current_run_id,
        'step': 'global_tagger', 
        'status': 'running', 
        'msg': '▶ P2: 正在全量打标 (global_tagger)...'
    })
    
    p2_prog = temp_dir / f"p2_prog_{int(time.time())}.json"
    tag_cmd = f'"{py}" src/search_mode/program2_tagging/global_tagger.py --bot "{bot_name}" --progress-file "{str(p2_prog)}"'
    
    proc = subprocess.Popen(tag_cmd, shell=True, cwd=project_root_str)
    while True:
        poll = proc.poll()
        if p2_prog.exists():
            try:
                data = json.loads(p2_prog.read_text(encoding='utf-8'))
                if data.get('status') in ['running', 'completed']:
                    curr = data.get('current', 0)
                    total = max(1, data.get('total', 1))
                    percent = 100 if data.get('status') == 'completed' else int(curr * 100 / total)
                    msg = data.get('step_msg', f"正在打标: {curr}/{total}")
                    if data.get('status') == 'completed': msg = "✅ P2 打标完成"
                    log_pipeline({
                        'run_id': current_run_id,
                        'step': 'global_tagger', 
                        'status': 'running', 
                        'msg': msg, 
                        'percent': percent,
                        'detailed_html': data.get('detail_html')
                    })
            except: pass
        if poll is not None: break
        time.sleep(1)

    # [ROBUSTNESS] 强制打满 100% 进度消息
    if p2_prog.exists():
        try:
            data = json.loads(p2_prog.read_text(encoding='utf-8'))
            curr = data.get('current', 0)
            total = max(1, data.get('total', 1))
            is_done = data.get('status') == 'completed' or curr >= total
            log_pipeline({
                'run_id': current_run_id,
                'step': 'global_tagger', 
                'status': 'running', 
                'msg': "✅ P2 打标完成" if is_done else f"⚠️ P2 意外终止 ({curr}/{total})", 
                'percent': 100,
                'detailed_html': data.get('detail_html')
            })
        except: pass
    
    if p2_prog.exists():
        try: mission_stats['p2'] = json.loads(p2_prog.read_text(encoding='utf-8'))
        except: pass

    # 4. P3 Index Exporter
    log_pipeline({
        'run_id': current_run_id,
        'step': 'index_exporter', 
        'status': 'running', 
        'msg': '▶ P3: 正在生成 tags/Markdown (index_exporter)...'
    })
    
    p3_prog = temp_dir / f"p3_prog_{int(time.time())}.json"
    exp_cmd = f'"{py}" src/search_mode/program3_export/index_exporter.py --bot "{bot_name}" --progress-file "{str(p3_prog)}"'
    
    proc = subprocess.Popen(exp_cmd, shell=True, cwd=project_root_str)
    while True:
        poll = proc.poll()
        if p3_prog.exists():
            try:
                data = json.loads(p3_prog.read_text(encoding='utf-8'))
                if data.get('status') in ['running', 'completed']:
                    curr = data.get('current', 0)
                    total = max(1, data.get('total', 1))
                    percent = 100 if data.get('status') == 'completed' else int(curr * 100 / total)
                    msg = data.get('step_msg', f"正在导出: {curr}/{total}")
                    if data.get('status') == 'completed': msg = "✅ P3 导出完成"
                    log_pipeline({
                        'run_id': current_run_id,
                        'step': 'index_exporter', 
                        'status': 'running', 
                        'msg': msg, 
                        'percent': percent,
                        'detailed_html': data.get('detail_html')
                    })
            except: pass
        if poll is not None: break
        time.sleep(1)

    # [ROBUSTNESS] 强制打满 100% 进度消息
    if p3_prog.exists():
        try:
            data = json.loads(p3_prog.read_text(encoding='utf-8'))
            curr = data.get('current', 0)
            total = max(1, data.get('total', 1))
            is_done = data.get('status') == 'completed' or curr >= total
            log_pipeline({
                'run_id': current_run_id,
                'step': 'index_exporter', 
                'status': 'running', 
                'msg': "✅ P3 导出完成" if is_done else f"⚠️ P3 意外终止 ({curr}/{total})", 
                'percent': 100,
                'detailed_html': data.get('detail_html')
            })
        except: pass

    if p3_prog.exists():
        try: mission_stats['p3'] = json.loads(p3_prog.read_text(encoding='utf-8'))
        except: pass

    # 5. Mission Summary
    total_duration = time.time() - start_all
    sum_html = (
        f"🎉 <b>任务全线完成</b><br>"
        f"━━━━━━━━━━━━━━<br>"
        f"⏱️ <b>总耗时</b>: {total_duration/60:.1f} 分钟<br>"
        f"🏁 <b>状态</b>: 成功<br>"
        f"━━━━━━━━━━━━━━<br>"
        f"✅ P1-P3 流水线已顺利跑完。所有索引文档已更新至 <code>docs/tags/</code>。"
    )
    log_pipeline({
        'run_id': current_run_id,
        'step': 'summary', 
        'status': 'completed', 
        'msg': '✨ 任务流运行结束', 
        'percent': 100,
        'detailed_html': sum_html
    })

    # 5. 生成总结报告 (Stylized Mission Report)
    end_time = time.strftime("%H:%M:%S")
    report = []
    
    # 标题部分
    report.append(f"📊 **流水线任务报告 【任务完成】**")
    report.append(f"━━━━━━━━━━━━━━")
    report.append(f"🆔 任务编号: {current_run_id.replace('run_', '#R')}")
    
    # 提取信息
    p1 = mission_stats.get('p1', {})
    p2 = mission_stats.get('p2', {})
    p3 = mission_stats.get('p3', {})
    
    p1_scan = p1.get('total_msgs', 0) if p1 else 0
    p1_found = p1.get('candidate_count', 0) if p1 else 0
    p2_total = p2.get('total', 0) if p2 else 0
    p2_updated = p2.get('updated', 0) if p2 else 0
    p3_total = p3.get('total', 0) if p3 else 0
    
    report.append(f"📊 统计概览: P1 发现 {p1_found} 个新词 / P2 更新 {p2_updated} 条记录 / P3 生成 {p3_total} 个文档")
    report.append(f"🗃️ 候选池扫描: {p1_scan} 条原始记录")
    report.append(f"⏰ 耗时: {mission_stats['start_time']} -> {end_time}")
    report.append(f"━━━━━━━━━━━━━━")
    
    report.append(f"📍 各子系统明细 (运行结果 | 数据量):")
    if p1:
        report.append(f"  ✅ [P1/Discovery] 挖掘完成: 新增 {p1_found} 个候选词 | {p1_scan} 扫描量")
    else:
        report.append(f"  ⏭️ [P1/Discovery] 增量判断: 现存词库已是最新，跳过扫描")
        
    if p2:
        report.append(f"  ✅ [P2/GlobalTagger] 打标完成: 更新 {p2_updated} 条 | 全量 {p2_total} 条")
    else:
        report.append(f"  ⚠️ [P2/GlobalTagger] 数据异常或尚未执行")
        
    if p3:
        report.append(f"  ✅ [P3/IndexExporter] 导出完成: 合计生成 {p3_total} 个频道文档")
    else:
        report.append(f"  ⚠️ [P3/IndexExporter] 数据异常或尚未执行")
        
    report.append(f"")
    report.append(f"✨ 全量打标已生效，索引文档已同步至 `docs/tags/{bot_name}/`。")

    with pipeline_lock:
        PIPELINE_LOG.append({
            'run_id': current_run_id, # 使用当前任务的 ID
            'step': 'done', 
            'status': 'done', 
            'msg': '🎉 完整更新流程已结束',
            'report': "\n".join(report)
        })
        _save_pipeline_log()

@app.route('/api/run_p2p3', methods=['POST'])
def api_run_p2p3():
    """触发全量打标导出 (实际上现在是全量 Pipeline)"""
    if not pipeline_active_lock.acquire(blocking=False):
        return jsonify({'ok': False, 'msg': '流水线已在运行中，请等结束后再试。'}), 429
    
    def wrapped_worker():
        try:
            run_full_pipeline_worker()
        finally:
            pipeline_active_lock.release()

    t = threading.Thread(target=wrapped_worker, daemon=True)
    t.start()
    return jsonify({'ok': True, 'msg': 'Pipeline 已在后台启动'})

@app.route('/api/reload_candidates', methods=['POST'])
def api_reload_candidates():
    """从磁盘重新加载候选池和样本到内存缓存（开发/调试用）。"""
    global CANDIDATES_CACHE, SAMPLES_CACHE
    try:
        CANDIDATES_CACHE = load_candidates(CONFIG)
        SAMPLES_CACHE = load_samples(CONFIG)
        return jsonify({'ok': True, 'total': len(CANDIDATES_CACHE)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ─── 主入口 ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bot', default='tgporncopilot')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--no-browser', action='store_true')
    args = parser.parse_args()
    
    global CONFIG, CANDIDATES_CACHE, SAMPLES_CACHE, CATEGORIES
    CONFIG = get_bot_config(args.bot)
    CANDIDATES_CACHE = load_candidates(CONFIG)
    SAMPLES_CACHE = load_samples(CONFIG)
    CATEGORIES = load_categories(CONFIG)
    
    print(f"🔬 [P1.5 分拣工具] Bot: {CONFIG['app_name']} | 候选词: {len(CANDIDATES_CACHE)} 条 | 分类: {len(CATEGORIES)} 个")
    print(f"🌐 访问地址: http://localhost:{args.port}")
    
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(f'http://localhost:{args.port}')).start()
    
    app.run(host='0.0.0.0', port=args.port, debug=False, threaded=True)


if __name__ == '__main__':
    main()
