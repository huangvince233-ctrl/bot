
import json
from pathlib import Path

# Fix server.py recursive delete logic
server_path = Path(r'f:\funny_project\tgporncopilot\tools\sorter\server.py')
content = server_path.read_text(encoding='utf-8')

# Define the old function as it appears in the file
# Note: I'll use a more robust regex-like search if possible, 
# but let's try a block replacement first.

target = """@app.route('/api/categories/delete', methods=['POST'])
def api_delete_category():
    body = request.json
    name = body.get('name')
    if not name or name == "未分类":
        return jsonify({'ok': False, 'error': '不能删除基础类目'})

    entities_path = PROJECT_ROOT / CONFIG['entities_json']
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
    return jsonify({'ok': False, 'error': '类目不存在'})"""

replacement = """@app.route('/api/categories/delete', methods=['POST'])
def api_delete_category():
    body = request.json
    name = body.get('name')
    if not name or name == "未分类":
        return jsonify({'ok': False, 'error': '不能删除基础类目'})

    entities_path = PROJECT_ROOT / CONFIG['entities_json']
    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        
        # 递归找以此为前缀的所有类目
        to_delete = []
        for k in kws.keys():
            if k == name or k.startswith(name + "/"):
                to_delete.append(k)
        
        if not to_delete:
            return jsonify({'ok': False, 'error': '该路径下未发现任何有效类目'})
            
        all_items = []
        for k in to_delete:
            all_items.extend(kws.pop(k))
            
        # 移动到未分类
        if "未分类" not in kws: kws["未分类"] = []
        kws["未分类"].extend(all_items)
        
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        global CATEGORIES
        CATEGORIES = load_categories(CONFIG)
        return jsonify({'ok': True, 'deleted_count': len(to_delete)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})"""

if target in content:
    new_content = content.replace(target, replacement)
    server_path.write_text(new_content, encoding='utf-8')
    print("Successfully replaced api_delete_category")
else:
    # Try a more flexible search if exact match fails
    print("Exact match failed, attempting partial replacement")
    # Just replace the try/except block inside api_delete_category
    inner_target = """    try:
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
    return jsonify({'ok': False, 'error': '类目不存在'})"""
    
    inner_replacement = """    try:
        data = json.loads(entities_path.read_text(encoding='utf-8'))
        kws = data.get('keywords', {})
        
        # 递归找以此为前缀的所有类目
        to_delete = []
        for k in kws.keys():
            if k == name or k.startswith(name + "/"):
                to_delete.append(k)
        
        if not to_delete:
            return jsonify({'ok': False, 'error': '该路径下未发现任何有效类目'})
            
        all_items = []
        for k in to_delete:
            all_items.extend(kws.pop(k))
            
        # 移动到未分类
        if "未分类" not in kws: kws["未分类"] = []
        kws["未分类"].extend(all_items)
        
        entities_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        global CATEGORIES
        CATEGORIES = load_categories(CONFIG)
        return jsonify({'ok': True, 'deleted_count': len(to_delete)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})"""
        
    if inner_target in content:
        new_content = content.replace(inner_target, inner_replacement)
        server_path.write_text(new_content, encoding='utf-8')
        print("Successfully replaced inner logic")
    else:
        print("Inner target also failed")
