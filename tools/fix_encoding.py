
import os

file_path = 'src/search_bot.py'
with open(file_path, 'rb') as f:
    raw = f.read()

# 尝试清理乱码：由于之前可能被 Latin-1 误读又写回，现在可能存在双重编码
# 但最稳妥的办法是：如果 raw 看起来像 UTF-8 编码的 Latin-1 字符串，还原它。
# 或者尝试直接用 utf-8 忽略错误读取。

try:
    content = raw.decode('utf-8')
    print("✅ Decoded as UTF-8")
except:
    try:
        content = raw.decode('gbk')
        print("✅ Decoded as GBK")
    except:
        content = raw.decode('utf-8', errors='ignore')
        print("⚠️ Decoded as UTF-8 (ignored errors)")

# 如果发现内容包含大量乱码（mojibake），尝试二次解码
if '猫庐' in content or '忙聙?' in content:
    try:
        # 尝试将乱码字符串编码为 latin-1 再解码为 utf-8
        content = content.encode('latin-1').decode('utf-8')
        print("♻️ Fixed Mojibake (latin-1 -> utf-8)")
    except:
        pass

# 确保引号冲突修复
content = content.replace('text += "\\n*(暂无名单，请先运行"更新词库")*"', 'text += "\\n*(暂无名单，请先运行\'更新词库\')*"')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ File rewritten.")
