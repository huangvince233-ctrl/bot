
import os

file_path = 'src/search_bot.py'
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 尝试还原: UTF-8 -> Latin-1 (bytes) -> UTF-8
    restored = content.encode('latin-1', errors='replace').decode('utf-8', errors='replace')
    
    if len(restored) > len(content) * 0.5: # 简单校验，防止全删了
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(restored)
        print("✅ Attempted restoration via latin-1 encode.")
    else:
        print("❌ Restoration would result in too much data loss.")
except Exception as e:
    print(f"❌ Error during restoration: {e}")
