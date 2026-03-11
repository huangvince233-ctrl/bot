import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from db import Database
from utils.config import CONFIG

def fix_orphaned_offsets():
    db = Database()
    bot_name = CONFIG.get('app_name', 'tgporncopilot')
    print(f"🛠️  正在为 Bot [{bot_name}] 修复残留的同步时间戳...")
    
    # 执行一次强制全量重计，它会把没有消息关联的断点时间全部设为 NULL
    db._recalc_counters(is_test=True, bot_name=bot_name)
    db._recalc_counters(is_test=False, bot_name=bot_name)
    
    print("✅ 修复完成！现在 UI 里的「暂无同步记录」应该已经恢复正常了。")

if __name__ == "__main__":
    fix_orphaned_offsets()
