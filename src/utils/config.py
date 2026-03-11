import os
import sys
import argparse
from dotenv import load_dotenv

# [FIX] 解决 Windows 控制台 GBK 编码无法打印 Emoji 的问题
if sys.platform == "win32":
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except:
        pass

# 尝试从项目根目录加载 .env
# config.py 位于 src/utils/，三级往上是根目录
_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_dotenv_path = os.path.join(_base_dir, '.env')
if os.path.exists(_dotenv_path):
    print(f"[INIT] Loading config from: {_dotenv_path}")
    load_dotenv(_dotenv_path, override=True)
else:
    load_dotenv(override=True) # Fallback to CWD

def safe_int(val, default=None):
    """安全地将字符串转为整数，失败则返回 None 并打印警告"""
    if not val:
        return default
    try:
        # 去除非数字字符（Telegram ID 中可能包含 - 符号）
        val_clean = str(val).split('#')[0].strip() # 过滤掉可能的注释
        return int(val_clean)
    except (ValueError, TypeError):
        print(f"⚠️ 警告: 配置值 '{val}' 无法转换为整数，已设为 None。请检查 .env。")
        return default

def get_bot_config(bot_identifier=None):
    """解析全局配置，返回当前运行的 Bot 身份隔离设置"""
    if not bot_identifier:
        parser = argparse.ArgumentParser(description="TGPornCopilot Server")
        parser.add_argument('--bot', type=str, default='tgporncopilot',
                            help="指定启动的 Bot 身份 (tgporncopilot 或 my_porn_private_bot)")
        
        # 允许被其他模块导入时不解析命令行 args
        try:
            args, _ = parser.parse_known_args()
            bot_identifier = args.bot
        except:
            bot_identifier = 'tgporncopilot'

    # 定义双 Bot 配置大一统
    bot_configs = {
        'tgporncopilot': {
            'app_name': 'tgporncopilot',
            'bot_token': os.getenv('BOT_TOKEN'),
            'target_group_id': safe_int(os.getenv('TARGET_GROUP_ID')),
            'source_channels': [c.strip() for c in os.getenv('SOURCE_CHANNELS', '').split(',') if c.strip()],
            'managed_folders': [f.strip() for f in os.getenv('MANAGED_FOLDERS', '极品捆绑, 精品捆绑, 整理').split(',') if f.strip()],
            'entities_dir_data': 'data/entities/tgporncopilot',
            'entities_dir_docs': 'docs/entities/tgporncopilot',
            'candidates_dir_data': 'data/entities/tgporncopilot/candidates',
            'candidates_dir_docs': 'docs/entities/tgporncopilot/candidates',
            'currententities_dir_data': 'data/entities/tgporncopilot/currententities',
            'currententities_dir_docs': 'docs/entities/tgporncopilot/currententities',
        },
        'my_porn_private_bot': {
            'app_name': 'my_porn_private_bot',
            'bot_token': os.getenv('MY_PORN_PRIVATE_BOT_TOKEN') or os.getenv('MY_BDSM_PRIVATE_BOT_TOKEN'),
            'target_group_id': safe_int(os.getenv('MY_PORN_PRIVATE_BOT_TARGET_GROUP_ID') or os.getenv('MY_BDSM_PRIVATE_BOT_TARGET_GROUP_ID')),
            'source_channels': [c.strip() for c in (os.getenv('MY_PORN_PRIVATE_BOT_SOURCE_CHANNELS') or os.getenv('MY_BDSM_PRIVATE_BOT_SOURCE_CHANNELS') or '').split(',') if c.strip()],
            'managed_folders': [f.strip() for f in os.getenv('MY_PORN_PRIVATE_BOT_MANAGED_FOLDERS', '较少捆绑AV/实拍/定制, 已归档, 未分组, 杂文件, 约/较少捆绑中精品').split(',') if f.strip()],
            'entities_dir_data': 'data/entities/my_porn_private_bot',
            'entities_dir_docs': 'docs/entities/my_porn_private_bot',
            'candidates_dir_data': 'data/entities/my_porn_private_bot/candidates',
            'candidates_dir_docs': 'docs/entities/my_porn_private_bot/candidates',
            'currententities_dir_data': 'data/entities/my_porn_private_bot/currententities',
            'currententities_dir_docs': 'docs/entities/my_porn_private_bot/currententities',
        }
    }

    if bot_identifier not in bot_configs:
        # 兼容性别名: my_bdsm_private_bot -> my_porn_private_bot
        if bot_identifier == 'my_bdsm_private_bot' and 'my_porn_private_bot' in bot_configs:
            bot_identifier = 'my_porn_private_bot'
        else:
            available = ", ".join(bot_configs.keys())
            raise KeyError(f"未知的 Bot 身份标识: '{bot_identifier}'。可选范围: {available}")

    config = bot_configs[bot_identifier]
    # 公共常量
    config['api_id'] = safe_int(os.getenv('API_ID'))
    config['api_hash'] = os.getenv('API_HASH', '').strip()
    admin_val = os.getenv('ADMIN_USER_ID', '')
    config['admin_user_ids'] = [safe_int(i.strip()) for i in admin_val.split(',') if i.strip()]
    config['admin_user_id'] = config['admin_user_ids'][0] if config['admin_user_ids'] else None
    
    # 动态路径设置 (用于 Session 隔离)
    if config['app_name'] == 'tgporncopilot':
        config['bot_session'] = 'data/sessions/copilot_bot' # 维持主 Bot 的遗留 Session
    else:
        config['bot_session'] = f"data/sessions/{config['app_name']}_bot"
    
    return config

# 兼容性别名
load_config = get_bot_config
# 暴露一个默认的 CONFIG 实例给所有子模块
CONFIG = get_bot_config()
