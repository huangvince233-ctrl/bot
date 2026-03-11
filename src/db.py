import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
import sqlite3
from pathlib import Path

class Database:
    def __init__(self, db_path='data/copilot.db'):
        self.conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._create_tables()
        self._migrate()

    def _normalize_id(self, chat_id):
        """统一 ID 为 10 位绝对值格式，用于索引去重 (避免 -100 前缀差异)"""
        if chat_id is None: return None
        return abs(int(chat_id)) % 1000000000000

    def _create_tables(self):
        # 同步记录表：每次同步一条记录
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                is_test INTEGER DEFAULT 1,
                formal_number INTEGER DEFAULT NULL,
                start_time TEXT,
                end_time TEXT,
                duration TEXT,
                groups_count INTEGER DEFAULT 0,
                videos_count INTEGER DEFAULT 0,
                photos_count INTEGER DEFAULT 0,
                files_count INTEGER DEFAULT 0,
                gifs_count INTEGER DEFAULT 0,
                links_count INTEGER DEFAULT 0,
                previews_count INTEGER DEFAULT 0,
                texts_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                start_msg_id INTEGER DEFAULT NULL,
                end_msg_id INTEGER DEFAULT NULL,
                bot_name TEXT DEFAULT 'tgporncopilot'
            )
        ''')
        
        # 针对 v9.1 双 Bot 升级：尝试为旧表追加 bot_name 列
        try:
            self.cursor.execute("ALTER TABLE sync_runs ADD COLUMN bot_name TEXT DEFAULT 'tgporncopilot'")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass # 列已存在
            
        # 针对 v9.13 跨群隔离：尝试追加 target_group_id
        try:
            self.cursor.execute("ALTER TABLE sync_runs ADD COLUMN target_group_id INTEGER DEFAULT NULL")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        # 消息映射表：original -> forwarded
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_run_id INTEGER,
                msg_type TEXT,
                original_msg_id INTEGER,
                original_chat_id INTEGER,
                forwarded_msg_id INTEGER,
                forwarded_chat_id INTEGER,
                res_id INTEGER,
                res_photo_id INTEGER,
                res_video_id INTEGER,
                res_other_id INTEGER,
                res_text_id INTEGER,
                header_msg_id INTEGER DEFAULT 0,
                FOREIGN KEY (sync_run_id) REFERENCES sync_runs(run_id)
            )
        ''')
        
        # 针对 v9.14: 追加 header_msg_id 列
        try:
            self.cursor.execute("ALTER TABLE messages ADD COLUMN header_msg_id INTEGER DEFAULT 0")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass


        # 目标群组注册表 [NEW]
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS target_groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                is_active BOOLEAN DEFAULT 0,
                bot_name TEXT
            )
        ''')

        # 同步断点偏移量 (per-chat, per-env)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_offsets (
                chat_id INTEGER,
                is_test INTEGER DEFAULT 0,
                last_msg_id INTEGER DEFAULT 0,
                updated_at TIMESTAMP,
                last_run_id INTEGER DEFAULT NULL,
                PRIMARY KEY (chat_id, is_test)
            )
        ''')
        # 针对 v9.15: 追加 last_run_id 列
        try:
            self.cursor.execute("ALTER TABLE sync_offsets ADD COLUMN last_run_id INTEGER DEFAULT NULL")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        # 备份断点/时间戳偏移量 (per-chat, per-env)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_offsets (
                chat_id INTEGER,
                last_msg_id INTEGER DEFAULT 0,
                updated_at TIMESTAMP,
                is_test INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, is_test)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_messages (
                chat_id INTEGER,
                chat_name TEXT,
                msg_id INTEGER,
                msg_type TEXT,
                sender_name TEXT,
                original_time TEXT,
                text_content TEXT,
                file_name TEXT,
                media_group_id TEXT,
                res_id INTEGER DEFAULT NULL,
                res_photo_id INTEGER DEFAULT NULL,
                res_video_id INTEGER DEFAULT NULL,
                res_gif_id INTEGER DEFAULT NULL,
                res_link_id INTEGER DEFAULT NULL,
                res_link_msg_id INTEGER DEFAULT NULL,
                res_preview_id INTEGER DEFAULT NULL,
                res_other_id INTEGER DEFAULT NULL,
                res_text_id INTEGER DEFAULT NULL,
                res_msg_id INTEGER DEFAULT NULL,
                search_tags TEXT DEFAULT NULL,
                is_extracted INTEGER DEFAULT 0,
                creator TEXT DEFAULT NULL,
                actor TEXT DEFAULT NULL,
                keywords TEXT DEFAULT NULL,
                supplement TEXT DEFAULT NULL,
                PRIMARY KEY (chat_id, msg_id)
            )
        ''')

        # 资源计数器表 (改为按频道独立计数)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS resource_counters (
                chat_id INTEGER,
                counter_key TEXT,
                last_value INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, counter_key)
            )
        ''')

        # 备份记录表：每次备份一条记录 (类似 sync_runs)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                is_test INTEGER DEFAULT 1,
                formal_number INTEGER DEFAULT NULL,
                start_time TEXT,
                end_time TEXT,
                duration TEXT,
                total_channels INTEGER DEFAULT 0,
                total_messages INTEGER DEFAULT 0,
                new_messages INTEGER DEFAULT 0,
                backup_mode TEXT,
                is_incremental INTEGER DEFAULT 0,
                channels_detail TEXT DEFAULT NULL,
                bot_name TEXT DEFAULT 'tgporncopilot'
            )
        ''')

        # 针对 v9.1 双 Bot 升级：尝试为旧表追加 bot_name 列
        try:
            self.cursor.execute("ALTER TABLE backup_runs ADD COLUMN bot_name TEXT DEFAULT 'tgporncopilot'")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass # 列已存在

        # 频道名称映射表 (用于锁定本地目录名，防止改名导致断层)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel_names (
                chat_id INTEGER PRIMARY KEY,
                canonical_name TEXT,
                latest_name TEXT
            )
        ''')

        # 实体审核表 (用于检索模式：创作者、演员、工作室等)
        # status: 0=待审, 1=已确认, 2=屏蔽
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                type TEXT,
                status INTEGER DEFAULT 0,
                msg_count INTEGER DEFAULT 0,
                UNIQUE(name, type)
            )
        ''')
        
        self.conn.commit()

    def _migrate(self):
        # 增加 forwarded_chat_id [v2.0]
        try:
            self.cursor.execute('ALTER TABLE messages ADD COLUMN forwarded_chat_id INTEGER')
        except:
            pass
        
        # 增加 sync_runs 的扩展字段 [v3.0 跨群显示与编号稳定]
        try:
            self.cursor.execute('ALTER TABLE sync_runs ADD COLUMN test_number INTEGER DEFAULT NULL')
        except: pass
        try:
            self.cursor.execute('ALTER TABLE sync_runs ADD COLUMN target_group_id INTEGER DEFAULT NULL')
        except: pass

        self.conn.commit()
        """兼容旧数据库：如果 videos 表存在，保留它"""
        try:
            self.cursor.execute("ALTER TABLE messages ADD COLUMN file_name TEXT")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
            
        all_res_cols = ['res_id', 'res_photo_id', 'res_video_id', 'res_gif_id', 'res_link_id', 'res_link_msg_id', 'res_preview_id', 'res_other_id', 'res_text_id', 'res_msg_id']
        for col in all_res_cols:
            for table in ['global_messages', 'messages']:
                try:
                    self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} INTEGER DEFAULT NULL")
                    self.conn.commit()
                except sqlite3.OperationalError:
                    pass
                    
        # Migrate: search tags and extraction status
        for col in ['search_tags', 'is_extracted', 'creator', 'actor', 'keywords', 'supplement']:
            try:
                ctype = "INTEGER DEFAULT 0" if col == 'is_extracted' else "TEXT DEFAULT NULL"
                self.cursor.execute(f"ALTER TABLE global_messages ADD COLUMN {col} {ctype}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass
            
        # Migrate: create channel_names table if it doesn't exist (safety for older DBs)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel_names (
                chat_id INTEGER PRIMARY KEY,
                canonical_name TEXT,
                latest_name TEXT
            )
        ''')
        self.conn.commit()
            
        for col in ['gifs_count', 'links_count', 'previews_count', 'res_msgs_count']:
            try:
                self.cursor.execute(f"ALTER TABLE sync_runs ADD COLUMN {col} INTEGER DEFAULT 0")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass

        for col, ctype in [('backup_mode', 'TEXT'), ('is_incremental', 'INTEGER DEFAULT 0'), ('new_messages', 'INTEGER DEFAULT 0')]:
            try:
                self.cursor.execute(f"ALTER TABLE backup_runs ADD COLUMN {col} {ctype}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass
        
        # 迁移 resource_counters 表结构 (如果还是旧的只有 counter_key 的结构)
        try:
            self.cursor.execute("SELECT chat_id FROM resource_counters LIMIT 1")
        except sqlite3.OperationalError:
            # 结构不兼容，直接删了重建（反正用户说要清空数据）
            self.cursor.execute("DROP TABLE IF EXISTS resource_counters")
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resource_counters (
                    chat_id INTEGER,
                    counter_key TEXT,
                    last_value INTEGER DEFAULT 0,
                    PRIMARY KEY (chat_id, counter_key)
                )
            ''')
            self.conn.commit()

        # 兼容性检查：如果 sync_offsets 的主键不是复合主键，则重构表结构
        try:
            # 获取当前主键列名
            pk_cols = [c[1] for c in self.cursor.execute("PRAGMA table_info(sync_offsets)").fetchall() if c[5] > 0]
            if len(pk_cols) < 2:
                print("⚙️ Upgrading sync_offsets table to support compound PK for environment isolation...")
                # 1. 临时重命名
                self.cursor.execute("ALTER TABLE sync_offsets RENAME TO sync_offsets_old")
                # 2. 创建新表
                self.cursor.execute('''
                    CREATE TABLE sync_offsets (
                        chat_id INTEGER,
                        is_test INTEGER DEFAULT 0,
                        last_msg_id INTEGER DEFAULT 0,
                        updated_at TIMESTAMP,
                        PRIMARY KEY (chat_id, is_test)
                    )
                ''')
                # 3. 迁移数据
                # 如果旧表只有 chat_id, last_msg_id, updated_at
                old_cols = [c[1] for c in self.cursor.execute("PRAGMA table_info(sync_offsets_old)").fetchall()]
                common_cols = [c for c in ['chat_id', 'last_msg_id', 'updated_at'] if c in old_cols]
                col_str = ", ".join(common_cols)
                self.cursor.execute(f"INSERT INTO sync_offsets ({col_str}, is_test) SELECT {col_str}, 0 FROM sync_offsets_old")
                # 4. 删除旧表
                self.cursor.execute("DROP TABLE sync_offsets_old")
                self.conn.commit()
                print("✅ sync_offsets table upgraded successfully.")
        except Exception as e:
            print(f"⚠️ Error migrating sync_offsets: {e}")
            pass

        try:
            self.cursor.execute("SELECT 1 FROM videos LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_msg_id INTEGER,
                    original_chat_id INTEGER,
                    forwarded_msg_id INTEGER,
                    creator TEXT,
                    description TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.conn.commit()

    # ===== 频道名称映射 =====
    def check_and_update_channel_name(self, chat_id, current_title):
        """
        检查频道名称是否发生变化，并更新数据库。
        返回 (旧名称, 新名称)。如果没变或首次记录，则旧名称与新名称相同。
        """
        if not current_title:
            current_title = str(chat_id)
            
        row = self.cursor.execute('SELECT latest_name FROM channel_names WHERE chat_id = ?', (chat_id,)).fetchone()
        if row:
            old_name = row[0]
            if old_name != current_title:
                self.cursor.execute('UPDATE channel_names SET latest_name = ? WHERE chat_id = ?', (current_title, chat_id))
                self.conn.commit()
                return old_name, current_title
            return current_title, current_title
        else:
            self.cursor.execute('''
                INSERT INTO channel_names (chat_id, canonical_name, latest_name) 
                VALUES (?, ?, ?)
            ''', (chat_id, current_title, current_title))
            self.conn.commit()
            return current_title, current_title

    # ===== 同步记录 =====
    def start_sync_run(self, is_test=True, bot_name='tgporncopilot', target_group_id=None):
        """开始一次同步，返回 run_id"""
        formal_num = None
        test_num = None
        if not is_test:
            row = self.cursor.execute(
                'SELECT MAX(formal_number) FROM sync_runs WHERE is_test = 0 AND bot_name = ?', (bot_name,)
            ).fetchone()
            formal_num = (row[0] + 1) if row and row[0] is not None else 1
        else:
            # [NEW] 为测试环境也分配持久化编号，防止回滚后编号漂移
            row = self.cursor.execute(
                'SELECT MAX(test_number) FROM sync_runs WHERE is_test = 1 AND bot_name = ?', (bot_name,)
            ).fetchone()
            test_num = (row[0] + 1) if row and row[0] is not None else 1
        
        self.cursor.execute('''
            INSERT INTO sync_runs (is_test, formal_number, test_number, start_time, bot_name, target_group_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1 if is_test else 0, formal_num, test_num, datetime.now().isoformat(), bot_name, target_group_id))
        self.conn.commit()
        return self.cursor.lastrowid

    def finish_sync_run(self, run_id, stats):
        """完成同步，更新统计"""
        self.cursor.execute('''
            UPDATE sync_runs SET 
                end_time = ?, duration = ?,
                groups_count = ?, videos_count = ?, photos_count = ?,
                files_count = ?, gifs_count = ?, links_count = ?, previews_count = ?,
                texts_count = ?, skipped_count = ?
            WHERE run_id = ?
        ''', (
            datetime.now().isoformat(), stats.get('duration', ''),
            stats.get('groups', 0), stats.get('videos', 0), stats.get('photos', 0),
            stats.get('files', 0), stats.get('gifs', 0), stats.get('links', 0), stats.get('previews', 0),
            stats.get('texts', 0), stats.get('skipped', 0),
            run_id
        ))
        self.conn.commit()

    def set_sync_run_boundaries(self, run_id, start_msg_id, end_msg_id):
        """记录该次同步在目标群组产生的起始与结束的消息 ID，为回滚提供物理切除边界"""
        self.cursor.execute('''
            UPDATE sync_runs SET start_msg_id = ?, end_msg_id = ? WHERE run_id = ?
        ''', (start_msg_id, end_msg_id, run_id))
        
        # 1. 先查基础的 MIN/MAX
        res_info = list(self.cursor.execute('''
            SELECT MIN(res_id), MAX(res_id),
                   MIN(res_video_id), MAX(res_video_id),
                   MIN(res_photo_id), MAX(res_photo_id),
                   MIN(res_gif_id), MAX(res_gif_id),
                   MIN(res_other_id), MAX(res_other_id),
                   MIN(res_preview_id), MAX(res_preview_id),
                   NULL, NULL, -- res_link_id 位置先置空，后续单独计算
                   MIN(res_link_msg_id), MAX(res_link_msg_id),
                   MIN(res_text_id), MAX(res_text_id),
                   MIN(res_msg_id), MAX(res_msg_id)
            FROM messages WHERE sync_run_id = ?
        ''', (run_id,)).fetchone())
        
        # 2. 针对 res_link_id 这个字符串字段，单独拉出所有值并解析真实数值范围
        links = self.cursor.execute('SELECT res_link_id FROM messages WHERE sync_run_id = ? AND res_link_id IS NOT NULL', (run_id,)).fetchall()
        if links:
            all_nums = []
            for (l_str,) in links:
                if '-' in str(l_str):
                    parts = str(l_str).split('-')
                    for p in parts:
                        if p.isdigit(): all_nums.append(int(p))
                elif str(l_str).isdigit():
                    all_nums.append(int(l_str))
            if all_nums:
                res_info[12] = min(all_nums) # min_link_id
                res_info[13] = max(all_nums) # max_link_id

        self.conn.commit()
        return tuple(res_info)

    def get_recent_sync_runs(self, is_test=None, limit=50, bot_name=None):
        """获取最近的同步记录用于交互菜单展示 (按 Bot 过滤)"""
        query = 'SELECT run_id, start_time, is_test, formal_number, bot_name FROM sync_runs'
        params = []
        conditions = []
        
        if is_test is not None:
            conditions.append('is_test = ?')
            params.append(1 if is_test else 0)
            
        if bot_name:
            if bot_name == 'tgporncopilot':
                conditions.append('(bot_name = ? OR bot_name IS NULL)')
            else:
                conditions.append('bot_name = ?')
            params.append(bot_name)
            
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        
        query += ' ORDER BY run_id DESC LIMIT ?'
        params.append(limit)
        
        rows = self.cursor.execute(query, params).fetchall()
        
        results = []
        for run_id, start_time, is_test, formal_number, b_name in rows:
            label = self.get_run_label(run_id)
            try:
                dt = datetime.fromisoformat(start_time)
                time_str = dt.strftime("%m-%d %H:%M")
            except:
                time_str = "未知时间"
            results.append((label, time_str))
        return results

    def get_run_label(self, run_id):
        """获取同步标签：使用持久化的编号"""
        row = self.cursor.execute(
            'SELECT is_test, formal_number, test_number, bot_name FROM sync_runs WHERE run_id = ?', (run_id,)
        ).fetchone()
        if not row:
            return f"RUN-{run_id}"
            
        is_test, formal_num, test_num, bot_name = row
        prefix = "P" if bot_name == 'my_porn_private_bot' else ""

        if is_test:
            # 优先使用持久化的 test_number，如果没有（旧数据），则按顺序查找
            if test_num is not None:
                return f"{prefix}TEST-{test_num}"
            
            count = self.cursor.execute(
                'SELECT COUNT(*) FROM sync_runs WHERE is_test = 1 AND (bot_name = ? OR (bot_name IS NULL AND ?="tgporncopilot")) AND run_id <= ?', 
                (bot_name, bot_name, run_id)
            ).fetchone()[0]
            if count == 0: count = 1
            return f"{prefix}TEST-{count}"
            
        if formal_num is None:
            return f"{prefix}RUN-{run_id}"
        return f"{prefix}#{formal_num}"

    # ===== 备份记录 =====
    def start_backup_run(self, mode, is_incremental, is_test=True, bot_name='tgporncopilot'):
        """开始一次备份，返回 run_id"""
        formal_num = None
        effective_incremental = bool(is_incremental)
        if not is_test:
            row = self.cursor.execute(
                'SELECT MAX(formal_number) FROM backup_runs WHERE is_test = 0 AND bot_name = ?', (bot_name,)
            ).fetchone()
            if row[0] is None:
                formal_num = 1  # 每个 bot 都从 1 开始，不共用 B0
                # 首个正式备份天然应视为全量基线，不允许写成增量
                effective_incremental = False
            else:
                formal_num = row[0] + 1

        self.cursor.execute('''
            INSERT INTO backup_runs (is_test, formal_number, start_time, backup_mode, is_incremental, bot_name)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (1 if is_test else 0, formal_num, datetime.now().isoformat(), str(mode), 1 if effective_incremental else 0, bot_name))
        self.conn.commit()
        return self.cursor.lastrowid

    def finish_backup_run(self, run_id, stats):
        """完成备份，更新统计"""
        import json
        self.cursor.execute('''
            UPDATE backup_runs SET 
                end_time = ?, duration = ?,
                total_channels = ?, total_messages = ?,
                new_messages = ?,
                channels_detail = ?
            WHERE run_id = ?
        ''', (
            datetime.now().isoformat(), stats.get('duration', ''),
            stats.get('total_channels', 0), stats.get('total_messages', 0),
            stats.get('new_messages', 0),
            json.dumps(stats.get('channels', []), ensure_ascii=False),
            run_id
        ))
        self.conn.commit()

    def get_backup_label(self, run_id):
        """获取备份标签：主 Bot '#B1' '#B2', 副 Bot 'P#B1' 'P#B2'"""
        row = self.cursor.execute(
            'SELECT is_test, formal_number, bot_name FROM backup_runs WHERE run_id = ?', (run_id,)
        ).fetchone()
        if not row:
            return f"BKUP-{run_id}"
            
        is_test, formal_num, bot_name = row or ('', None, 'tgporncopilot')
        prefix = "P" if (bot_name or '') == 'my_porn_private_bot' else ""
        
        if is_test:
            count = self.cursor.execute(
                'SELECT COUNT(*) FROM backup_runs WHERE is_test = 1 AND (bot_name = ? OR bot_name IS NULL) AND run_id <= ?',
                (bot_name or 'tgporncopilot', run_id)
            ).fetchone()[0]
            if count == 0: count = 1
            return f"{prefix}TEST-B{count}"
            
        if formal_num is None:
            return f"{prefix}BKUP-{run_id}"
        return f"{prefix}#B{formal_num}"  # 主: #B1, #B2; 副: P#B1, P#B2

    def get_bot_latest_backup_label(self, bot_name):
        """获取指定 Bot 最新的备份标签 (兼容别名)"""
        names = [bot_name]
        # 别名兼容逻辑
        if bot_name == 'my_porn_private_bot': names.append('my_bdsm_private_bot')
        elif bot_name == 'my_bdsm_private_bot': names.append('my_porn_private_bot')
        
        placeholders = ', '.join(['?'] * len(names))
        row = self.cursor.execute(
            f'SELECT run_id FROM backup_runs WHERE bot_name IN ({placeholders}) AND end_time IS NOT NULL ORDER BY run_id DESC LIMIT 1',
            tuple(names)
        ).fetchone()
        if not row: return "NONE"
        return self.get_backup_label(row[0])


    def update_backup_offset(self, chat_id, last_msg_id, is_test=0):
        """更新频道备份断点和时间戳"""
        norm_id = self._normalize_id(chat_id)
        self.cursor.execute('''
            INSERT INTO backup_offsets (chat_id, last_msg_id, updated_at, is_test)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, is_test) DO UPDATE SET
                last_msg_id = excluded.last_msg_id,
                updated_at = excluded.updated_at
        ''', (norm_id, last_msg_id, datetime.now().isoformat(), 1 if is_test else 0))
        self.conn.commit()

    def get_backup_offset(self, chat_id, is_test=0):
        """获取特定频道的备份断点"""
        norm_id = self._normalize_id(chat_id)
        res = self.cursor.execute(
            'SELECT last_msg_id FROM backup_offsets WHERE chat_id = ? AND is_test = ?',
            (norm_id, 1 if is_test else 0)
        ).fetchone()
        return res[0] if res else 0

    def get_last_offset(self, chat_id, is_test=False):
        t_val = 1 if is_test else 0
        norm_id = self._normalize_id(chat_id)
        row = self.cursor.execute(
            'SELECT last_msg_id FROM sync_offsets WHERE chat_id = ? AND is_test = ?', 
            (norm_id, t_val)
        ).fetchone()
        return row[0] if row else 0

    def get_all_backup_offsets(self):
        """获取所有频道的备份时间，返回 {chat_id: latest_time}"""
        rows = self.cursor.execute('SELECT chat_id, MAX(updated_at) FROM backup_offsets GROUP BY chat_id').fetchall()
        return {r[0]: r[1] for r in rows}

    def get_all_sync_offsets(self):
        """获取所有频道的同步时间，返回 {chat_id: latest_time}"""
        # 从 sync_offsets 或 sync_runs 关联获取也可以，这里优先查 sync_offsets
        rows = self.cursor.execute('SELECT chat_id, MAX(updated_at) FROM sync_offsets GROUP BY chat_id').fetchall()
        return {r[0]: r[1] for r in rows}

    def get_sync_info_per_group(self, chat_id, is_test=False):
        """
        获取某个频道在所有不同目标群组下的最新同步信息。
        用于跨群聚合展示。
        返回 list: [{group_title, label, time, target_group_id}]
        """
        istest_val = 1 if is_test else 0
        # 1. 找出该频道参与过的所有 sync_run 过往记录 (通过 messages 表关联)
        # 我们按 target_group_id 聚类，取每个群组下最新的 run_id
        sql = '''
            SELECT tg.title as group_title, sr.run_id, sr.start_time, sr.target_group_id
            FROM sync_runs sr
            JOIN messages m ON m.sync_run_id = sr.run_id
            LEFT JOIN target_groups tg ON tg.chat_id = sr.target_group_id
            WHERE sr.is_test = ? 
              AND (m.original_chat_id = ? OR ABS(m.original_chat_id) % 1000000000000 = ABS(?) % 1000000000000)
            GROUP BY sr.target_group_id
            ORDER BY sr.run_id DESC
        '''
        rows = self.cursor.execute(sql, (istest_val, chat_id, chat_id)).fetchall()
        
        results = []
        for r_title, r_id, r_time, r_tgid in rows:
            results.append({
                'group_title': r_title or "默认群组(或其他)",
                'label': self.get_run_label(r_id),
                'time': r_time,
                'target_group_id': r_tgid
            })
        return results

    # ===== 状态查询 (树状图用) =====
    def get_latest_sync_info(self, chat_id=None, is_test=False, bot_name=None):
        """
        查询最近一次同步信息（区分测试与正式环境）。
        chat_id=None: 查全局最近记录
        chat_id=具体值: 查某频道最近一次同步耗时
        bot_name: 可选，按 Bot 身份过滤
        返回 dict: {label, time, run_id} 或 None
        """
        istest_val = 1 if is_test else 0
        if chat_id is None:
            row = self.cursor.execute('''
                SELECT run_id, formal_number, start_time FROM sync_runs 
                WHERE is_test = ? ORDER BY run_id DESC LIMIT 1
            ''', (istest_val,)).fetchone()
            if row:
                label = self.get_run_label(row[0])
                return {'run_id': row[0], 'label': label, 'time': row[2]}
        else:
            # [V2 Fix] 统一使用规格化 ID 查找
            norm_id = self._normalize_id(chat_id)
            off = self.cursor.execute(
                '''SELECT last_run_id, updated_at FROM sync_offsets 
                   WHERE is_test = ? AND chat_id = ?''',
                (istest_val, norm_id)
            ).fetchone()
            
            if off and off[0]:
                label = self.get_run_label(off[0])
                return {'run_id': off[0], 'label': label, 'time': off[1]}

            # Fallback: 从 messages 中查 (用于由 V1 升级上来的历史记录兼容)
            sql = '''
                SELECT sr.run_id, sr.formal_number, sr.start_time
                FROM sync_runs sr
                JOIN messages m ON m.sync_run_id = sr.run_id
                WHERE sr.is_test = ? AND (m.original_chat_id = ? OR ABS(m.original_chat_id) % 1000000000000 = ABS(?) % 1000000000000)
            '''
            params = [istest_val, chat_id, chat_id]
            if bot_name:
                sql += " AND sr.bot_name = ?"
                params.append(bot_name)
            
            sql += " ORDER BY sr.run_id DESC LIMIT 1"
            row = self.cursor.execute(sql, params).fetchone()
            if row:
                label = self.get_run_label(row[0])
                return {'run_id': row[0], 'label': label, 'time': row[2]}
        
        return None

    def get_latest_backup_info(self, chat_id=None, is_test=None, bot_name=None):
        """
        查询最近一次备份信息。
        chat_id=None: 全局最近
        chat_id=具体值: 某频道最近一次备份 label 和时间
        bot_name: 可选，按 Bot 身份过滤
        """
        import json
        if chat_id is None:
            # 原有逻辑：找最近的一个 run
            istest_clause = "WHERE is_test = ?" if is_test is not None else ""
            params = (1 if is_test else 0,) if is_test is not None else ()
            row = self.cursor.execute(f'''
                SELECT run_id, formal_number, start_time FROM backup_runs 
                {istest_clause} ORDER BY run_id DESC LIMIT 1
            ''', params).fetchone()
            if row:
                return {'run_id': row[0], 'label': self.get_backup_label(row[0]), 'time': row[2]}
        else:
            # [IMPROVEMENT-v9.5] 更加健壮的 JSON ID 匹配逻辑 (兼容数字和字符串)
            p1 = f'%"id": {chat_id}%'
            p2 = f'%"id": "{chat_id}"%'
            
            sql = 'SELECT run_id, start_time FROM backup_runs WHERE (channels_detail LIKE ? OR channels_detail LIKE ?)'
            params = [p1, p2]
            if bot_name:
                sql += " AND bot_name = ?"
                params.append(bot_name)
                
            sql += " ORDER BY run_id DESC LIMIT 1"
            row = self.cursor.execute(sql, params).fetchone()
            
            if row:
                return {
                    'run_id': row[0],
                    'label': self.get_backup_label(row[0]),
                    'time': row[1]
                }

            # [REFINED] 2. 兜底逻辑：扫描全部 run 的 channels_detail (兼容老记录格式)
            all_rows = self.cursor.execute(
                'SELECT run_id, start_time FROM backup_runs WHERE is_test = 0 ORDER BY run_id DESC LIMIT 50'
            ).fetchall()
            for row in all_rows:
                rid, rtime = row
                detail_row = self.cursor.execute(
                    'SELECT channels_detail, bot_name FROM backup_runs WHERE run_id = ?', (rid,)
                ).fetchone()
                if not detail_row: continue
                detail_str, rb_name = detail_row
                if bot_name and rb_name and rb_name != bot_name: continue
                if detail_str:
                    import json as _json
                    try:
                        channels = _json.loads(detail_str)
                        for ch in (channels if isinstance(channels, list) else []):
                            ch_id = ch.get('id') or ch.get('chat_id')
                            if ch_id and (str(ch_id) == str(chat_id) or ch_id == chat_id):
                                return {
                                    'run_id': rid,
                                    'label': self.get_backup_label(rid),
                                    'time': rtime
                                }
                    except: pass
        return None

    def get_manageable_backup_runs(self, limit=20, bot_name=None):
        """获取最近的备份记录，用于展示在删除菜单中 (可选按 Bot 过滤)"""
        query = '''
            SELECT run_id, start_time, is_test, formal_number, channels_detail, backup_mode, is_incremental, total_messages, new_messages
            FROM backup_runs 
        '''
        params = []
        if bot_name:
            if bot_name == 'tgporncopilot':
                # 主 Bot：包含 NULL 旧记录（迁移前存储的 B0 等）
                query += " WHERE (bot_name = ? OR bot_name IS NULL) "
            else:
                query += " WHERE bot_name = ? "
            params.append(bot_name)
        
        query += " ORDER BY run_id DESC LIMIT ? "
        params.append(limit)
        
        rows = self.cursor.execute(query, tuple(params)).fetchall()
        
        runs = []
        for r in rows:
            label = self.get_backup_label(r[0])
            is_first_formal_baseline = (not bool(r[2])) and (r[3] == 1)
            effective_incremental = bool(r[6]) and not is_first_formal_baseline
            effective_new_messages = r[8] or 0
            if is_first_formal_baseline and r[7]:
                effective_new_messages = r[7]
            runs.append({
                'run_id': r[0],
                'time': r[1],
                'is_test': bool(r[2]),
                'label': label,
                'channels': json.loads(r[4]) if r[4] else [],
                'mode': r[5],
                'incremental': effective_incremental,
                'total_messages': r[7],
                'new_messages': effective_new_messages,
                'raw_incremental': bool(r[6]),
                'is_first_formal_baseline': is_first_formal_baseline
            })
        return runs

    def delete_backup_run(self, run_id):
        """删除特定的备份运行记录"""
        self.cursor.execute('DELETE FROM backup_runs WHERE run_id = ?', (run_id,))
        self.conn.commit()
        return True

    def clear_all_backup_runs(self, bot_name=None):
        """全量清空备份运行记录，并重置所有资源编号计数器"""
        try:
            # 1. 清除运行记录表
            self.cursor.execute('DELETE FROM backup_runs')
        except sqlite3.OperationalError: pass
        
        # 2. 如果指定了 bot_name，我们只重置该 Bot 管辖频道的计数器 (更安全)
        # 注意：这里我们全量清理计数器，因为用户明确要求“编号正常”
        try:
            self.cursor.execute('DELETE FROM resource_counters')
        except sqlite3.OperationalError: pass
        
        try:
            # 3. 抹除消息表中的已分配编号 (这样下次备份才会重新触发 assign_resource_ids)
            # 在全量清理模式下，我们需要让所有消息都失去编号，以便重新分配
            self.cursor.execute('''
                UPDATE global_messages SET 
                    res_id=NULL, res_photo_id=NULL, res_video_id=NULL, res_gif_id=NULL,
                    res_link_id=NULL, res_link_msg_id=NULL, res_preview_id=NULL, res_other_id=NULL, res_text_id=NULL, res_msg_id=NULL
            ''')
            self.cursor.execute('''
                UPDATE messages SET 
                    res_id=NULL, res_photo_id=NULL, res_video_id=NULL, res_gif_id=NULL,
                    res_link_id=NULL, res_link_msg_id=NULL, res_preview_id=NULL, res_other_id=NULL, res_text_id=NULL, res_msg_id=NULL
            ''')
        except sqlite3.OperationalError: pass

        # 4. 同时清理备份偏移量，确保下次执行时会从头扫描（或者由于没有历史记录而按全量/增量逻辑重新定位）
        try:
            self.cursor.execute('DELETE FROM backup_offsets')
        except sqlite3.OperationalError: pass
            
        self.conn.commit()
        return True

    def _backup_bot_names(self, bot_name):
        """返回备份记录查询时应匹配的 bot_name 别名集合。"""
        names = [bot_name]
        if bot_name == 'my_porn_private_bot':
            names.append('my_bdsm_private_bot')
        elif bot_name == 'my_bdsm_private_bot':
            names.append('my_porn_private_bot')
        return list(dict.fromkeys(names))

    def _extract_last_msg_id_from_backup_file(self, file_path):
        """从单个备份 JSON 中提取最后一条 msg_id。"""
        try:
            p = Path(file_path)
            if not p.exists() or p.suffix.lower() != '.json' or '_PARTIAL' in p.name:
                return 0
            data = json.loads(p.read_text(encoding='utf-8'))
            if not isinstance(data, list):
                return 0
            return max((int(item.get('msg_id', 0) or 0) for item in data if isinstance(item, dict)), default=0)
        except Exception:
            return 0

    def _pick_latest_backup_entry(self, channels):
        """从 channels_detail 中挑出最新且有效的 JSON 快照路径。"""
        best = None
        best_time = ''
        for ch in channels or []:
            if not isinstance(ch, dict):
                continue
            json_file = ch.get('json_file')
            if not json_file or '_PARTIAL' in str(json_file):
                continue
            path = Path(json_file)
            if not path.exists():
                continue
            c_time = str(ch.get('original_latest_time') or ch.get('time') or '')
            if best is None or c_time > best_time:
                best = ch
                best_time = c_time
        return best

    def get_latest_backup_channel_stats(self, chat_id, bot_name=None, is_test=False):
        """按频道回查最近一次有效备份统计，用于报告阶段回填 count/raw_count/json_file。"""
        norm_chat_id = self._normalize_id(chat_id)
        params = []
        query = 'SELECT run_id, channels_detail, bot_name FROM backup_runs WHERE is_test = ?'
        params.append(1 if is_test else 0)

        if bot_name:
            bot_names = self._backup_bot_names(bot_name)
            placeholders = ','.join(['?'] * len(bot_names))
            query += f' AND (bot_name IN ({placeholders}) OR (bot_name IS NULL AND ? = \'tgporncopilot\'))'
            params.extend(bot_names)
            params.append(bot_name)

        query += ' ORDER BY run_id DESC'
        rows = self.cursor.execute(query, tuple(params)).fetchall()

        for _, channels_detail, _ in rows:
            try:
                channels = json.loads(channels_detail) if channels_detail else []
            except Exception:
                continue
            if not isinstance(channels, list):
                continue

            for ch in channels:
                if not isinstance(ch, dict):
                    continue
                raw_chat_id = ch.get('id') or ch.get('chat_id')
                if raw_chat_id is None:
                    continue
                if self._normalize_id(raw_chat_id) != norm_chat_id:
                    continue
                if (ch.get('count', 0) or ch.get('raw_count', 0) or ch.get('json_file')):
                    return {
                        'count': ch.get('count', 0) or 0,
                        'raw_count': ch.get('raw_count', 0) or 0,
                        'json_file': ch.get('json_file'),
                        'md_file': ch.get('md_file'),
                        'status': ch.get('status'),
                    }
        return None

    def get_channel_global_counts(self, chat_id):
        """从 global_messages 表中统计频道的原始消息数与估算组数（将 media_group_id 视为组标识）。
        返回 dict: { 'raw_count': int, 'estimated_groups': int }
        """
        try:
            # 原始消息数
            self.cursor.execute('SELECT COUNT(*) FROM global_messages WHERE chat_id = ?', (chat_id,))
            raw = int(self.cursor.fetchone()[0] or 0)

            # 去重的 media_group_id 计数（非 NULL）
            self.cursor.execute('SELECT COUNT(DISTINCT media_group_id) FROM global_messages WHERE chat_id = ? AND media_group_id IS NOT NULL', (chat_id,))
            grouped = int(self.cursor.fetchone()[0] or 0)

            # 没有 media_group_id 的消息视为独立一组
            self.cursor.execute('SELECT COUNT(*) FROM global_messages WHERE chat_id = ? AND media_group_id IS NULL', (chat_id,))
            solo = int(self.cursor.fetchone()[0] or 0)

            estimated_groups = grouped + solo
            return {'raw_count': raw, 'estimated_groups': estimated_groups}
        except Exception:
            return {'raw_count': 0, 'estimated_groups': 0}

    def recalc_backup_offsets(self, bot_name='tgporncopilot', is_test=None, affected_chat_ids=None, clear_missing=False):
        """
        根据当前数据库中仍保留的 backup_runs 记录，重算 backup_offsets。

        - bot_name: 仅处理指定 Bot（含历史别名）
        - is_test: None=正式/测试都处理；True/False=仅处理指定环境
        - affected_chat_ids: 仅重算指定频道集合
        - clear_missing: 对于已经没有任何快照支撑的频道，将断点清零/删除
        """
        bot_names = self._backup_bot_names(bot_name)
        placeholders = ','.join(['?'] * len(bot_names))

        conditions = [f"(bot_name IN ({placeholders}) OR (bot_name IS NULL AND ? = 'tgporncopilot'))"]
        params = list(bot_names) + [bot_name]

        if is_test is not None:
            conditions.append('is_test = ?')
            params.append(1 if is_test else 0)

        query = f'''
            SELECT run_id, is_test, channels_detail
            FROM backup_runs
            WHERE {' AND '.join(conditions)}
            ORDER BY run_id DESC
        '''
        rows = self.cursor.execute(query, tuple(params)).fetchall()

        target_filter = None
        if affected_chat_ids:
            target_filter = {self._normalize_id(cid) for cid in affected_chat_ids if cid is not None}

        recalculated = {}
        for _, row_is_test, channels_detail in rows:
            try:
                channels = json.loads(channels_detail) if channels_detail else []
            except Exception:
                continue
            if not isinstance(channels, list):
                continue

            for ch in channels:
                if not isinstance(ch, dict):
                    continue
                raw_chat_id = ch.get('id') or ch.get('chat_id')
                if raw_chat_id is None:
                    continue
                norm_chat_id = self._normalize_id(raw_chat_id)
                env_key = (norm_chat_id, int(row_is_test))
                if target_filter and norm_chat_id not in target_filter:
                    continue
                if env_key in recalculated:
                    continue

                best_entry = self._pick_latest_backup_entry([ch])
                if not best_entry:
                    continue

                last_msg_id = self._extract_last_msg_id_from_backup_file(best_entry.get('json_file'))
                if last_msg_id <= 0:
                    continue

                recalculated[env_key] = {
                    'chat_id': norm_chat_id,
                    'is_test': int(row_is_test),
                    'last_msg_id': last_msg_id,
                    'updated_at': datetime.now().isoformat()
                }

        existing_conditions = []
        existing_params = []
        if is_test is not None:
            existing_conditions.append('is_test = ?')
            existing_params.append(1 if is_test else 0)
        if target_filter:
            placeholders_ids = ','.join(['?'] * len(target_filter))
            existing_conditions.append(f'chat_id IN ({placeholders_ids})')
            existing_params.extend(sorted(target_filter))

        existing_query = 'SELECT chat_id, is_test FROM backup_offsets'
        if existing_conditions:
            existing_query += ' WHERE ' + ' AND '.join(existing_conditions)
        existing_rows = self.cursor.execute(existing_query, tuple(existing_params)).fetchall()

        if clear_missing:
            for chat_id, row_is_test in existing_rows:
                if (chat_id, row_is_test) not in recalculated:
                    self.cursor.execute(
                        'DELETE FROM backup_offsets WHERE chat_id = ? AND is_test = ?',
                        (chat_id, row_is_test)
                    )

        for item in recalculated.values():
            self.cursor.execute('''
                INSERT INTO backup_offsets (chat_id, last_msg_id, updated_at, is_test)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, is_test) DO UPDATE SET
                    last_msg_id = excluded.last_msg_id,
                    updated_at = excluded.updated_at
            ''', (item['chat_id'], item['last_msg_id'], item['updated_at'], item['is_test']))

        self.conn.commit()
        return recalculated

    def add_entity_candidate(self, name, entity_type, msg_count=1):
        """记录发现的潜在实体（待审）"""
        if not name or len(name) < 2: return
        self.cursor.execute('''
            INSERT INTO entities (name, type, msg_count, status)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(name, type) DO UPDATE SET msg_count = msg_count + ?
        ''', (name, entity_type, msg_count, msg_count))
        self.conn.commit()

    def get_entities(self, status=None, entity_type=None, limit=50, offset=0):
        """查询实体列表"""
        query = "SELECT id, name, type, status, msg_count FROM entities"
        params = []
        where_clauses = []
        if status is not None:
            where_clauses.append("status = ?")
            params.append(status)
        if entity_type:
            where_clauses.append("type = ?")
            params.append(entity_type)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY msg_count DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        rows = self.cursor.execute(query, params).fetchall()
        return [{
            'id': r[0], 'name': r[1], 'type': r[2], 
            'status': r[3], 'msg_count': r[4]
        } for r in rows]

    def update_entity_status(self, entity_id, status):
        """更新实体状态 (1:已确认, 2:屏蔽)"""
        self.cursor.execute('UPDATE entities SET status = ? WHERE id = ?', (status, entity_id))
        self.conn.commit()

    def search_with_sync_links(self, query, search_type='keyword'):
        """
        深度检索：以备份库(global_messages)为基准，关联同步库(messages)的转发ID。
        search_type: 'keyword', 'creator', 'actor'
        返回: (chat_name, msg_type, sender_name, original_time, text_content, forwarded_msg_id, forwarded_chat_id, original_msg_id)
        """
        # 构造搜索条件
        if search_type == 'creator':
            cond = "gm.creator LIKE ?"
        elif search_type == 'actor':
            cond = "gm.actor LIKE ?"
        else:
            cond = "(gm.text_content LIKE ? OR gm.search_tags LIKE ? OR gm.keywords LIKE ?)"
            
        params = [f"%{query}%"]
        if search_type == 'keyword':
            params = [f"%{query}%", f"%{query}%", f"%{query}%"]

        sql = f'''
            SELECT gm.chat_name, gm.msg_type, gm.sender_name, gm.original_time, gm.text_content, 
                   m.forwarded_msg_id, m.forwarded_chat_id, gm.msg_id, gm.file_name
            FROM global_messages gm
            LEFT JOIN messages m ON m.original_chat_id = gm.chat_id AND m.original_msg_id = gm.msg_id
            WHERE {cond}
            ORDER BY gm.original_time DESC LIMIT 30
        '''
        
        return self.cursor.execute(sql, params).fetchall()

    # ===== 消息存档 =====
    def save_message(self, sync_run_id, msg_type, original_msg_id, original_chat_id, 
                     forwarded_msg_id, res_id, res_photo_id=0, res_video_id=0, 
                     res_other_id=0, res_text_id=0, forwarded_chat_id=None, header_msg_id=0):
        self.cursor.execute('''
            INSERT INTO messages (
                sync_run_id, msg_type, original_msg_id, original_chat_id, 
                forwarded_msg_id, forwarded_chat_id, res_id, res_photo_id, 
                res_video_id, res_other_id, res_text_id, header_msg_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (sync_run_id, msg_type, original_msg_id, original_chat_id, 
              forwarded_msg_id, forwarded_chat_id, res_id, res_photo_id, 
              res_video_id, res_other_id, res_text_id, header_msg_id))
        self.conn.commit()

    def save_global_message(self, chat_id, chat_name, msg_id, msg_type, sender_name,
                            original_time, text_content, file_name=None, media_group_id=None,
                            res_id=None, res_photo_id=None, res_video_id=None,
                            res_gif_id=None, res_link_id=None, res_link_msg_id=None,
                            res_preview_id=None, res_other_id=None, res_text_id=None, res_msg_id=None):
        """保存全局统一元信息，并包含资源编号"""
        self.cursor.execute('''
            INSERT OR REPLACE INTO global_messages (
                chat_id, chat_name, msg_id, msg_type, sender_name,
                original_time, text_content, file_name, media_group_id,
                res_id, res_photo_id, res_video_id, res_gif_id, res_link_id, res_link_msg_id,
                res_preview_id, res_other_id, res_text_id, res_msg_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            chat_id, chat_name, msg_id, msg_type, sender_name,
            original_time, text_content, file_name, media_group_id,
            res_id, res_photo_id, res_video_id, res_gif_id, res_link_id, res_link_msg_id,
            res_preview_id, res_other_id, res_text_id, res_msg_id
        ))
        self.conn.commit()

    def assign_resource_ids(self, chat_id, msg_id, msg_type, is_test=False, url_count=0, is_new_msg=False, commit=True):
        """
        为指定消息分配资源编号（按频道独立）。
        msg_type: 'video', 'photo', 'gif', 'link', 'link_preview', 'file', 'text'
        url_count: 该消息携带的 URL 链接总数 (用于支持单条多号)
        is_new_msg: 是否是该消息组的第一条消息（防止多图相册多次计费资源消息号）
        
        三套链接编号系统:
        1. preview: 可预览链接 (link_preview only)
        2. link: 链接计数 (对应每一个 URL)
        3. link_msg: 携带链接的消息号 (每条含 URL 的消息占一个号)
        """
        valid_types = ['video', 'photo', 'gif', 'link', 'link_preview', 'file', 'text']
        if msg_type not in valid_types:
            return None

        # --- 新增一致性审计逻辑 ---
        # 如果不是测试模式，先尝试获取已有编号
        if not is_test:
            existing = self.get_message_res_ids(chat_id, msg_id)
            if existing and any(v is not None and v != [] for v in existing.values()):
                # 简单校验：如果库里已有 total 编号，但当前处理的是资源类消息，直接复用
                # 注意：如果发现类型冲突（比如库里是视频，现在说是图片），抛出异常触发自杀
                return existing

        prefix = "test_" if is_test else ""
        
        def _next_counter(key, step=1):
            k = f"{prefix}{key}"
            self.cursor.execute('INSERT OR IGNORE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, ?, 0)', (chat_id, k))
            if step > 1:
                self.cursor.execute('UPDATE resource_counters SET last_value = last_value + ? WHERE chat_id = ? AND counter_key = ?', (step, chat_id, k))
                last = self.cursor.execute('SELECT last_value FROM resource_counters WHERE chat_id = ? AND counter_key = ?', (chat_id, k)).fetchone()[0]
                return list(range(last - step + 1, last + 1))
            else:
                self.cursor.execute('UPDATE resource_counters SET last_value = last_value + 1 WHERE chat_id = ? AND counter_key = ?', (chat_id, k))
                return self.cursor.execute('SELECT last_value FROM resource_counters WHERE chat_id = ? AND counter_key = ?', (chat_id, k)).fetchone()[0]

        ids = {'total': None, 'video': None, 'photo': None, 'gif': None, 'link': [], 'link_msg': None, 'preview': None, 'other': None, 'text': None, 'res_msg': None}

        # 资源消息号 (互补逻辑: 文字消息分配 text 号, 资源类消息分配 res_msg 号)
        if is_new_msg and msg_type in ['video', 'photo', 'gif', 'link_preview', 'file']:
            ids['res_msg'] = _next_counter('res_msg')

        # 总编号: 实际可保存的资源 (video/photo/gif/link_preview/file)
        if msg_type in ['video', 'photo', 'gif', 'link_preview', 'file']:
            ids['total'] = _next_counter('total')

        # 各类型独立编号
        if msg_type == 'video':    ids['video'] = _next_counter('video')
        elif msg_type == 'photo':  ids['photo'] = _next_counter('photo')
        elif msg_type == 'gif':    ids['gif'] = _next_counter('gif')
        elif msg_type == 'file':   ids['other'] = _next_counter('other')
        elif msg_type == 'text':   ids['text'] = _next_counter('text')
        
        # 链接（🔗）逻辑：消息包含多少链接就分配多少个号
        if url_count > 0:
            link_list = _next_counter('link', step=url_count)
            ids['link'] = link_list if isinstance(link_list, list) else [link_list]
            
            # 如果是单纯的 link 类型（非预览且非媒体消息中的链接），也要分配文本号
            if msg_type == 'link':
                ids['text'] = _next_counter('text')
        
        # 预览号（👁‍🗨️）仅限 link_preview 且每条消息一个
        if msg_type == 'link_preview':
            ids['preview'] = _next_counter('preview')

        # 携带链接消息编号 (📎) 每条含 URL 的消息占一个号
        if url_count > 0:
            ids['link_msg'] = _next_counter('link_msg')

        # 更新到 global_messages (非测试)
        if not is_test:
            link_val = f"{min(ids['link'])}-{max(ids['link'])}" if ids['link'] else None
            self.cursor.execute('''
                UPDATE global_messages SET 
                    res_id=?, res_photo_id=?, res_video_id=?, res_gif_id=?,
                    res_link_id=?, res_link_msg_id=?, res_preview_id=?, res_other_id=?, res_text_id=?, res_msg_id=?
                WHERE chat_id = ? AND msg_id = ?
            ''', (ids['total'], ids['photo'], ids['video'], ids['gif'],
                  link_val, ids['link_msg'], ids['preview'], ids['other'], ids['text'], ids['res_msg'],
                  chat_id, msg_id))
            
            self.cursor.execute('''
                UPDATE messages SET 
                    res_id=?, res_photo_id=?, res_video_id=?, res_gif_id=?,
                    res_link_id=?, res_link_msg_id=?, res_preview_id=?, res_other_id=?, res_text_id=?, res_msg_id=?
                WHERE original_chat_id = ? AND original_msg_id = ?
            ''', (ids['total'], ids['photo'], ids['video'], ids['gif'],
                  link_val, ids['link_msg'], ids['preview'], ids['other'], ids['text'], ids['res_msg'],
                  chat_id, msg_id))
        if commit:
            self.conn.commit()
        return ids

    def get_message_res_ids(self, chat_id, msg_id):
        """获取已存储的资源编号"""
        row = self.cursor.execute('''
            SELECT res_id, res_photo_id, res_video_id, res_gif_id,
                   res_link_id, res_link_msg_id, res_preview_id, res_other_id, res_text_id, res_msg_id
            FROM global_messages WHERE chat_id = ? AND msg_id = ?
        ''', (chat_id, msg_id)).fetchone()
        
        # 补充后备查找：如果在全局表中没找到 (比如只有测试备份的记录没进全局表)，则从 messages 里找最新的一条
        if not row:
            row = self.cursor.execute('''
                SELECT res_id, res_photo_id, res_video_id, res_gif_id,
                       res_link_id, res_link_msg_id, res_preview_id, res_other_id, res_text_id, res_msg_id
                FROM messages WHERE original_chat_id = ? AND original_msg_id = ?
                ORDER BY id DESC LIMIT 1
            ''', (chat_id, msg_id)).fetchone()

        if row:
            r_link = row[4]
            link_list = []
            if r_link:
                if '-' in str(r_link):
                    try:
                        mi, ma = map(int, str(r_link).split('-'))
                        link_list = list(range(mi, ma + 1))
                    except: pass
                else:
                    try: link_list = [int(r_link)]
                    except: pass
            
            return {
                'total': row[0], 'photo': row[1], 'video': row[2], 'gif': row[3],
                'link': link_list, 'link_msg': row[5], 'preview': row[6], 'other': row[7], 'text': row[8], 'res_msg': row[9]
            }
        return None

    # ===== 旧接口（兼容 search_bot） =====
    def add_video(self, original_msg_id, original_chat_id, forwarded_msg_id, creator, description):
        self.cursor.execute('''
            INSERT INTO videos (original_msg_id, original_chat_id, forwarded_msg_id, creator, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (original_msg_id, original_chat_id, forwarded_msg_id, creator, description))
        self.conn.commit()


    def update_offset(self, chat_id, last_msg_id, is_test=False, run_id=None):
        t_val = 1 if is_test else 0
        norm_id = self._normalize_id(chat_id)
        self.cursor.execute('''
            INSERT OR REPLACE INTO sync_offsets (chat_id, is_test, last_msg_id, updated_at, last_run_id)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
        ''', (norm_id, t_val, last_msg_id, run_id))
        self.conn.commit()

    def get_epoch_start_msg_id(self, chat_id, is_test=False):
        """
        获取当前频道在当前环境下的'纪元起点'（即最新一次全量同步的最早消息ID）。
        如果不存在或没有有效记录，返回0。由 backup.py 用于限制抓取范围，防止越界获取已作废的旧纪元消息。
        """
        t_val = 1 if is_test else 0
        # 找到属于该环境的所有保留下来的历史记录中的最小 original_msg_id
        row = self.cursor.execute('''
            SELECT MIN(original_msg_id) 
            FROM messages 
            WHERE original_chat_id = ? 
            AND sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = ?)
        ''', (chat_id, t_val)).fetchone()

        # 为了保险起见，减去1，因为 min_id 是开区间 (exclusive)，或者视 iter_messages 怎么处理
        # 实际上 telegram 的 min_id 意味着返回 > min_id 的消息
        # 所以如果最早的一条是 100，min_id 应该是 99
        start_id = row[0] if row and row[0] else 0
        return max(0, start_id - 1) if start_id > 0 else 0


    def search_by_creator(self, creator_name):
        query = f"%{creator_name}%"
        self.cursor.execute('''
            SELECT forwarded_msg_id, creator, description, timestamp 
            FROM videos WHERE creator LIKE ?
        ''', (query,))
        return self.cursor.fetchall()

    def search_global(self, keyword):
        """在全局和同步消息表中全文检索 (基于内容、发送者或来源)"""
        kw = f"%{keyword}%"
        # Search in messages
        self.cursor.execute('''
            SELECT original_chat_name, msg_type, sender_name, original_time, text_content, forwarded_msg_id
            FROM messages
            WHERE text_content LIKE ? OR sender_name LIKE ? OR original_chat_name LIKE ? OR file_name LIKE ?
            ORDER BY original_time DESC LIMIT 50
        ''', (kw, kw, kw, kw))
        res_sync = self.cursor.fetchall()
        
        # Search in global_messages
        self.cursor.execute('''
            SELECT chat_name, msg_type, sender_name, original_time, text_content, NULL
            FROM global_messages
            WHERE text_content LIKE ? OR sender_name LIKE ? OR chat_name LIKE ? OR file_name LIKE ?
            ORDER BY original_time DESC LIMIT 50
        ''', (kw, kw, kw, kw))
        res_global = self.cursor.fetchall()
        
        # Merge and deduplicate (roughly by time and content)
        seen = set()
        merged = []
        for r in res_sync + res_global:
            key = (r[0], r[3], r[4]) # chat_name, original_time, text_content
            if key not in seen:
                seen.add(key)
                merged.append(r)
        
        # Sort heavily by time
        merged.sort(key=lambda x: x[3] if x[3] else "", reverse=True)
        return merged[:50]

    def search_media_messages(self, keyword, search_type='keyword', limit=20):
        """
        工作模式3 — 多维度搜索。升级版：支持四个独立条目字段。
        """
        kw = f"%{keyword}%"
        MEDIA_TYPES = ('video', 'photo', 'gif', 'file', 'link_preview')
        placeholders = ','.join('?' * len(MEDIA_TYPES))
        
        # 基础条件：仅含资源的消息
        base_where = f"g.msg_type IN ({placeholders})"
        
        # 维度过滤逻辑
        if search_type == 'creator':
            # 搜索：条目1、标签、文案
            type_where = "(g.creator LIKE ? OR g.search_tags LIKE ? OR g.text_content LIKE ?)"
            tag_kw = f"%Creator:%{keyword}%"
            params = list(MEDIA_TYPES) + [kw, tag_kw, kw, limit * 3]
        elif search_type == 'actor':
            # 搜索：条目2、标签、文案
            type_where = "(g.actor LIKE ? OR g.search_tags LIKE ? OR g.text_content LIKE ?)"
            tag_kw = f"%Actor:%{keyword}%"
            params = list(MEDIA_TYPES) + [kw, tag_kw, kw, limit * 3]
        else:
            # 标准全局关键词搜索：包含四个条目和所有文本字段
            type_where = """(
                g.text_content LIKE ? OR g.search_tags LIKE ? OR g.file_name LIKE ? OR g.chat_name LIKE ?
                OR g.creator LIKE ? OR g.actor LIKE ? OR g.keywords LIKE ? OR g.supplement LIKE ?
            )"""
            params = list(MEDIA_TYPES) + [kw]*8 + [limit * 3]

        sql = f'''
            SELECT
                g.chat_id,
                g.msg_id,
                m.forwarded_msg_id,
                g.chat_name,
                g.msg_type,
                g.text_content,
                g.original_time,
                COALESCE(m.res_id, g.res_id)   AS res_id,
                g.media_group_id,
                g.search_tags,
                g.file_name,
                g.creator,
                g.actor,
                g.keywords,
                g.supplement
            FROM global_messages g
            JOIN messages m
                ON g.chat_id = m.original_chat_id
               AND g.msg_id  = m.original_msg_id
            WHERE {base_where} AND {type_where}
            ORDER BY g.original_time DESC
            LIMIT ?
        '''
        rows = self.cursor.execute(sql, params).fetchall()

        # 按 media_group_id 去重
        seen_groups = set()
        results = []
        for row in rows:
            (chat_id, msg_id, fwd_id, chat_name, msg_type,
             text_content, orig_time, res_id, media_group_id,
             search_tags, file_name, creator, actor, keywords, supplement) = row

            dedup_key = media_group_id if media_group_id else f"{chat_id}_{msg_id}"
            if dedup_key in seen_groups:
                continue
            seen_groups.add(dedup_key)

            results.append({
                'chat_id':        chat_id,
                'msg_id':         msg_id,
                'forwarded_msg_id': fwd_id,
                'chat_name':      chat_name or '未知频道',
                'msg_type':       msg_type,
                'text_content':   text_content or '',
                'original_time':  orig_time or '',
                'res_id':         res_id,
                'media_group_id': media_group_id,
                'search_tags':    search_tags or '',
                'file_name':      file_name or '',
                'creator':        creator or '',
                'actor':          actor or '',
                'keywords':       keywords or '',
                'supplement':     supplement or '',
            })
            if len(results) >= limit:
                break

        return results


    def get_msg_by_forwarded_id(self, forwarded_msg_id):
        """
        根据转发后的消息 ID 查找原始消息。
        用于工作模式4：转发私密视频库消息给 Bot 进行打标。
        """
        sql = '''
            SELECT 
                g.chat_id, g.msg_id, g.chat_name, g.text_content, 
                g.creator, g.actor, g.keywords, g.supplement,
                m.forwarded_msg_id
            FROM global_messages g
            JOIN messages m ON g.chat_id = m.original_chat_id AND g.msg_id = m.original_msg_id
            WHERE m.forwarded_msg_id = ?
        '''
        row = self.cursor.execute(sql, (forwarded_msg_id,)).fetchone()
        if row:
            return {
                'chat_id': row[0],
                'msg_id': row[1],
                'chat_name': row[2],
                'text_content': row[3],
                'creator': row[4],
                'actor': row[5],
                'keywords': row[6],
                'supplement': row[7],
                'forwarded_msg_id': row[8]
            }
        return None

    def update_msg_entries(self, chat_id, msg_id, creator=None, actor=None, keywords=None, supplement=None):
        """
        更新消息的四个核心条目。
        """
        updates = []
        params = []
        if creator is not None:
            updates.append("creator = ?")
            params.append(creator)
        if actor is not None:
            updates.append("actor = ?")
            params.append(actor)
        if keywords is not None:
            updates.append("keywords = ?")
            params.append(keywords)
        if supplement is not None:
            updates.append("supplement = ?")
            params.append(supplement)
        
        if not updates:
            return
            
        params.extend([chat_id, msg_id])
        sql = f"UPDATE global_messages SET {', '.join(updates)} WHERE chat_id = ? AND msg_id = ?"
        self.cursor.execute(sql, params)
        self.conn.commit()

    def rollback_to(self, target_label, bot_name='tgporncopilot', commit=True):
        """
        回滚到特定的某次同步状态。强制隔离 bot_name。
        :param commit: True 表示正式提交擦除；False 表示仅预检，提取需要撤回的消息 ID。
        """
        target_label = str(target_label).strip().upper()
        # 处理 POINT_0 的友好显示转换为内部标签
        if target_label == "POINT_0_TEST": target_label = "TEST-0"
        if target_label == "POINT_0_FORMAL": target_label = "#0"
        
        is_test_target = 1 if ('TEST' in target_label) else 0

        # 查找目标 run_id，必须匹配 bot_name
        if target_label in ['TEST-0', '#0']:
            target_run_id = -1
            curr_idx = 0
        else:
            if not target_label.startswith('TEST-') and not target_label.startswith('#'):
                if target_label.isdigit(): target_label = f"#{target_label}"
                else: raise ValueError("格式错误")

            runs = self.cursor.execute(
                'SELECT run_id FROM sync_runs WHERE is_test = ? AND (bot_name = ? OR bot_name IS NULL) ORDER BY run_id ASC', 
                (is_test_target, bot_name)
            ).fetchall()
            
            target_run_id = None
            curr_idx = 1
            for (r_id,) in runs:
                label = f"TEST-{curr_idx}" if is_test_target else self.get_run_label(r_id)
                if label.upper() == target_label:
                    target_run_id = r_id
                    break
                curr_idx += 1
            
            if target_run_id is None:
                raise ValueError(f"没找到目标版本 {target_label} (Bot: {bot_name})")

        # 查找这批要被删掉的记录
        deleted_runs = self.cursor.execute(
            'SELECT run_id FROM sync_runs WHERE is_test = ? AND run_id > ? AND (bot_name = ? OR bot_name IS NULL)', 
            (is_test_target, target_run_id, bot_name)
        ).fetchall()

        if not deleted_runs:
            return [], None
            
        deleted_run_ids = [r[0] for r in deleted_runs]
        placeholders = ','.join('?' * len(deleted_run_ids))
        
        # 物理边界提取
        bounds_info = self.cursor.execute(f"""
            SELECT MIN(start_msg_id), MAX(end_msg_id)
            FROM sync_runs r
            WHERE r.run_id IN ({placeholders})
        """, deleted_run_ids).fetchone()

        # 获取消息删除目标
        msg_deletion_targets = {"target_group": []}
        
        # 1. 提取所有关联的内容消息 ID 和 消息头 ID
        rows = self.cursor.execute(f"""
            SELECT forwarded_chat_id, forwarded_msg_id, header_msg_id 
            FROM messages 
            WHERE sync_run_id IN ({placeholders})
        """, deleted_run_ids).fetchall()
        
        id_pairs = []
        for cid, fwd_id, hdr_id in rows:
            if not cid: continue
            norm_cid = self._normalize_id(cid)
            if fwd_id and fwd_id > 0: id_pairs.append((norm_cid, fwd_id))
            if hdr_id and hdr_id > 0: id_pairs.append((norm_cid, hdr_id))
            
        # 2. 提取 sync_runs 中的起始与结束总结消息 ID，并生成闭区间内的所有 ID
        boundary_rows = self.cursor.execute(f"""
            SELECT target_group_id, start_msg_id, end_msg_id
            FROM sync_runs
            WHERE run_id IN ({placeholders})
        """, deleted_run_ids).fetchall()
        
        for tgt_id, start_id, end_id in boundary_rows:
            if not tgt_id: continue
            norm_tgt_id = self._normalize_id(tgt_id)
            if start_id and end_id and start_id > 0 and end_id >= start_id:
                # [V2] 核心改进：将边界内的所有 ID 全部加入删除列表
                # 这能确保即便有未记录的相册片段或额外消息，也能被物理清除
                for mid in range(start_id, end_id + 1):
                    id_pairs.append((norm_tgt_id, mid))
            elif start_id and start_id > 0:
                id_pairs.append((norm_tgt_id, start_id))
            elif end_id and end_id > 0:
                id_pairs.append((norm_tgt_id, end_id))
            
        # 3. 去重排序
        msg_deletion_targets["target_group"] = sorted(list(set(id_pairs)), key=lambda x: x[1])

        deleted_labels = []
        for r_id in deleted_run_ids:
            deleted_labels.append(self.get_run_label(r_id))

        # --- 分支点：仅预检还是正式提交 ---
        if not commit:
            print(f"🔍 [Pre-Check] 识别到待处理版本: {', '.join(deleted_labels)}")
            return deleted_labels, {
                "min_start": bounds_info[0] if bounds_info else None,
                "max_end": bounds_info[1] if bounds_info else None,
                "is_test": is_test_target,
                "msg_ids_to_delete": msg_deletion_targets["target_group"]
            }

        # 1. 物理清理日志
        print(f"✅ 准备清理对应物理日志文件...")
        for root_dir in ['data/archived/logs', 'docs/archived/logs', 'docs/logs']:
            if not os.path.exists(root_dir): continue
            for dirpath, dirnames, filenames in os.walk(root_dir):
                if 'backups' in dirpath.lower(): continue
                for f in filenames:
                    if any(f"_{lbl}." in f or f"_{lbl}_" in f or lbl == f.split('.')[0] for lbl in deleted_labels):
                        file_path = os.path.join(dirpath, f)
                        try:
                            os.remove(file_path)
                            print(f"  🗑️ 已删除废弃日志: {file_path}")
                        except: pass

        # 2. 擦除 messages 和 sync_runs
        self.cursor.execute(f"DELETE FROM messages WHERE sync_run_id IN ({placeholders})", deleted_run_ids)
        self.cursor.execute(f"DELETE FROM sync_runs WHERE run_id IN ({placeholders})", deleted_run_ids)

        # 3. 收缩 offsets 和 counters
        self._recalc_counters(is_test_target, bot_name)
        
        self.conn.commit()
        return deleted_labels, {
            "min_start": bounds_info[0],
            "max_end": bounds_info[1],
            "is_test": is_test_target,
            "msg_ids_to_delete": msg_deletion_targets["target_group"]
        }

    def _recalc_counters(self, is_test, bot_name):
        """内部工具：回滚后根据幸存记录重置偏移量和计数器"""
        # 3. 如果是正式记录，还需要收缩 global_messages 并重置同步断点偏移量
        istest_val = 1 if is_test else 0
        
        if not is_test:
            # 清理孤立的 global_messages
            self.cursor.execute('''
                DELETE FROM global_messages
                WHERE (chat_id, msg_id) NOT IN (
                    SELECT original_chat_id, original_msg_id
                    FROM messages
                    WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 0 AND (bot_name = ? OR bot_name IS NULL))
                )
            ''', (bot_name,))
            
            # 收缩断点到幸存的最大 ID (包括 last_run_id)
            self.cursor.execute('UPDATE sync_offsets SET last_msg_id = 0, updated_at = NULL, last_run_id = NULL WHERE is_test = 0')
            updates = self.cursor.execute('''
                SELECT ABS(m.original_chat_id) % 1000000000000, MAX(m.original_msg_id), MAX(sr.start_time), MAX(sr.run_id)
                FROM messages m
                JOIN sync_runs sr ON sr.run_id = m.sync_run_id
                WHERE sr.is_test = 0 AND (sr.bot_name = ? OR sr.bot_name IS NULL)
                GROUP BY ABS(m.original_chat_id) % 1000000000000
            ''', (bot_name,)).fetchall()
            for chat_id, max_id, max_time, max_run_id in updates:
                self.cursor.execute('''
                    UPDATE sync_offsets 
                    SET last_msg_id = ?, updated_at = ?, last_run_id = ? 
                    WHERE chat_id = ? AND is_test = 0
                ''', (max_id, max_time, max_run_id, chat_id))
        else:
            # 测试模式收缩 (包括 last_run_id)
            self.cursor.execute('UPDATE sync_offsets SET last_msg_id = 0, updated_at = NULL, last_run_id = NULL WHERE is_test = 1')
            updates = self.cursor.execute('''
                SELECT ABS(m.original_chat_id) % 1000000000000, MAX(m.original_msg_id), MAX(sr.start_time), MAX(sr.run_id)
                FROM messages m
                JOIN sync_runs sr ON sr.run_id = m.sync_run_id
                WHERE sr.is_test = 1 AND (sr.bot_name = ? OR sr.bot_name IS NULL)
                GROUP BY ABS(m.original_chat_id) % 1000000000000
            ''', (bot_name,)).fetchall()
            for chat_id, max_id, max_time, max_run_id in updates:
                self.cursor.execute('''
                    UPDATE sync_offsets 
                    SET last_msg_id = ?, updated_at = ?, last_run_id = ? 
                    WHERE chat_id = ? AND is_test = 1
                ''', (max_id, max_time, max_run_id, chat_id))

        # 4. 根据幸存的消息，动态重计算全局文件/文字等自增 ID
        prefix = "test_" if is_test else ""
        self.cursor.execute(f"DELETE FROM resource_counters WHERE counter_key LIKE '{prefix}%' AND chat_id NOT IN (SELECT original_chat_id FROM messages WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = ? AND (bot_name = ? OR bot_name IS NULL)))", (is_test, bot_name))
        
        # Reset existing counters to 0 before recalculating
        self.cursor.execute(f"UPDATE resource_counters SET last_value = 0 WHERE counter_key LIKE '{prefix}%'")

        max_stats = self.cursor.execute(f'''
            SELECT original_chat_id, 
                   MAX(res_id), MAX(res_photo_id), MAX(res_video_id), 
                   MAX(res_other_id), MAX(res_text_id), MAX(res_gif_id),
                   MAX(CAST(SUBSTR(res_link_id, INSTR(res_link_id, '-') + 1) AS INTEGER)), -- Max of the range
                   MAX(res_link_msg_id), MAX(res_preview_id), MAX(res_msg_id)
            FROM messages
            WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = ? AND (bot_name = ? OR bot_name IS NULL))
            GROUP BY original_chat_id
        ''', (is_test, bot_name)).fetchall()
        
        for chat_id, m_total, m_photo, m_video, m_other, m_text, m_gif, m_link, m_link_msg, m_preview, m_res_msg in max_stats:
            if m_total: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}total', ?)", (chat_id, m_total))
            if m_photo: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}photo', ?)", (chat_id, m_photo))
            if m_video: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}video', ?)", (chat_id, m_video))
            if m_other: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}other', ?)", (chat_id, m_other))
            if m_text:  self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}text', ?)", (chat_id, m_text))
            if m_gif: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}gif', ?)", (chat_id, m_gif))
            if m_link: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}link', ?)", (chat_id, m_link))
            if m_link_msg: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}link_msg', ?)", (chat_id, m_link_msg))
            if m_preview: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}preview', ?)", (chat_id, m_preview))
            if m_res_msg: self.cursor.execute(f"INSERT OR REPLACE INTO resource_counters (chat_id, counter_key, last_value) VALUES (?, '{prefix}res_msg', ?)", (chat_id, m_res_msg))

        # 5. 收缩 AUTOINCREMENT 指针 (让下一个版本号完美衔接)
        if not is_test:
            max_formal = self.cursor.execute("SELECT MAX(formal_number) FROM sync_runs WHERE is_test = 0 AND (bot_name = ? OR bot_name IS NULL)", (bot_name,)).fetchone()[0] or 0
            # Note: SQLite's AUTOINCREMENT is tricky to reset per-bot. This will only affect the global sequence.
            # For formal_number, we rely on MAX(formal_number) + 1 logic.
        else:
            # For tests we delete runs going forward so run_id itself should be compressed
            # However SQLite doesn't easily expose sequence adjustment without sqlite_sequence table
            try:
                max_id = self.cursor.execute("SELECT MAX(run_id) FROM sync_runs WHERE is_test = 1 AND (bot_name = ? OR bot_name IS NULL)", (bot_name,)).fetchone()[0] or 0
                self.cursor.execute("UPDATE sqlite_sequence SET seq = ? WHERE name = 'sync_runs'", (max_id,))
            except Exception:
                pass

    # --- 目标群组管理相关 [NEW] ---
    def register_target_group(self, chat_id, title, bot_name):
        self.cursor.execute('''
            INSERT OR REPLACE INTO target_groups (chat_id, title, bot_name)
            VALUES (?, ?, ?)
        ''', (chat_id, title, bot_name))
        self.conn.commit()

    def set_active_target_group(self, chat_id, bot_name):
        # 先全置为 0
        self.cursor.execute('UPDATE target_groups SET is_active = 0 WHERE bot_name = ?', (bot_name,))
        self.cursor.execute('UPDATE target_groups SET is_active = 1 WHERE chat_id = ? AND bot_name = ?', (chat_id, bot_name))
        self.conn.commit()

    def get_target_groups(self, bot_name):
        rows = self.cursor.execute('SELECT chat_id, title, is_active FROM target_groups WHERE bot_name = ?', (bot_name,)).fetchall()
        return [{'chat_id': r[0], 'title': r[1], 'is_active': bool(r[2])} for r in rows]

    def get_active_target_group(self, bot_name):
        row = self.cursor.execute('SELECT chat_id, title FROM target_groups WHERE is_active = 1 AND bot_name = ?', (bot_name,)).fetchone()
        if row:
            return {'chat_id': row[0], 'title': row[1]}
        return None

    def delete_target_group(self, chat_id, bot_name):
        self.cursor.execute('DELETE FROM target_groups WHERE chat_id = ? AND bot_name = ?', (chat_id, bot_name))
        self.conn.commit()

    def get_entities_v2(self):
        """
        [V2.0] 获取层级化的实体数据，用于驱动 Bot 菜单。
        返回: { 'creators': [...], 'actors': [...], 'keywords': { 'Category': [...] } }
        """
        # 1. 加载创作者和演员
        creators = self.get_entities(status=1, entity_type='creator', limit=1000)
        actors = self.get_entities(status=1, entity_type='actor', limit=1000)
        
        # 2. 从 entities.json 加载关键词分类 (因为 DB entities 表目前不存 Category)
        # TODO: 长期方案应该在 DB 中增加 category 字段，目前从 JSON 读取作为辅助
        from pathlib import Path
        config_path = Path('data/entities/tgporncopilot_entities.json')
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                keywords = data.get('keywords', {})
        else:
            keywords = {}
            
        return {
            'creators': creators,
            'actors': actors,
            'keywords': keywords
        }

    def reset_channel_sync(self, chat_id, is_test):
        """
        在全新全时间轴同步 (Mode 2/4) 开始前，彻底清洗该频道对应环境（测试/正式）的本地偏移量、资源编号记录与元数据，
        使其从头开始计数，避免之前残余记录导致的编号断层或错乱。
        """
        prefix = "test_" if is_test else ""
        
        # 1. 抹除消息历史隔离区
        self.cursor.execute('''
            DELETE FROM messages 
            WHERE original_chat_id = ? 
            AND sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = ?)
        ''', (chat_id, 1 if is_test else 0))
        
        # 2. 抹除资源累加器
        self.cursor.execute(f"DELETE FROM resource_counters WHERE chat_id = ? AND counter_key LIKE '{prefix}%'", (chat_id,))
        
        # 3. 如果是正式模式，需额外清除 global 和 offsets
        if not is_test:
            self.cursor.execute("DELETE FROM global_messages WHERE chat_id = ?", (chat_id,))
            self.cursor.execute("UPDATE sync_offsets SET last_msg_id = 0 WHERE chat_id = ? AND is_test = 0", (chat_id,))
        else:
            self.cursor.execute("UPDATE sync_offsets SET last_msg_id = 0 WHERE chat_id = ? AND is_test = 1", (chat_id,))
        self.conn.commit()

    def clear_test_data(self):
        """清洗所有测试产出的隔离数据"""
        self.cursor.execute("DELETE FROM messages WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 1)")
        self.cursor.execute("DELETE FROM sync_runs WHERE is_test = 1")
        self.cursor.execute("DELETE FROM resource_counters WHERE counter_key LIKE 'test_%'")
        self.conn.commit()

    def close(self):
        self.conn.close()
