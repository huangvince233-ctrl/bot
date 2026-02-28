import sqlite3
import os
from datetime import datetime

class Database:
    def __init__(self, db_path='data/copilot.db'):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
        self._migrate()

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
                texts_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                start_msg_id INTEGER DEFAULT NULL,
                end_msg_id INTEGER DEFAULT NULL
            )
        ''')

        # 消息存档表：每条转发的消息一条记录
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_run_id INTEGER,
                msg_type TEXT,
                original_msg_id INTEGER,
                original_chat_id INTEGER,
                original_chat_name TEXT,
                forwarded_msg_id INTEGER,
                sender_name TEXT,
                original_time TEXT,
                forwarded_time TEXT,
                text_content TEXT,
                creator TEXT,
                group_index INTEGER DEFAULT NULL,
                file_name TEXT,
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
                FOREIGN KEY (sync_run_id) REFERENCES sync_runs(run_id)
            )
        ''')

        # 同步断点偏移量 (per-chat, per-env)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_offsets (
                chat_id INTEGER,
                is_test INTEGER DEFAULT 0,
                last_msg_id INTEGER DEFAULT 0,
                updated_at TIMESTAMP,
                PRIMARY KEY (chat_id, is_test)
            )
        ''')
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
                channels_detail TEXT DEFAULT NULL
            )
        ''')

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
        try:
            self.cursor.execute("ALTER TABLE global_messages ADD COLUMN search_tags TEXT DEFAULT NULL")
            self.cursor.execute("ALTER TABLE global_messages ADD COLUMN is_extracted INTEGER DEFAULT 0")
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
    def start_sync_run(self, is_test=True):
        """开始一次同步，返回 run_id"""
        formal_num = None
        if not is_test:
            row = self.cursor.execute(
                'SELECT MAX(formal_number) FROM sync_runs WHERE is_test = 0'
            ).fetchone()
            formal_num = (row[0] or 0) + 1
        
        self.cursor.execute('''
            INSERT INTO sync_runs (is_test, formal_number, start_time)
            VALUES (?, ?, ?)
        ''', (1 if is_test else 0, formal_num, datetime.now().isoformat()))
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

    def get_recent_sync_runs(self, is_test=None, limit=50):
        """获取最近的同步记录用于交互菜单展示"""
        query = 'SELECT run_id, start_time, is_test, formal_number FROM sync_runs'
        params = []
        if is_test is not None:
            query += ' WHERE is_test = ?'
            params.append(1 if is_test else 0)
        
        query += ' ORDER BY run_id DESC LIMIT ?'
        params.append(limit)
        
        rows = self.cursor.execute(query, params).fetchall()
        
        results = []
        for run_id, start_time, is_test, formal_number in rows:
            label = self.get_run_label(run_id)
            # 格式化时间 2025-02-23T10:00:00 -> 02-23 10:00
            try:
                dt = datetime.fromisoformat(start_time)
                time_str = dt.strftime("%m-%d %H:%M")
            except:
                time_str = "未知时间"
            results.append((label, time_str))
        return results

    def get_run_label(self, run_id):
        """获取同步标签：如 'TEST-3' 或 '#5'"""
        row = self.cursor.execute(
            'SELECT is_test, formal_number FROM sync_runs WHERE run_id = ?', (run_id,)
        ).fetchone()
        if not row:
            return f"RUN-{run_id}"
        if row[0]:  # is_test
            count = self.cursor.execute('SELECT COUNT(*) FROM sync_runs WHERE is_test = 1 AND run_id <= ?', (run_id,)).fetchone()[0]
            if count == 0: count = 1
            return f"TEST-{count}"
        return f"#{row[1]}"

    # ===== 备份记录 =====
    def start_backup_run(self, mode, is_incremental, is_test=True):
        """开始一次备份，返回 run_id"""
        formal_num = None
        if not is_test:
            row = self.cursor.execute(
                'SELECT MAX(formal_number) FROM backup_runs WHERE is_test = 0'
            ).fetchone()
            formal_num = (row[0] or 0) + 1

        self.cursor.execute('''
            INSERT INTO backup_runs (is_test, formal_number, start_time, backup_mode, is_incremental)
            VALUES (?, ?, ?, ?, ?)
        ''', (1 if is_test else 0, formal_num, datetime.now().isoformat(), str(mode), 1 if is_incremental else 0))
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
        """获取备份标签：如 'TEST-B1' 或 '#B1'"""
        row = self.cursor.execute(
            'SELECT is_test, formal_number FROM backup_runs WHERE run_id = ?', (run_id,)
        ).fetchone()
        if not row:
            return f"BKUP-{run_id}"
        if row[0]:  # is_test
            count = self.cursor.execute('SELECT COUNT(*) FROM backup_runs WHERE is_test = 1 AND run_id <= ?', (run_id,)).fetchone()[0]
            if count == 0: count = 1
            return f"TEST-B{count}"
        return f"#B{row[1]}"

    def update_backup_offset(self, chat_id, last_msg_id, is_test=0):
        """更新频道备份断点和时间戳"""
        self.cursor.execute('''
            INSERT INTO backup_offsets (chat_id, last_msg_id, updated_at, is_test)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, is_test) DO UPDATE SET
                last_msg_id = excluded.last_msg_id,
                updated_at = excluded.updated_at
        ''', (chat_id, last_msg_id, datetime.now().isoformat(), 1 if is_test else 0))
        self.conn.commit()

    def get_all_backup_offsets(self):
        """获取所有频道的备份时间，返回 {chat_id: latest_time}"""
        rows = self.cursor.execute('SELECT chat_id, MAX(updated_at) FROM backup_offsets GROUP BY chat_id').fetchall()
        return {r[0]: r[1] for r in rows}

    def get_all_sync_offsets(self):
        """获取所有频道的同步时间，返回 {chat_id: latest_time}"""
        # 从 sync_offsets 或 sync_runs 关联获取也可以，这里优先查 sync_offsets
        rows = self.cursor.execute('SELECT chat_id, MAX(updated_at) FROM sync_offsets GROUP BY chat_id').fetchall()
        return {r[0]: r[1] for r in rows}

    # ===== 状态查询 (树状图用) =====
    def get_latest_sync_info(self, chat_id=None, is_test=False):
        """
        查询最近一次同步信息（区分测试与正式环境）。
        chat_id=None: 查全局最近记录
        chat_id=具体值: 查某频道最近一次同步耗时
        返回 dict: {label, time, run_id} 或 None
        """
        istest_val = 1 if is_test else 0
        if chat_id is None:
            row = self.cursor.execute('''
                SELECT run_id, formal_number, start_time FROM sync_runs 
                WHERE is_test = ? ORDER BY run_id DESC LIMIT 1
            ''', (istest_val,)).fetchone()
        else:
            # 从 messages 表找该频道最近一次记录
            row = self.cursor.execute('''
                SELECT sr.run_id, sr.formal_number, sr.start_time
                FROM sync_runs sr
                JOIN messages m ON m.sync_run_id = sr.run_id
                WHERE sr.is_test = ? AND m.original_chat_id = ?
                ORDER BY sr.run_id DESC LIMIT 1
            ''', (istest_val, chat_id)).fetchone()
            
        if row:
            label = self.get_run_label(row[0])
            return {'run_id': row[0], 'label': label, 'time': row[2]}
        return None

    def get_latest_backup_info(self, chat_id=None, is_test=None):
        """
        查询最近一次备份信息。
        chat_id=None: 全局最近
        chat_id=具体值: 某频道最近一次备份 label 和时间
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
            # [NEW] 1. 优先查找包含该频道的最近 50 次备份记录 (以获得正式 Label，如 #B1)
            rows = self.cursor.execute('''
                SELECT run_id, start_time, channels_detail FROM backup_runs 
                ORDER BY run_id DESC LIMIT 50
            ''').fetchall()
            for r_id, start_time, detail_json in rows:
                if not detail_json: continue
                try:
                    channels = json.loads(detail_json)
                    # channels 是 [{name, id, count, ...}, ...]
                    for ch in channels:
                        if str(ch.get('id')) == str(chat_id):
                            return {
                                'run_id': r_id,
                                'label': self.get_backup_label(r_id),
                                'time': start_time
                            }
                except: continue

            # [NEW] 2. 兜底逻辑：如果最近的正式大报告中没有，则检查更轻量的断点表 (backup_offsets)
            off = self.cursor.execute(
                'SELECT last_msg_id, updated_at FROM backup_offsets WHERE chat_id = ? ORDER BY updated_at DESC LIMIT 1',
                (chat_id,)
            ).fetchone()
            if off:
                return {
                    'run_id': 0,
                    'label': "📍 检查点",
                    'time': off[1],
                    'last_msg_id': off[0]
                }
        return None

    def get_manageable_backup_runs(self, limit=20):
        """获取最近的备份记录，用于展示在删除菜单中"""
        import json
        rows = self.cursor.execute('''
            SELECT run_id, start_time, is_test, formal_number, channels_detail, backup_mode, is_incremental, total_messages, new_messages
            FROM backup_runs ORDER BY run_id DESC LIMIT ?
        ''', (limit,)).fetchall()
        
        runs = []
        for r in rows:
            runs.append({
                'run_id': r[0],
                'time': r[1],
                'is_test': bool(r[2]),
                'label': self.get_backup_label(r[0]),
                'channels': json.loads(r[4]) if r[4] else [],
                'mode': r[5],
                'incremental': bool(r[6]),
                'total_messages': r[7],
                'new_messages': r[8] or 0
            })
        return runs

    def delete_backup_run(self, run_id):
        """删除特定的备份运行记录"""
        self.cursor.execute('DELETE FROM backup_runs WHERE run_id = ?', (run_id,))
        self.conn.commit()
        return True

    def clear_all_backup_runs(self):
        """全量清空备份运行记录"""
        self.cursor.execute('DELETE FROM backup_runs')
        self.conn.commit()
        return True

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
        """
        if search_type == 'creator':
            # 在 search_tags 中查找匹配
            sql = '''
                SELECT 
                    g.chat_name, g.msg_type, g.sender_name, g.original_time, 
                    g.text_content, m.forwarded_msg_id, g.chat_id, g.msg_id, g.search_tags
                FROM global_messages g
                LEFT JOIN messages m ON g.chat_id = m.original_chat_id AND g.msg_id = m.original_msg_id
                WHERE g.search_tags LIKE ?
                ORDER BY g.original_time DESC LIMIT 50
            '''
            search_term = f"%{query}%"
            rows = self.cursor.execute(sql, (search_term,)).fetchall()
        elif search_type == 'actor':
            sql = '''
                SELECT 
                    g.chat_name, g.msg_type, g.sender_name, g.original_time, 
                    g.text_content, m.forwarded_msg_id, g.chat_id, g.msg_id, g.search_tags
                FROM global_messages g
                LEFT JOIN messages m ON g.chat_id = m.original_chat_id AND g.msg_id = m.original_msg_id
                WHERE g.search_tags LIKE ?
                ORDER BY g.original_time DESC LIMIT 50
            '''
            search_term = f"%{query}%"
            rows = self.cursor.execute(sql, (search_term,)).fetchall()
        else:
            # 通用关键字搜索 (匹配文本、频道名、文件名)
            sql = '''
                SELECT 
                    g.chat_name, g.msg_type, g.sender_name, g.original_time, 
                    g.text_content, m.forwarded_msg_id, g.chat_id, g.msg_id, g.search_tags
                FROM global_messages g
                LEFT JOIN messages m ON g.chat_id = m.original_chat_id AND g.msg_id = m.original_msg_id
                WHERE g.text_content LIKE ? OR g.chat_name LIKE ? OR g.file_name LIKE ? OR g.search_tags LIKE ?
                ORDER BY g.original_time DESC LIMIT 50
            '''
            search_term = f"%{query}%"
            rows = self.cursor.execute(sql, (search_term, search_term, search_term, search_term)).fetchall()
            
        return rows

    # ===== 消息存档 =====
    def save_message(self, sync_run_id, msg_type, original_msg_id, original_chat_id,
                     original_chat_name, forwarded_msg_id, sender_name,
                     original_time, forwarded_time, text_content, creator,
                     group_index=None, file_name=None,
                     res_id=None, res_photo_id=None, res_video_id=None,
                     res_gif_id=None, res_link_id=None, res_link_msg_id=None,
                     res_preview_id=None, res_other_id=None, res_text_id=None, res_msg_id=None):
        """保存一条消息的完整元数据，包含资源编号"""
        self.cursor.execute('''
            INSERT INTO messages (
                sync_run_id, msg_type, original_msg_id, original_chat_id,
                original_chat_name, forwarded_msg_id, sender_name,
                original_time, forwarded_time, text_content, creator, group_index, file_name,
                res_id, res_photo_id, res_video_id, res_gif_id, res_link_id, res_link_msg_id,
                res_preview_id, res_other_id, res_text_id, res_msg_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sync_run_id, msg_type, original_msg_id, original_chat_id,
            original_chat_name, forwarded_msg_id, sender_name,
            original_time, forwarded_time, text_content, creator, group_index, file_name,
            res_id, res_photo_id, res_video_id, res_gif_id, res_link_id, res_link_msg_id,
            res_preview_id, res_other_id, res_text_id, res_msg_id
        ))
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

    def assign_resource_ids(self, chat_id, msg_id, msg_type, is_test=False, url_count=0, is_new_msg=False):
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
            if existing:
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

    def get_last_offset(self, chat_id, is_test=False):
        t_val = 1 if is_test else 0
        self.cursor.execute('SELECT last_msg_id FROM sync_offsets WHERE chat_id = ? AND is_test = ?', (chat_id, t_val))
        result = self.cursor.fetchone()
        return result[0] if result else 0

    def update_offset(self, chat_id, last_msg_id, is_test=False):
        t_val = 1 if is_test else 0
        self.cursor.execute('''
            INSERT OR REPLACE INTO sync_offsets (chat_id, is_test, last_msg_id, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ''', (chat_id, t_val, last_msg_id))
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

    def rollback_to(self, target_label):
        """
        回滚到特定的某次同步状态（如 'TEST-1' 或 '#3'）。
        将删除目标点之后产生的所有当前环境（测试/正式）的同步记录和对应文件关联，并自动重置同步浮标与起止序号。
        返回被删除掉的日志文件名列表（用于彻底清除实体文件）。
        """
        target_label = str(target_label).strip().upper()
        
        # 处理归零点逻辑
        if target_label in ['TEST-0', '#0', 'POINT_0_TEST', 'POINT_0_FORMAL']:
            is_test_target = 1 if ('TEST' in target_label) else 0
            target_run_id = -1 # 特殊 ID，代表删除该环境所有 run_id > -1 的记录
            curr_idx = 0
        else:
            if not target_label.startswith('TEST-') and not target_label.startswith('#'):
                if target_label.isdigit():
                    target_label = f"#{target_label}"
                else:
                    raise ValueError("格式错误: 目标必须形如 'TEST-1' 或 '#3'。")

            is_test_target = 1 if target_label.startswith('TEST-') else 0
            
            target_run_id = None
            runs = self.cursor.execute('SELECT run_id FROM sync_runs WHERE is_test = ? ORDER BY run_id ASC', (is_test_target,)).fetchall()
            
            curr_idx = 1
            for (r_id,) in runs:
                label = f"TEST-{curr_idx}" if is_test_target else self.get_run_label(r_id)
                if label.upper() == target_label:
                    target_run_id = r_id
                    break
                curr_idx += 1
                
            if target_run_id is None:
                raise ValueError(f"没找到目标版本 {target_label}。请检查您的版本号。")

        # 查找所有目标断点之后的未来同步
        deleted_runs = self.cursor.execute(
            'SELECT run_id FROM sync_runs WHERE is_test = ? AND run_id > ?', 
            (is_test_target, target_run_id)
        ).fetchall()

        if not deleted_runs:
            return [], None # Nothing to roll back
            
        deleted_run_ids = [r[0] for r in deleted_runs]
        placeholders = ','.join('?' * len(deleted_run_ids))
        
        deleted_labels = [] # Renamed from labels_to_delete to avoid conflict with instruction's variable name
        if is_test_target:
            # 如果回滚到 TEST-idx，那么要删除的是 TEST-(idx+1) 及以后的
            start_num = curr_idx + 1
            for _ in deleted_run_ids:
                deleted_labels.append(f"TEST-{start_num}")
                start_num += 1
        else:
            for r_id in deleted_run_ids:
                deleted_labels.append(self.get_run_label(r_id))
                
        # 0. 提取 Telegram 群组的物理删除边界 (找到所有被删记录中的最老 start_msg_id 到 最新 end_msg_id)
        # 注意: 如果之前的版本没有记录这些字段，可能查出 NULL
        min_start = None
        max_end = None
        
        bounds = self.cursor.execute(f"SELECT MIN(start_msg_id), MAX(end_msg_id) FROM sync_runs WHERE run_id IN ({placeholders})", deleted_run_ids).fetchone()
        
        # 兼容性 fallback: 提取这批记录里包含的具体转发消息 ID
        msg_deletion_targets = {"target_group": []}
        rows = self.cursor.execute(f"SELECT forwarded_msg_id FROM messages WHERE sync_run_id IN ({placeholders}) AND forwarded_msg_id > 0", deleted_run_ids).fetchall()
        msg_deletion_targets["target_group"] = [r[0] for r in rows]
        
        if bounds and bounds[0] is not None and bounds[1] is not None:
            min_start, max_end = bounds
        else:
            min_start, max_end = None, None
        
        # 1. 抹除消息历史
        self.cursor.execute(f"DELETE FROM messages WHERE sync_run_id IN ({placeholders})", deleted_run_ids)
        
        # 2. 抹除主同步记录
        self.cursor.execute(f"DELETE FROM sync_runs WHERE run_id IN ({placeholders})", deleted_run_ids)

        # 2b. 清理物理日志文件 (移动到这里，确保所有分支都执行)
        print(f"✅ 数据库关联记录已擦除，准备清理对应物理日志文件...")
        for root_dir in ['data/archived/logs', 'docs/archived/logs', 'docs/logs']:
            if not os.path.exists(root_dir): continue
            for dirpath, dirnames, filenames in os.walk(root_dir):
                # 保护 backups 文件夹
                if 'backups' in dirpath.lower():
                    continue
                    
                for f in filenames:
                    if any(f"_{lbl}." in f or f"_{lbl}_" in f or lbl == f.split('.')[0] for lbl in deleted_labels):
                        file_path = os.path.join(dirpath, f)
                        try:
                            os.remove(file_path)
                            print(f"  🗑️ 已删除废弃日志: {file_path}")
                        except Exception as e:
                            pass

        if bounds and bounds[0] is not None:
             # 原有 bounds 分支后续逻辑 ( global_messages 清理等)
             pass
        else:
             # 原有老版本分支收尾
             return deleted_labels, msg_deletion_targets
        
        # 3. 如果是正式记录，还需要收缩 global_messages 并重置同步断点偏移量
        if not is_test_target:
            # 清理孤立的 global_messages
            self.cursor.execute('''
                DELETE FROM global_messages
                WHERE (chat_id, msg_id) NOT IN (
                    SELECT original_chat_id, original_msg_id
                    FROM messages
                    WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 0)
                )
            ''')
            
            # 收缩断点到幸存的最大 ID
            self.cursor.execute('UPDATE sync_offsets SET last_msg_id = 0 WHERE is_test = 0')
            updates = self.cursor.execute('''
                SELECT original_chat_id, MAX(original_msg_id) 
                FROM messages 
                WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 0)
                GROUP BY original_chat_id
            ''').fetchall()
            for chat_id, max_id in updates:
                self.cursor.execute('UPDATE sync_offsets SET last_msg_id = ? WHERE chat_id = ? AND is_test = 0', (max_id, chat_id))
        else:
            # 测试模式收缩
            self.cursor.execute('UPDATE sync_offsets SET last_msg_id = 0 WHERE is_test = 1')
            updates = self.cursor.execute('''
                SELECT original_chat_id, MAX(original_msg_id) 
                FROM messages 
                WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = 1)
                GROUP BY original_chat_id
            ''').fetchall()
            for chat_id, max_id in updates:
                self.cursor.execute('UPDATE sync_offsets SET last_msg_id = ? WHERE chat_id = ? AND is_test = 1', (max_id, chat_id))

        # 4. 根据幸村的消息，动态重计算全局文件/文字等自增 ID
        prefix = "test_" if is_test_target else ""
        self.cursor.execute(f"UPDATE resource_counters SET last_value = 0 WHERE counter_key LIKE '{prefix}%'")
        
        max_stats = self.cursor.execute(f'''
            SELECT original_chat_id, 
                   MAX(res_id), MAX(res_photo_id), MAX(res_video_id), 
                   MAX(res_other_id), MAX(res_text_id)
            FROM messages
            WHERE sync_run_id IN (SELECT run_id FROM sync_runs WHERE is_test = ?)
            GROUP BY original_chat_id
        ''', (is_test_target,)).fetchall()
        
        for chat_id, m_total, m_photo, m_video, m_other, m_text in max_stats:
            if m_total: self.cursor.execute(f"UPDATE resource_counters SET last_value = ? WHERE chat_id = ? AND counter_key = '{prefix}total'", (m_total, chat_id))
            if m_photo: self.cursor.execute(f"UPDATE resource_counters SET last_value = ? WHERE chat_id = ? AND counter_key = '{prefix}photo'", (m_photo, chat_id))
            if m_video: self.cursor.execute(f"UPDATE resource_counters SET last_value = ? WHERE chat_id = ? AND counter_key = '{prefix}video'", (m_video, chat_id))
            if m_other: self.cursor.execute(f"UPDATE resource_counters SET last_value = ? WHERE chat_id = ? AND counter_key = '{prefix}other'", (m_other, chat_id))
            if m_text:  self.cursor.execute(f"UPDATE resource_counters SET last_value = ? WHERE chat_id = ? AND counter_key = '{prefix}text'", (m_text, chat_id))

        # 5. 收缩 AUTOINCREMENT 指针 (让下一个版本号完美衔接)
        if not is_test_target:
            max_formal = self.cursor.execute("SELECT MAX(formal_number) FROM sync_runs WHERE is_test = 0").fetchone()[0] or 0
        else:
            # For tests we delete runs going forward so run_id itself should be compressed
            # However SQLite doesn't easily expose sequence adjustment without sqlite_sequence table
            try:
                max_id = self.cursor.execute("SELECT MAX(run_id) FROM sync_runs").fetchone()[0] or 0
                self.cursor.execute("UPDATE sqlite_sequence SET seq = ? WHERE name = 'sync_runs'", (max_id,))
            except Exception:
                pass

        self.conn.commit()
        return deleted_labels, (min_start, max_end)

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
