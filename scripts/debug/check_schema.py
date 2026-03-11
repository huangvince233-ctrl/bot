
import sqlite3
conn = sqlite3.connect('data/copilot.db')
print(conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sync_offsets'").fetchone()[0])
conn.close()
