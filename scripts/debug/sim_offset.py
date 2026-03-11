
import sqlite3
import sys

# simulate db.py _normalize_id
def norm(cid):
    if cid is None: return None
    return abs(int(cid)) % 1000000000000

conn = sqlite3.connect('data/copilot.db')
c = conn.cursor()

def get_offset(cid, is_test):
    t_val = 1 if is_test else 0
    n = norm(cid)
    r = c.execute("SELECT last_msg_id FROM sync_offsets WHERE chat_id = ? AND is_test = ?", (n, t_val)).fetchone()
    print(f"get_last_offset({cid}, is_test={is_test}) -> norm {n}, t_val {t_val} -> {r[0] if r else 0}")
    
get_offset(-1005051247857, True)
get_offset(-1005051247857, False)
get_offset(5051247857, True)
get_offset(5051247857, False)
conn.close()
