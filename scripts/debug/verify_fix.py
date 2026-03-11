
import sys
import os
# Add src to path
sys.path.append(os.path.abspath('src'))
from db import Database

db = Database()

chat_id = -1005051247857
norm_id = 5051247857

# Check actual values
offset_raw = db.get_last_offset(chat_id, is_test=True)
offset_norm = db.get_last_offset(norm_id, is_test=True)

print(f"Actual Offset for raw ID {chat_id}: {offset_raw}")
print(f"Actual Offset for normalized ID {norm_id}: {offset_norm}")

if offset_raw == offset_norm and offset_raw > 0:
    print(f"SUCCESS: Consistency confirmed at {offset_raw}")
else:
    print(f"FAILED: Mismatch or zero! (Raw: {offset_raw}, Norm: {offset_norm})")

db.close()
