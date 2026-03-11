from src.db import Database
import json

db = Database('data/copilot.db')
results = db.search_with_sync_links('窒物者', search_type='creator')

print(f"Found {len(results)} results")
for r in results[:5]:
    chat_name, mtype, sender, otime, text, fwd_id, cid, mid = r
    print(f"Type: {mtype} | Time: {otime} | FwdID: {fwd_id} | CID: {cid}")
    print(f"Text: {repr(text)}")
    print("-" * 20)
