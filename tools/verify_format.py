import re

# Mocking the formatting logic from execute_advanced_search
def format_result(r):
    chat_name, mtype, sender, otime, text, fwd_id, cid, mid, fname = r
    icon = {"video": "🎬", "photo": "🖼️", "file": "📄", "gif": "🎞️"}.get(mtype, "📝")
    
    duration = ""
    combined_text = (text or "") + (fname or "")
    dur_match = re.search(r'\[(\d{1,2}:\d{2})\]', combined_text)
    if dur_match:
        duration = f"[{dur_match.group(1)}]"
    
    display_text = ""
    if text:
        first_line = text.split('\n')[0].strip().strip('#')
        first_line = re.sub(r'\[\d{1,2}:\d{2}\]', '', first_line).strip()
        display_text = first_line
    
    if not display_text and fname:
        display_text = re.sub(r'\[\d{1,2}:\d{2}\]', '', fname).strip()
        
    if not display_text:
        display_text = f"未命名资源 {mid}"

    if len(display_text) > 40:
        display_text = display_text[:37] + "..."

    if fwd_id:
        d_id = str(cid or 0).replace('-100', '')
        link = f"https://t.me/c/{d_id}/{fwd_id}"
        return f"{icon}{duration} [{display_text}]({link})"
    else:
        return f"{icon}{duration} {display_text} *(仅备份)*"

# Test samples
samples = [
    ("Group1", "video", "User1", "2026-03-10", "#窒物者\n[48:10] 窒物者222231.mp4", 123, -1005011841156, 6000, "窒物者222231.mp4"),
    ("Group1", "video", "User1", "2026-03-10", "【窒物者】 极致幻想_我的学妹", 124, -1005011841156, 6001, "[31:15] 窒物者_学妹.mp4"),
    ("Group2", "photo", "User2", "2026-03-11", "一张精美的照片", None, -100123456789, 7000, "photo.jpg")
]

print("--- Verification Results ---")
for s in samples:
    print(format_result(s))
print("--- End ---")
