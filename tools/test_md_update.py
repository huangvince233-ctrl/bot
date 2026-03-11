from pathlib import Path
import re
import sys

# Mocking the structure
def save_decision_to_md(file_path, word, decided, category):
    p = Path(file_path)
    if not p.exists():
        print(f"File {file_path} does not exist.")
        return
    try:
        content = p.read_text(encoding='utf-8')
        lines = content.splitlines()
        new_lines = []
        found = False
        for line in lines:
            if not found and f"` {word} `" in line:
                c_box = "[x]" if "creator" in decided else "[ ]"
                a_box = "[x]" if "actor" in decided else "[ ]"
                t_box = "[x]" if "tag" in decided else "[ ]"
                n_box = "[x]" if "noise" in decided else "[ ]"
                tag_label = f"TAG({category})" if category and category != "未分类" else "TAG"
                
                line = re.sub(r'\[.\]\s*CREATOR', f'{c_box} CREATOR', line)
                line = re.sub(r'\[.\]\s*ACTOR', f'{a_box} ACTOR', line)
                line = re.sub(r'\[.\]\s*TAG(\(.*?\))?', f'{t_box} {tag_label}', line)
                line = re.sub(r'\[.\]\s*NOISE', f'{n_box} NOISE', line)
                found = True
            new_lines.append(line)
        if found:
            p.write_text("\n".join(new_lines) + "\n", encoding='utf-8')
            print(f"Successfully updated line for '{word}' in {file_path}")
        else:
            print(f"Word '{word}' not found in {file_path}")
    except Exception as e:
        print(f"Error saving to MD: {e}")

# Test
test_file = "docs/entities/tgporncopilot_candidates/candidate_pool_part_1.md"
save_decision_to_md(test_file, "特惠价", ["tag"], "反恐")

