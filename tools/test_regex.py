import re

pattern_table = re.compile(
    r'\| \d+ \| `\s*(.*?)\s*` \| \[(.)\] CREATOR\s*\\\|\s*\[(.)\] ACTOR\s*\\\|\s*\[(.)\] TAG(?:\s*\((.*?)\))?\s*\\\|\s*\[(.)\] NOISE\s*\|'
    r'\s*\(频次:\s*(\d+),\s*来源:\s*(.*?)\)'
)

line = "| 1 | ` 调教 ` | [ ] CREATOR \| [ ] ACTOR \| [ ] TAG \| [ ] NOISE | (频次: 75142, 来源: NLP_Term, Bracket, Tag) |"

m = pattern_table.search(line)
if m:
    print(f"Match found!")
    print(f"Word: '{m.group(1)}'")
    print(f"Creator: '{m.group(2)}'")
    print(f"Actor: '{m.group(3)}'")
    print(f"Tag: '{m.group(4)}'")
    print(f"Category: '{m.group(5)}'")
    print(f"Noise: '{m.group(6)}'")
    print(f"Count: '{m.group(7)}'")
    print(f"Source: '{m.group(8)}'")
else:
    print("Match failed!")

# Test with category
line_cat = "| 1 | ` 调教 ` | [ ] CREATOR \| [ ] ACTOR \| [ ] TAG (分类A) \| [ ] NOISE | (频次: 75142, 来源: NLP_Term) |"
m2 = pattern_table.search(line_cat)
if m2:
    print(f"Match category: '{m2.group(5)}'")
else:
    print("Match category failed!")
