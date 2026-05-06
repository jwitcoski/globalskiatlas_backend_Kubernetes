import re
import zipfile
from pathlib import Path

qgz = Path("atlas_work/winterplace-ski-resort/winterplace-ski-resort_map.qgz")
with zipfile.ZipFile(qgz) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8")

# Each LayoutItem ... LayoutItem/> might span lines — naive split by "</LayoutItem>"
blocks = txt.split("</LayoutItem>")
print("blocks", len(blocks))

main_candidates = []
for b in blocks:
    if "type=\"65639\"" in b or 'type="65639"' in b:
        mr = re.search(r'mapRotation="([^"]+)"', b)
        sid = re.search(r'id="([^"]*)"', b)
        sz = re.search(r'size="([^"]+)"', b)
        pos = re.search(r'position="([^"]+)"', b)
        zval = re.search(r'zValue="([^"]+)"', b)
        clip = "clipSettings" in b or "maskSource" in b or "linkedClip" in b.lower()
        main_candidates.append((sid.group(1) if sid else "", mr.group(1) if mr else "", sz.group(1) if sz else "", pos.group(1) if pos else "", zval.group(1) if zval else "", clip))

for row in main_candidates:
    print(row)

# Dump longest map block (likely main)
longest = None
for b in blocks:
    if "type=\"65639\"" not in b:
        continue
    if longest is None or len(b) > len(longest):
        longest = b

if longest:
    idx = longest.find("<layoutMap>")
    print("\n--- clipping substring ---")
    for needle in ["clip", "mask", "atlas", "linked"]:
        print(needle, needle.lower() in longest.lower())

    path = Path("_layout_main_map_snippet.xml")
    path.write_text(longest[-8000:], encoding="utf-8")
    print("tail written", path, "chars", len(longest))
