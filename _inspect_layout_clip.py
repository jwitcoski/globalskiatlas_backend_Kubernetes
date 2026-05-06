import zipfile
from pathlib import Path

p = Path("atlas/map_gen/templates/ski_atlas_small_medium_template.qgz")
with zipfile.ZipFile(p) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8", errors="replace")

for needle in ["globe_clip_shape", "overview_inset_map", "north-arrow", "scaleBar"]:
    idx = txt.find(needle)
    print("\n===", needle, "at", idx, "===")
    if idx >= 0:
        print(txt[max(0, idx - 200) : idx + 1200])

# Find LayoutItems type 65639 with largest area (main map)
import re

layouts_section_start = txt.find("<Layouts>")
layouts_section_end = txt.find("</Layouts>")
layouts = txt[layouts_section_start : layouts_section_end]

items = []
for m in re.finditer(r"<LayoutItem\b([^>]*)>", layouts):
    tag = m.group(1)
    if "type=\"65639\"" not in tag and "type=\"65639\"" not in tag.replace("'", '"'):
        continue
    w = re.search(r'width="([0-9.]+)"', tag)
    h = re.search(r'height="([0-9.]+)"', tag)
    # Actually position uses vector attribute position="x,y,mm"
    pos = re.search(r'position="([^"]+)"', tag)
    size = re.search(r'size="([^"]+)"', tag)
    iid = re.search(r' id="([^"]*)"', tag)
    items.append((tag[:120], pos.group(1) if pos else "?", size.group(1) if size else "?", iid.group(1) if iid else "?"))

print("\n65639 map items:", len(items))
for it in items:
    print(it)
