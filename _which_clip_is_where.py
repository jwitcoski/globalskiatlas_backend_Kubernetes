import re
import zipfile
from pathlib import Path

p = Path("atlas/map_gen/templates/ski_atlas_small_medium_template.qgz")
with zipfile.ZipFile(p) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8")

# Split Ski Atlas Export layout section roughly
start = txt.find("<Layout ")
si = txt.find("name=\"Ski Atlas Export\"", start)
chunk_start = txt.rfind("<Layout ", 0, si + 1)
chunk_end = txt.find("</Layout>", si)
chunk = txt[chunk_start:chunk_end]

for i, m in enumerate(re.finditer(r"<LayoutItem\b([^>]*)>", chunk)):
    head = m.group(1)
    if "65639" not in head:
        continue
    mid = head.find("mapRotation")
    iid = re.search(r'id="([^"]*)"', head)
    sz = re.search(r'size="([^"]+)"', head)
    mr = re.search(r'mapRotation="([^"]+)"', head)
    print("\n--- LayoutItem map idx", i, "---")
    print("id=", iid.group(1) if iid else "?")
    print("size=", sz.group(1) if sz else "?")
    print("mapRotation=", mr.group(1) if mr else "?")

for i, m in enumerate(re.finditer(r"<itemClippingSettings[^/]*/>", chunk)):
    ctx_start = max(0, m.start() - 800)
    ctx = chunk[ctx_start : m.end()]
    idnear = re.findall(r'id="([^"]+)"', ctx[-400:])
    print("\nclip occurrence", i + 1, "near ids tail:", idnear[-3:])
