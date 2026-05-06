import re
import zipfile
from pathlib import Path

p = Path("atlas/map_gen/templates/ski_atlas_small_medium_template.qgz")
with zipfile.ZipFile(p) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8")

si = txt.find("Ski Atlas Export")
chunk_start = txt.rfind("<Layout ", 0, si)
chunk_end = txt.find("</Layout>", si)
chunk = txt[chunk_start:chunk_end]

pat = '<itemClippingSettings enabled="1" forceLabelsInside="0" clipSource="{eb3dbbea-2d98-4fe4-a099-7b457733bdb0}" clippingType="1"/>'

pos = 0
while True:
    i = chunk.find(pat, pos)
    if i < 0:
        break
    win = chunk[max(0, i - 3500) : i]
    mrot = list(re.finditer(r'mapRotation="([^"]+)"', win))
    last_mr = mrot[-1].group(1) if mrot else "?"
    size_m = list(re.finditer(r'size="([^"]+)"', win))
    last_sz = size_m[-1].group(1) if size_m else "?"
    iid = list(re.finditer(r'id="([^"]*)"', win))
    last_id = iid[-1].group(1) if iid else "?"
    print("clip at", i, "nearest mapRotation=", last_mr, "size=", last_sz, "id=", repr(last_id))
    pos = i + len(pat)
