import zipfile
from pathlib import Path

p = Path("atlas/map_gen/templates/ski_atlas_small_medium_template.qgz")
with zipfile.ZipFile(p) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8")

si = txt.find("Ski Atlas Export")
chunk_start = txt.rfind("<Layout ", 0, si)
chunk_end = txt.find("</Layout>", si)
chunk = txt[chunk_start:chunk_end]

needle = 'mapRotation="91.8'
idx = chunk.find(needle)
print("found rotation at", idx)
seg = chunk[idx : idx + 14000]
j = seg.find("<itemClippingSettings")
print("clip tag:", seg[j : j + 160])
