import zipfile
from pathlib import Path

p = Path("atlas/map_gen/templates/ski_atlas_small_medium_template.qgz")
with zipfile.ZipFile(p) as z:
    txt = z.read(next(n for n in z.namelist() if n.endswith(".qgs"))).decode("utf-8")

si = txt.find("Ski Atlas Export")
chunk_start = txt.rfind("<Layout ", 0, si)
chunk_end = txt.find("</Layout>", si)
chunk = txt[chunk_start:chunk_end]

pat = '<itemClippingSettings enabled="1"'
idx = 0
while True:
    i = chunk.find(pat, idx)
    if i < 0:
        break
    print("\n===== occurrence =====")
    print(chunk[max(0, i - 2000) : i + 200])
    idx = i + len(pat)
