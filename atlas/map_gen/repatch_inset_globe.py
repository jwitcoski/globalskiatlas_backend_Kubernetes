#!/usr/bin/env python3
"""
Re-apply orthographic inset CRS + symmetric hemisphere extent on existing QGZ files.

Use after fixing _ortho_extent_for_globe_window, then re-export PNGs:
  python -m atlas.map_gen.repatch_inset_globe --region europe/slovenia
  atlas\\map_gen\\run_export_layouts.bat --glob europe/slovenia/**/_map.qgz
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

from atlas.map_gen.data_to_qgis import TEMPLATE_QGS_NAME, _recenter_overview_inset_ortho


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _centroid_from_geojson(resort_dir: Path) -> tuple[float, float] | None:
    geojson = resort_dir / "resort_inset_point.geojson"
    if not geojson.is_file():
        return None
    data = json.loads(geojson.read_text(encoding="utf-8"))
    feat = (data.get("features") or [None])[0]
    if not feat:
        return None
    coords = feat.get("geometry", {}).get("coordinates")
    if not coords or len(coords) < 2:
        return None
    return float(coords[0]), float(coords[1])


def repatch_qgz(qgz_path: Path) -> bool:
    centroid = _centroid_from_geojson(qgz_path.parent)
    if centroid is None:
        print(f"  skip (no inset point): {qgz_path.parent.name}")
        return False
    lon, lat = centroid

    with zipfile.ZipFile(qgz_path, "r") as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]

    qgs_name = next((info.filename for info, _ in items if info.filename.endswith(".qgs")), None)
    if not qgs_name:
        return False

    text = next(data for info, data in items if info.filename == qgs_name).decode("utf-8")
    patched = _recenter_overview_inset_ortho(text, lon, lat)

    m = re.search(
        r'<Extent xmin="([^"]+)" ymin="([^"]+)" xmax="([^"]+)" ymax="([^"]+)"/>',
        patched,
    )
    extent_str = m.group(0) if m else "no Extent tag"

    with zipfile.ZipFile(qgz_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in items:
            if info.filename in (qgs_name, TEMPLATE_QGS_NAME):
                zout.writestr(info.filename, patched.encode("utf-8"))
            else:
                zout.writestr(info, data)

    print(f"  {qgz_path.parent.name}: {extent_str}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-patch globe inset ortho extent on QGZ files")
    parser.add_argument("--region", required=True, help="e.g. europe/slovenia or europe")
    parser.add_argument("--work-dir", type=Path, default=None)
    args = parser.parse_args()

    root = _repo_root()
    work_dir = args.work_dir or (root / "atlas_work")
    region = args.region.strip().replace("\\", "/")
    prefix = work_dir / region
    if prefix.is_dir():
        qgz_files = sorted(prefix.rglob("*_map.qgz"))
    else:
        qgz_files = sorted(
            p
            for p in work_dir.rglob("*_map.qgz")
            if region in p.as_posix().replace("\\", "/")
        )

    if not qgz_files:
        print(f"No QGZ under {prefix}", file=sys.stderr)
        return 1

    ok = sum(1 for q in qgz_files if repatch_qgz(q))
    print(f"\nRepatched {ok}/{len(qgz_files)}. Re-export layouts to refresh PNGs.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
