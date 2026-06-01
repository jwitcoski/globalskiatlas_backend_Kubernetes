#!/usr/bin/env python3
"""
Align osm_near_winter_sports / PointsClipped "Tree points" markers with Trees / Forest
RandomMarkerFill triangles in all ski_atlas_*_template.qgz files.

Run once after template edits, or when regenerating from an old template backup:
  python -m atlas.map_gen.patch_template_tree_points
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from atlas.map_gen.data_to_qgis import _fix_tree_point_marker_symbology

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = REPO_ROOT / "atlas" / "map_gen" / "templates"


def patch_qgz(path: Path) -> bool:
    if not path.is_file():
        return False
    with zipfile.ZipFile(path, "r") as zin:
        names = zin.namelist()
        files = {name: zin.read(name) for name in names}
    qgs_name = next((n for n in names if n.endswith(".qgs")), None)
    if not qgs_name:
        return False
    original = files[qgs_name].decode("utf-8")
    patched = _fix_tree_point_marker_symbology(original)
    if patched == original:
        print(f"  (no change) {path.name}")
        return False
    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        shutil.copy2(path, backup)
    files[qgs_name] = patched.encode("utf-8")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zout:
        for name, data in files.items():
            zout.writestr(name, data)
    print(f"  updated {path.name}")
    return True


def main() -> int:
    templates = sorted(TEMPLATES_DIR.glob("ski_atlas_*_template.qgz"))
    if not templates:
        print(f"No templates under {TEMPLATES_DIR}")
        return 1
    n = 0
    for path in templates:
        if patch_qgz(path):
            n += 1
    print(f"\nPatched {n}/{len(templates)} template(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
