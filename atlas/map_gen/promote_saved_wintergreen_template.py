"""
Promote a mapper-edited Wintergreen QGZ as the small/medium template.

We keep the generator stable by renaming the internal project file back to
`wintergreen_map.qgs` (the name expected by TEMPLATE_QGS_NAME in data_to_qgis.py).
"""

from __future__ import annotations

import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_QGZ = REPO_ROOT / "atlas_work/wintergreen-ski-resort/wintergreen-ski-resort_map.qgz"
DST_QGZ = REPO_ROOT / "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz"

SRC_QGS = "wintergreen-ski-resort_map.qgs"
DST_QGS = "wintergreen_map.qgs"


def main() -> None:
    if not SRC_QGZ.exists():
        raise SystemExit(f"Source QGZ not found: {SRC_QGZ}")

    with zipfile.ZipFile(SRC_QGZ, "r") as zin:
        items = [(info, zin.read(info.filename)) for info in zin.infolist()]

    with zipfile.ZipFile(DST_QGZ, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for info, data in items:
            if info.filename == SRC_QGS:
                info.filename = DST_QGS
            zout.writestr(info, data)

    print(f"Promoted {SRC_QGZ} -> {DST_QGZ} (internal {SRC_QGS} -> {DST_QGS})")


if __name__ == "__main__":
    main()

