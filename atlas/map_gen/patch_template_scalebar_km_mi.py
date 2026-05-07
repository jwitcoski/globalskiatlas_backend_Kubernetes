"""
Patch `ski_atlas_small_medium_template.qgz` scalebar(s) safely.

Target behavior:
- Top scale bar:
    - units: kilometers
    - fixed width: 0.5 km
- Bottom scale bar:
    - units: miles
    - scaleable ("fit segment width")

IMPORTANT: We locate the scalebar by its UUID (from the template) to avoid any
chance of touching other LayoutItem types.
"""

from __future__ import annotations

import re
import uuid
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
QGZ_PATH = REPO_ROOT / "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz"
QGS_NAME = "wintergreen_map.qgs"
SCALEBAR_UUID = "{ab260a4b-b995-4ced-b6d7-0bfd8f8bce21}"


def _replace_attr(tag: str, key: str, value: str) -> str:
    if f'{key}="' not in tag:
        # insert before closing >
        i = tag.rfind(">")
        return tag[:i] + f' {key}="{value}"' + tag[i:]
    return re.sub(rf'{key}="[^"]*"', f'{key}="{value}"', tag, count=1)


def _layoutitem_block_by_uuid(text: str, uuid_str: str) -> tuple[int, int]:
    """Return (start,end) indices for the LayoutItem block with uuid="{...}"."""
    open_tok = f'<LayoutItem uuid="{uuid_str}"'
    pos = text.find(open_tok)
    if pos < 0:
        raise ValueError(f"LayoutItem with uuid {uuid_str} not found")
    start = pos  # opening tag starts exactly here in our token
    end = text.find("</LayoutItem>", start)
    if end < 0:
        raise ValueError("Could not locate closing </LayoutItem>")
    return start, end + len("</LayoutItem>")


def _split_opening(block: str) -> tuple[str, str]:
    j = block.find(">")
    if j < 0:
        raise ValueError("LayoutItem opening tag not found.")
    return block[: j + 1], block[j + 1 :]


def main() -> None:
    with zipfile.ZipFile(QGZ_PATH, "r") as zin:
        payload = {info.filename: zin.read(info.filename) for info in zin.infolist()}

    qgs = payload[QGS_NAME].decode("utf-8", errors="replace")

    # Remove any *extra* scalebars beyond the template's primary UUID (idempotent).
    extra_opens = list(
        re.finditer(r"<LayoutItem\b[^>]*\btype=\"65646\"[^>]*>", qgs)
    )
    to_remove: list[tuple[int, int]] = []
    for m in extra_opens:
        tag = m.group(0)
        if f'uuid="{SCALEBAR_UUID}"' in tag:
            continue
        # remove this entire LayoutItem block
        s = qgs.rfind("<LayoutItem", 0, m.start() + 1)
        e = qgs.find("</LayoutItem>", m.start())
        if s >= 0 and e >= 0:
            to_remove.append((s, e + len("</LayoutItem>")))
    for s, e in sorted(to_remove, key=lambda x: x[0], reverse=True):
        qgs = qgs[:s] + qgs[e:]

    # Now locate the canonical scalebar by UUID.
    try:
        s0, e0 = _layoutitem_block_by_uuid(qgs, SCALEBAR_UUID)
    except ValueError as e:
        raise SystemExit(str(e))
    block0 = qgs[s0:e0]
    open0, rest0 = _split_opening(block0)

    # --- Top (km, fixed width 0.5 km) ---
    open_km = open0
    open_km = _replace_attr(open_km, "unitType", "meters")
    open_km = _replace_attr(open_km, "unitLabel", "km")
    open_km = _replace_attr(open_km, "numMapUnitsPerScaleBarUnit", "1000")
    open_km = _replace_attr(open_km, "segmentSizeMode", "0")  # fixed width (units)
    open_km = _replace_attr(open_km, "numUnitsPerSegment", "0.5")
    open_km = _replace_attr(open_km, "numSegments", "2")
    open_km = _replace_attr(open_km, "numSegmentsLeft", "0")
    open_km = _replace_attr(open_km, "positionOnPage", "0,125.8,mm")
    open_km = _replace_attr(open_km, "position", "0,125.8,mm")
    block_km = open_km + rest0

    # --- Bottom (mi, scaleable / fit segment width) ---
    mi_uuid = "{" + str(uuid.uuid4()) + "}"
    open_mi = open_km
    # swap uuid/templateUuid
    open_mi = open_mi.replace(f'uuid="{SCALEBAR_UUID}"', f'uuid="{mi_uuid}"', 1)
    open_mi = open_mi.replace(
        f'templateUuid="{SCALEBAR_UUID}"', f'templateUuid="{mi_uuid}"', 1
    )
    open_mi = _replace_attr(open_mi, "unitLabel", "mi")
    open_mi = _replace_attr(open_mi, "numMapUnitsPerScaleBarUnit", "1609.344")
    open_mi = _replace_attr(open_mi, "segmentSizeMode", "1")  # fit segment width (mm)
    # keep same segmentMillimeters, but miles distance will adapt; choose 2 segments as a default
    open_mi = _replace_attr(open_mi, "numSegments", "2")
    open_mi = _replace_attr(open_mi, "numUnitsPerSegment", "1")
    open_mi = _replace_attr(open_mi, "positionOnPage", "0,131.8,mm")
    open_mi = _replace_attr(open_mi, "position", "0,131.8,mm")
    block_mi = open_mi + rest0

    # Replace original block with km + mi blocks
    qgs2 = qgs[:s0] + block_km + "\n" + block_mi + qgs[e0:]
    payload[QGS_NAME] = qgs2.encode("utf-8")

    with zipfile.ZipFile(QGZ_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in payload.items():
            zout.writestr(name, data)

    print("Patched km+mi scalebars in", QGZ_PATH)


if __name__ == "__main__":
    main()

