"""
Patch `ski_atlas_small_medium_template.qgz` to use two scale bars:

- Top: kilometers, FIXED width = 0.5 km (0, 0.5, 1 km)
- Bottom: miles, "scaleable" (FIT segment width in mm; values adapt with scale)

This keeps the original scalebar UUID for the km bar and ensures exactly one
additional miles bar exists (idempotent).
"""

from __future__ import annotations

import uuid as _uuid
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
QGZ_PATH = REPO_ROOT / "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz"
QGS_NAME = "wintergreen_map.qgs"

SCALEBAR_UUID = "{ab260a4b-b995-4ced-b6d7-0bfd8f8bce21}"
MI_MARKER = 'unitLabel="mi"'

# Layout positions (mm)
KM_Y = "125.8"
MI_Y = "131.8"


def _replace_attr(tag: str, key: str, value: str) -> str:
    """Replace key="..." in a LayoutItem opening tag."""
    import re

    return re.sub(rf'{key}="[^"]*"', f'{key}="{value}"', tag, count=1)


def _make_km_opening_tag(tag: str) -> str:
    # keep unitType="meters" but label/convert as km
    tag = _replace_attr(tag, "unitLabel", "km")
    tag = _replace_attr(tag, "numMapUnitsPerScaleBarUnit", "1000")
    tag = _replace_attr(tag, "numUnitsPerSegment", "0.5")
    tag = _replace_attr(tag, "numSegments", "2")
    # segmentSizeMode="0" corresponds to fixed-width (units) mode in QGIS XML
    tag = _replace_attr(tag, "segmentSizeMode", "0")
    tag = _replace_attr(tag, "positionOnPage", f"0,{KM_Y},mm")
    tag = _replace_attr(tag, "position", f"0,{KM_Y},mm")
    return tag


def _make_mi_opening_tag(tag: str) -> str:
    # 1 mile = 1609.344 meters
    tag = _replace_attr(tag, "unitLabel", "mi")
    tag = _replace_attr(tag, "numMapUnitsPerScaleBarUnit", "1609.344")
    # Fit segment width mode: distances adapt; keep a reasonable segment width + count.
    tag = _replace_attr(tag, "segmentSizeMode", "1")
    tag = _replace_attr(tag, "segmentMillimeters", "11.1456")
    tag = _replace_attr(tag, "numSegments", "2")
    tag = _replace_attr(tag, "numUnitsPerSegment", "1")
    tag = _replace_attr(tag, "positionOnPage", f"0,{MI_Y},mm")
    tag = _replace_attr(tag, "position", f"0,{MI_Y},mm")
    return tag


def _find_layoutitem_block(text: str, start_idx: int) -> tuple[int, int]:
    """Return (start,end) indices for the LayoutItem block containing start_idx."""
    start = text.rfind("<LayoutItem", 0, start_idx)
    if start < 0:
        raise ValueError("Could not locate opening <LayoutItem")
    end = text.find("</LayoutItem>", start)
    if end < 0:
        raise ValueError("Could not locate closing </LayoutItem>")
    end += len("</LayoutItem>")
    return start, end


def main() -> None:
    if not QGZ_PATH.exists():
        raise SystemExit(f"Template not found: {QGZ_PATH}")

    with zipfile.ZipFile(QGZ_PATH, "r") as zin:
        payload = {info.filename: zin.read(info.filename) for info in zin.infolist()}

    qgs = payload[QGS_NAME].decode("utf-8", errors="replace")

    # Find the full scalebar item block by uuid (this is our km bar).
    open_tok = f'<LayoutItem uuid="{SCALEBAR_UUID}"'
    km_pos = qgs.find(open_tok)
    if km_pos < 0:
        raise SystemExit("Scale bar LayoutItem not found in template QGS.")
    km_start, km_end = _find_layoutitem_block(qgs, km_pos)
    block = qgs[km_start:km_end]

    # Split opening tag vs inner xml vs closing.
    close_of_opening = block.find(">")
    opening = block[: close_of_opening + 1]
    inner_plus_close = block[close_of_opening + 1 :]

    # 1) Convert existing to kilometers (top bar).
    opening_km = _make_km_opening_tag(opening)
    block_km = opening_km + inner_plus_close

    # Remove any existing extra scale bars (e.g. from previous runs), preserving only the km bar.
    # We do this before inserting the new miles bar so the script is idempotent.
    import re

    scalebar_open_tags = list(re.finditer(r"<LayoutItem\b[^>]*\btype=\"65646\"[^>]*>", qgs))
    to_remove: list[tuple[int, int]] = []
    for m in scalebar_open_tags:
        if f'uuid="{SCALEBAR_UUID}"' in m.group(0):
            continue
        s, e = _find_layoutitem_block(qgs, m.start())
        to_remove.append((s, e))

    if to_remove:
        # Remove from back to front to preserve indices
        for s, e in sorted(to_remove, key=lambda x: x[0], reverse=True):
            qgs = qgs[:s] + qgs[e:]
        # Recompute km indices after removal
        km_pos = qgs.find(open_tok)
        km_start, km_end = _find_layoutitem_block(qgs, km_pos)

    # 2) Duplicate as miles (bottom bar). Reuse existing mi uuid if present, else generate.
    mi_uuid = None
    mi_pos = qgs.find(MI_MARKER)
    if mi_pos >= 0:
        mi_start, mi_end = _find_layoutitem_block(qgs, mi_pos)
        mi_open_end = qgs.find(">", mi_start)
        mi_opening = qgs[mi_start : mi_open_end + 1]
        import re as _re
        mm = _re.search(r'uuid="(\{[^}]+\})"', mi_opening)
        if mm:
            mi_uuid = mm.group(1)
    if mi_uuid is None:
        mi_uuid = "{" + str(_uuid.uuid4()) + "}"

    opening_mi = opening_km
    opening_mi = opening_mi.replace(f'uuid="{SCALEBAR_UUID}"', f'uuid="{mi_uuid}"', 1)
    opening_mi = opening_mi.replace(f'templateUuid="{SCALEBAR_UUID}"', f'templateUuid="{mi_uuid}"', 1)
    opening_mi = _make_mi_opening_tag(opening_mi)
    block_mi = opening_mi + inner_plus_close

    # Replace original block with km+mi blocks (km first).
    qgs2 = qgs[:km_start] + block_km + "\n" + block_mi + qgs[km_end:]

    payload[QGS_NAME] = qgs2.encode("utf-8")

    with zipfile.ZipFile(QGZ_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name, data in payload.items():
            zout.writestr(name, data)

    print("Patched dual-unit scale bars into:", QGZ_PATH)


if __name__ == "__main__":
    main()

