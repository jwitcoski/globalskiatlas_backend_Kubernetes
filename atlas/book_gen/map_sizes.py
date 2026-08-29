"""Map frame sizes for Scribus — match QGIS export plates (no extra scaling)."""

from __future__ import annotations

import struct
from pathlib import Path

from atlas.map_gen.layout_constants import LayoutTier, main_map_frame_mm

MM_PER_IN = 25.4
PT_PER_IN = 72.0

# Gap between stacked quarter-rows (normalized 0–1 of content height).
QUARTER_ROW_GAP_FRAC = 0.006
ROW_PAD_PT = 4.0
TEXT_MAP_GAP_PT = 8.0


def mm_to_pt(mm: float) -> float:
    return mm * PT_PER_IN / MM_PER_IN


def px_to_pt(px: float, dpi: int) -> float:
    return px * PT_PER_IN / float(dpi)


def tier_size_pt(tier: str) -> tuple[float, float]:
    """Print size of map plate from atlas layout tier (mm → pt)."""
    w_mm, h_mm = main_map_frame_mm(tier)  # type: ignore[arg-type]
    return mm_to_pt(w_mm), mm_to_pt(h_mm)


_DPI_INFER_TIERS = (
    "small",
    "medium",
    "large",
    "mega",
    "small_landscape",
    "medium_landscape",
    "large_landscape",
    "mega_landscape",
)


def infer_export_dpi(
    width_px: int,
    height_px: int,
    tier: str,
    *,
    try_all_plates: bool = True,
) -> int | None:
    """Guess QGIS export DPI from pixel size vs nominal plate mm."""
    tiers: list[str] = []
    t = str(tier or "small").strip().lower()
    if t:
        tiers.append(t)
        if not t.endswith("_landscape") and f"{t}_landscape" in _DPI_INFER_TIERS:
            tiers.append(f"{t}_landscape")
    if try_all_plates:
        for other in _DPI_INFER_TIERS:
            if other not in tiers:
                tiers.append(other)

    best_dpi: int | None = None
    best_err = 1e9
    for plate in tiers:
        try:
            w_mm, h_mm = main_map_frame_mm(plate)  # type: ignore[arg-type]
        except KeyError:
            continue
        for dpi in (300, 150, 72):
            for ew, eh in ((w_mm, h_mm), (h_mm, w_mm)):
                exp_w = ew / MM_PER_IN * dpi
                exp_h = eh / MM_PER_IN * dpi
                err = abs(width_px - exp_w) + abs(height_px - exp_h)
                if err < best_err:
                    best_err = err
                    best_dpi = dpi
    if best_err <= 24.0:
        return best_dpi
    return None


def map_dimensions_pt(
    map_path: str | Path | None,
    map_tier: str,
    *,
    default_dpi: int = 300,
) -> tuple[float, float]:
    """
    Physical size of map in Scribus points (1:1 with exported plate).

    Uses PNG pixels and inferred export DPI when a file exists; otherwise tier mm.
    DPI inference tries all known plates so a medium slot using a large PNG
    still gets the correct print size (not silently halved).
    """
    tier = str(map_tier or "small").strip().lower()
    if map_path:
        path = Path(map_path)
        if path.is_file():
            try:
                w_px, h_px = _png_size_px(path)
                # Infer plate from path when possible for better DPI match.
                parent = path.parent.name
                infer_tier = tier
                for name in (
                    "mega_landscape",
                    "large_landscape",
                    "medium_landscape",
                    "small_landscape",
                    "mega",
                    "large",
                    "medium",
                    "small",
                ):
                    token = name.replace("_", "-")
                    if f"-layout-{token}" in parent or parent.endswith(
                        f"-layout-{token}"
                    ):
                        infer_tier = name
                        break
                    if name.endswith("_landscape") and parent.endswith(
                        f"-layout-{name.split('_')[0]}-landscape"
                    ):
                        infer_tier = name
                        break
                dpi = infer_export_dpi(w_px, h_px, infer_tier) or default_dpi
                return px_to_pt(w_px, dpi), px_to_pt(h_px, dpi)
            except (OSError, ValueError):
                pass
    return tier_size_pt(tier)


def slot_height_fraction(map_h_pt: float, content_h_pt: float, *, pad_pt: float = ROW_PAD_PT) -> float:
    """Normalized vertical share of content area for one map row."""
    if content_h_pt <= 0:
        return 0.25
    return min(1.0, (map_h_pt + pad_pt) / content_h_pt)


def _png_size_px(path: Path) -> tuple[int, int]:
    """
    Read PNG width/height from IHDR without external deps (Pillow).

    PNG signature (8 bytes) then IHDR chunk: length (4) + type 'IHDR' (4) + data (13).
    Width/height are the first 8 bytes of IHDR data, big-endian.
    """
    with path.open("rb") as f:
        sig = f.read(8)
        if sig != b"\x89PNG\r\n\x1a\n":
            raise ValueError("not a PNG")
        length_bytes = f.read(4)
        ctype = f.read(4)
        if len(length_bytes) != 4 or len(ctype) != 4 or ctype != b"IHDR":
            raise ValueError("invalid PNG (missing IHDR)")
        ihdr = f.read(13)
        if len(ihdr) != 13:
            raise ValueError("invalid PNG (short IHDR)")
        w, h = struct.unpack(">II", ihdr[:8])
        if w <= 0 or h <= 0:
            raise ValueError("invalid PNG dimensions")
        return int(w), int(h)
