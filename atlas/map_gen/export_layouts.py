"""
Export all atlas_work QGZ files as PNG images using the QGIS layout engine.

Finds every *_map.qgz in atlas_work/, opens the "Ski Atlas Export" print
layout in each, and saves a PNG alongside the QGZ.

Run via the batch wrapper (recommended):
    atlas\\map_gen\\run_export_layouts.bat [--dpi 150] [--work-dir atlas_work]

Or directly with QGIS's own Python (qgis-python-env must be active):
    python atlas/map_gen/export_layouts.py [options]
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Optional

if os.name == "nt":
    # Headless Qt sometimes fails to discover Windows fonts, resulting in "tofu" squares in exports.
    os.environ.setdefault("QT_QPA_FONTDIR", r"C:\Windows\Fonts")


LAYOUT_NAME = "Ski Atlas Export"


# ── QGIS path auto-detection ─────────────────────────────────────────────────

def _find_qgis_root() -> Optional[Path]:
    """Return the QGIS installation root (the directory that contains bin/ and apps/)."""
    import glob as _glob

    candidates: list[Path] = []

    # 1. Environment override
    env = os.environ.get("QGIS_PREFIX_PATH")
    if env:
        p = Path(env)
        # PREFIX_PATH might point to apps/qgis; go up two levels to get the root
        for _ in range(3):
            if (p / "bin" / "qgis_process.exe").exists():
                return p
            if (p / "bin" / "qgis-ltr-bin.exe").exists():
                return p
            p = p.parent

    # 2. Scan Program Files for standalone QGIS installers
    for base in [r"C:\Program Files", r"C:\Program Files (x86)"]:
        for qdir in sorted(Path(base).glob("QGIS*"), reverse=True):
            candidates.append(qdir)

    # 3. OSGeo4W paths
    for osgeo in [r"C:\OSGeo4W64", r"C:\OSGeo4W"]:
        candidates.append(Path(osgeo))

    # 4. Look for qgis_process.exe on PATH
    import shutil
    qp = shutil.which("qgis_process")
    if qp:
        candidates.insert(0, Path(qp).parents[1])

    for root in candidates:
        if not root.exists():
            continue
        if (root / "bin" / "qgis_process.exe").exists():
            return root
        if (root / "bin" / "qgis-ltr-bin.exe").exists():
            return root
        if (root / "bin" / "qgis-bin.exe").exists():
            return root

    return None


def _setup_standalone_qgis(qgis_root: Path):
    """Add QGIS Python paths so `from qgis.core import ...` works."""
    # OSGeo4W layout: root/apps/qgis/python
    # Standalone layout: root/apps/qgis/python  (same)
    python_home = qgis_root / "apps" / "qgis" / "python"
    if not python_home.exists():
        python_home = qgis_root / "python"

    plugins = python_home / "plugins"
    for p in [str(python_home), str(plugins)]:
        if p not in sys.path:
            sys.path.insert(0, p)

    prefix = str(qgis_root / "apps" / "qgis")
    if not Path(prefix).exists():
        prefix = str(qgis_root)

    os.environ.setdefault("QGIS_PREFIX_PATH", prefix)
    os.environ["QT_QPA_PLATFORM"] = "offscreen"

    # DLLs
    bin_dir = str(qgis_root / "bin")
    apps_bin = str(qgis_root / "apps" / "qgis" / "bin")
    current_path = os.environ.get("PATH", "")
    for d in [bin_dir, apps_bin]:
        if d not in current_path:
            os.environ["PATH"] = d + ";" + current_path


# Singleton headless app so data_to_qgis can export each QGZ in-process without re-init per file.
_headless_qgis_app = None
_headless_qgis_app_owned = False


def ensure_headless_qgis_initialized(qgis_root: Optional[Path] = None) -> None:
    """Start one global QgsApplication unless already running (e.g. inside QGIS).

    Raises RuntimeError if QGIS cannot be located or initialized.
    """
    global _headless_qgis_app, _headless_qgis_app_owned
    if _headless_qgis_app is not None:
        return
    try:
        import qgis.core  # noqa: F401
    except ImportError:
        root = qgis_root or _find_qgis_root()
        if root is None:
            raise RuntimeError(
                "QGIS Python bindings are not available and no QGIS installation was found. "
                "Set QGIS_PREFIX_PATH, pass --qgis-root, or run via atlas\\map_gen\\run_export_layouts.bat"
            )
        print(f"Using QGIS at: {root}")
        _setup_standalone_qgis(root)

    from qgis.core import QgsApplication

    existing = QgsApplication.instance()
    if existing is not None:
        _headless_qgis_app = existing
        _headless_qgis_app_owned = False
        return

    _headless_qgis_app = QgsApplication([], False)
    _headless_qgis_app.initQgis()
    _headless_qgis_app_owned = True


def shutdown_headless_qgis_if_initialized() -> None:
    global _headless_qgis_app, _headless_qgis_app_owned
    if _headless_qgis_app is None:
        return
    if _headless_qgis_app_owned:
        _headless_qgis_app.exitQgis()
    _headless_qgis_app = None
    _headless_qgis_app_owned = False


# ── Export logic ─────────────────────────────────────────────────────────────

def _zoom_main_map_to_buffer(project, layout) -> bool:
    """Zoom the largest layout map item to the 1000ft buffer layer extent.

    Padding + aspect expansion match data_to_qgis so the geographic span fits the
    fixed mm map frame without letterboxing. Returns True if successfully zoomed.
    """
    from qgis.core import QgsCoordinateTransform, QgsLayoutItemMap, QgsRectangle

    # Find the 1000ft buffer layer by name (most reliable for generated projects)
    buffer_layer = None
    for candidate_name in ("ski_areas_1000ft_buffer", "ski_area_1000ft_buffer"):
        layers = project.mapLayersByName(candidate_name)
        if layers:
            buffer_layer = layers[0]
            break
    # Fallback: search by URI substring
    if buffer_layer is None:
        for layer in project.mapLayers().values():
            try:
                uri = layer.dataProvider().dataSourceUri()
            except Exception:
                continue
            if "1000ft_buffer" in uri:
                buffer_layer = layer
                break

    if buffer_layer is None:
        print("  [zoom] buffer layer not found — skipping zoom")
        return False

    ext = buffer_layer.extent()
    if ext.isNull() or ext.isEmpty():
        print("  [zoom] buffer extent is empty — skipping zoom")
        return False

    # Find the largest (main) map item — not the inset globe
    main_map = None
    max_area = 0.0
    for item in layout.items():
        if isinstance(item, QgsLayoutItemMap):
            sz = item.sizeWithUnits()
            area = sz.width() * sz.height()
            if area > max_area:
                max_area = area
                main_map = item

    if main_map is None:
        print("  [zoom] no map item found in layout — skipping zoom")
        return False

    # 5% padding on each side so the buffer boundary isn't clipped
    dx = ext.width() * 0.05
    dy = ext.height() * 0.05
    padded = QgsRectangle(
        ext.xMinimum() - dx, ext.yMinimum() - dy,
        ext.xMaximum() + dx, ext.yMaximum() + dy,
    )
    from atlas.map_gen.layout_constants import (
        expand_bounds_for_rotation,
        expand_bounds_to_main_map_aspect,
    )

    b = (
        padded.xMinimum(),
        padded.yMinimum(),
        padded.xMaximum(),
        padded.yMaximum(),
    )
    b2 = expand_bounds_to_main_map_aspect(b)
    b3 = expand_bounds_for_rotation(b2, float(main_map.mapRotation()))
    padded = QgsRectangle(b3[0], b3[1], b3[2], b3[3])
    map_crs = main_map.crs()
    layer_crs = buffer_layer.crs()
    if map_crs.isValid() and layer_crs.isValid() and map_crs != layer_crs:
        xform = QgsCoordinateTransform(layer_crs, map_crs, project)
        padded = xform.transformBoundingBox(padded)
    # zoomToExtent fits the rectangle into the map frame respecting rotation + aspect ratio
    main_map.zoomToExtent(padded)
    main_map.invalidateCache()
    main_map.refresh()
    layout.refresh()
    print(f"  [zoom] zoomed main map to buffer extent "
          f"({ext.width():.4f}° × {ext.height():.4f}°)")
    return True


def export_qgz(qgz_path: Path, dpi: int, overwrite: bool) -> bool:
    """Export the 'Ski Atlas Export' layout from qgz_path as a PNG.

    Returns True on success, False on failure.
    """
    from qgis.core import QgsProject, QgsPrintLayout, QgsLayoutExporter

    out_png = qgz_path.with_name(qgz_path.stem.replace("_map", "") + "_export.png")
    if out_png.exists() and not overwrite:
        print(f"  skip (exists): {out_png.name}")
        return True

    project = QgsProject.instance()
    project.clear()
    if not project.read(str(qgz_path)):
        print(f"  ERROR: could not open {qgz_path.name}")
        return False

    manager = project.layoutManager()
    layout = manager.layoutByName(LAYOUT_NAME)
    if layout is None:
        layouts = [l.name() for l in manager.layouts()]
        print(f"  ERROR: layout '{LAYOUT_NAME}' not found. Available: {layouts}")
        project.clear()
        return False

    _zoom_main_map_to_buffer(project, layout)

    exporter = QgsLayoutExporter(layout)
    settings = QgsLayoutExporter.ImageExportSettings()
    settings.dpi = dpi

    result = exporter.exportToImage(str(out_png), settings)
    project.clear()

    if result == QgsLayoutExporter.Success:
        size = out_png.stat().st_size // 1024
        print(f"  exported → {out_png.name}  ({size} KB)")
        return True
    else:
        codes = {
            QgsLayoutExporter.PrintError: "PrintError",
            QgsLayoutExporter.SvgLayerClipped: "SvgLayerClipped",
            QgsLayoutExporter.MemoryError: "MemoryError",
            QgsLayoutExporter.FileNotWritable: "FileNotWritable",
            QgsLayoutExporter.IteratorError: "IteratorError",
            QgsLayoutExporter.Canceled: "Canceled",
        }
        print(f"  ERROR exporting {qgz_path.name}: {codes.get(result, result)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Export atlas_work QGZ files to PNG via QGIS layout engine"
    )
    parser.add_argument(
        "--work-dir", type=Path, default=None,
        help="Path to atlas_work directory (default: auto-detected from repo root)",
    )
    parser.add_argument(
        "--dpi", type=int, default=150,
        help="Export resolution in DPI (default: 150)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Re-export even if PNG already exists",
    )
    parser.add_argument(
        "--qgis-root", type=Path, default=None,
        help="Path to QGIS installation root (auto-detected if omitted)",
    )
    parser.add_argument(
        "--slug",
        action="append",
        default=None,
        metavar="DIR",
        help="Only export atlas_work/<slug>/*_map.qgz (repeat for multiple resorts)",
    )
    args = parser.parse_args()

    # ── Resolve work directory ────────────────────────────────────────────────
    repo_root = Path(__file__).resolve().parents[2]
    work_dir = args.work_dir
    if work_dir is None:
        try:
            import tomllib  # Python 3.11+
        except ImportError:
            try:
                import tomli as tomllib
            except ImportError:
                tomllib = None
        cfg_path = repo_root / "atlas_config.toml"
        work_dir_str = "atlas_work"
        if tomllib and cfg_path.exists():
            with open(cfg_path, "rb") as f:
                cfg = tomllib.load(f)
            work_dir_str = cfg.get("work_dir", "atlas_work")
        work_dir = Path(work_dir_str)
    if not work_dir.is_absolute():
        work_dir = repo_root / work_dir

    if not work_dir.exists():
        print(f"Work directory not found: {work_dir}")
        sys.exit(1)

    qgz_files = sorted(work_dir.rglob("*_map.qgz"))
    if args.slug:
        want = set(args.slug)
        qgz_files = [p for p in qgz_files if p.parent.name in want]
        if not qgz_files:
            print(f"No *_map.qgz matched --slug {sorted(want)!r} under {work_dir}")
            sys.exit(0)

    if not qgz_files:
        print(f"No *_map.qgz files found under {work_dir}")
        sys.exit(0)

    print(f"Found {len(qgz_files)} QGZ file(s) to export under {work_dir}")

    try:
        ensure_headless_qgis_initialized(args.qgis_root)
    except RuntimeError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(2)

    ok = 0
    fail = 0
    t0 = time.time()

    try:
        for qgz in qgz_files:
            slug = qgz.parent.name
            print(f"{slug}")
            if export_qgz(qgz, dpi=args.dpi, overwrite=args.overwrite):
                ok += 1
            else:
                fail += 1
    finally:
        shutdown_headless_qgis_if_initialized()

    elapsed = time.time() - t0
    print(f"\nDone: {ok} exported, {fail} failed  ({elapsed:.1f}s)")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
