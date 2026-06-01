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
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

if os.name == "nt":
    # Headless Qt sometimes fails to discover Windows fonts, resulting in "tofu" squares in exports.
    os.environ.setdefault("QT_QPA_FONTDIR", r"C:\Windows\Fonts")


LAYOUT_NAME = "Ski Atlas Export"
OVERVIEW_LAYOUT_NAME = "Regional Overview"

_INSET_LAYER_NAMES = frozenset(
    {
        "globe_countries",
        "overview_resort_point",
        "overview_countries",
        "overview_states",
    }
)


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

    print("  Initializing QGIS (first start can take 1-3 minutes)...", flush=True)
    _headless_qgis_app = QgsApplication([], False)
    _headless_qgis_app.initQgis()
    _headless_qgis_app_owned = True
    print("  QGIS ready.", flush=True)


def shutdown_headless_qgis_if_initialized() -> None:
    global _headless_qgis_app, _headless_qgis_app_owned
    if _headless_qgis_app is None:
        return
    if _headless_qgis_app_owned:
        _headless_qgis_app.exitQgis()
    _headless_qgis_app = None
    _headless_qgis_app_owned = False


# ── Export logic ─────────────────────────────────────────────────────────────


def _layout_map_frame_mm(layout, main_map) -> tuple[float, float]:
    """Map item width/height in millimeters (matches data_to_qgis frame for each tier)."""
    ls = main_map.sizeWithUnits()
    try:
        from qgis.core import Qgis

        mm_unit = Qgis.LayoutUnit.LayoutMillimeters
    except (ImportError, AttributeError):
        from qgis.core import QgsUnitTypes

        mm_unit = QgsUnitTypes.LayoutMillimeters
    try:
        mm = layout.convertFromLayoutUnits(ls, mm_unit)
        fw, fh = float(mm.width()), float(mm.height())
        if fw > 0 and fh > 0:
            return fw, fh
    except Exception:
        pass
    if ls.units() == mm_unit:
        return float(ls.width()), float(ls.height())
    from atlas.map_gen.layout_constants import (
        MAIN_MAP_FRAME_HEIGHT_MM,
        MAIN_MAP_FRAME_WIDTH_MM,
    )

    return MAIN_MAP_FRAME_WIDTH_MM, MAIN_MAP_FRAME_HEIGHT_MM


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
    fw_mm, fh_mm = _layout_map_frame_mm(layout, main_map)
    b2 = expand_bounds_to_main_map_aspect(
        b, frame_width_mm=fw_mm, frame_height_mm=fh_mm
    )
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


def out_png_for_qgz(qgz_path: Path) -> Path:
    """PNG path written alongside a *_map.qgz project."""
    return qgz_path.with_name(qgz_path.stem.replace("_map", "") + "_export.png")


def _inset_centroid_from_geojson(qgz_path: Path) -> Optional[tuple[float, float]]:
    geojson = qgz_path.parent / "resort_inset_point.geojson"
    if not geojson.exists():
        return None
    try:
        data = json.loads(geojson.read_text(encoding="utf-8"))
        feat = (data.get("features") or [None])[0]
        if not feat:
            return None
        coords = feat.get("geometry", {}).get("coordinates")
        if not coords or len(coords) < 2:
            return None
        return float(coords[0]), float(coords[1])
    except Exception:
        return None


def _host_country_name(qgz_path: Path) -> str:
    ski = qgz_path.parent / "data" / "ski_areas.parquet"
    if ski.exists():
        try:
            import geopandas as gpd

            g = gpd.read_parquet(ski)
            if not g.empty and "Country" in g.columns:
                c = str(g.iloc[0]["Country"]).strip()
                if c and c.casefold() not in {"nan", "none"}:
                    return c
        except Exception:
            pass
    return "United States of America"


def _ortho_crs_for_lonlat(lon: float, lat: float):
    from qgis.core import QgsCoordinateReferenceSystem

    proj4 = (
        f"+proj=ortho +lat_0={lat} +lon_0={lon} "
        "+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
    )
    crs = QgsCoordinateReferenceSystem.fromProj(proj4)
    if not crs.isValid():
        crs = QgsCoordinateReferenceSystem.fromProj4(proj4)
    return crs


def _apply_globe_symbology(globe_layer, country: str) -> None:
    """Rule renderer that paints all countries beige and highlights the host (matches Wintergreen)."""
    from qgis.core import QgsFillSymbol, QgsRuleBasedRenderer

    country_esc = country.replace("'", "''")
    tan_sym = QgsFillSymbol.createSimple(
        {
            "color": "236,228,208,255",
            "outline_color": "218,212,198,255",
            "outline_width": "0.07",
            "outline_width_unit": "MM",
        }
    )
    rose_sym = QgsFillSymbol.createSimple(
        {
            "color": "218,188,184,255",
            "outline_color": "165,138,132,255",
            "outline_width": "0.07",
            "outline_width_unit": "MM",
        }
    )
    renderer = QgsRuleBasedRenderer(tan_sym)
    root = renderer.rootRule()
    rule_hi = QgsRuleBasedRenderer.Rule(rose_sym)
    rule_hi.setFilterExpression(
        f"\"ADMIN\" = '{country_esc}' OR \"NAME\" = '{country_esc}' "
        f"OR \"SOVEREIGNT\" = '{country_esc}' OR \"ADMIN\" ILIKE '{country_esc}%'"
    )
    rule_hi.setDescription("Host country")
    root.appendChild(rule_hi)
    globe_layer.setRenderer(renderer)
    globe_layer.triggerRepaint()


def _apply_inset_star(point_layer) -> None:
    from qgis.core import (
        QgsMarkerSymbol,
        QgsSimpleMarkerSymbolLayer,
        QgsSingleSymbolRenderer,
    )
    from qgis.PyQt.QtGui import QColor

    try:
        from qgis.core import Qgis

        mm_unit = Qgis.RenderUnit.RenderMillimeters
    except (ImportError, AttributeError):
        from qgis.core import QgsUnitTypes

        mm_unit = QgsUnitTypes.RenderMillimeters

    sym = QgsMarkerSymbol()
    while sym.symbolLayerCount():
        sym.deleteSymbolLayer(0)
    star = QgsSimpleMarkerSymbolLayer()
    star.setShape(QgsSimpleMarkerSymbolLayer.Star)
    star.setSize(2.75)
    star.setSizeUnit(mm_unit)
    star.setFillColor(QColor(0, 0, 0))
    star.setStrokeColor(QColor(255, 255, 255))
    star.setStrokeWidth(0.35)
    sym.appendSymbolLayer(star)
    point_layer.setRenderer(QgsSingleSymbolRenderer(sym))
    point_layer.triggerRepaint()


def _fix_main_map_exclude_inset_layers(project, layout) -> None:
    """Main map frame must not draw globe/star/overview layers (only on inset maps)."""
    from qgis.core import QgsLayoutItemMap

    for name in _INSET_LAYER_NAMES:
        for layer in project.mapLayersByName(name):
            node = project.layerTreeRoot().findLayer(layer.id())
            if node is not None:
                node.setItemVisibilityChecked(False)

    main_map = None
    max_area = 0.0
    for item in layout.items():
        if not isinstance(item, QgsLayoutItemMap):
            continue
        if item.id().startswith("overview"):
            continue
        area = item.sizeWithUnits().width() * item.sizeWithUnits().height()
        if area > max_area:
            max_area = area
            main_map = item

    if main_map is None:
        return

    main_layers = [
        lyr
        for lyr in project.mapLayers().values()
        if lyr.name() not in _INSET_LAYER_NAMES and lyr.isValid()
    ]
    if main_layers:
        main_map.setFollowVisibilityPreset(False)
        main_map.setKeepLayerSet(True)
        main_map.setLayers(main_layers)
        main_map.refresh()


def _fix_inset_maps_for_export(project, layout, qgz_path: Path) -> None:
    """Headless export: ortho globe, host-country symbology, star, and ellipse clip."""
    from qgis.core import (
        QgsCoordinateReferenceSystem,
        QgsCoordinateTransform,
        QgsLayoutItemMap,
        QgsRectangle,
    )

    globe_layers = project.mapLayersByName("globe_countries")
    point_layers = project.mapLayersByName("overview_resort_point")
    globe_layer = globe_layers[0] if globe_layers else None
    point_layer = point_layers[0] if point_layers else None

    if globe_layer is not None:
        _apply_globe_symbology(globe_layer, _host_country_name(qgz_path))
    if point_layer is not None:
        _apply_inset_star(point_layer)

    centroid = _inset_centroid_from_geojson(qgz_path)
    ortho_crs = None
    ortho_extent = None
    if centroid is not None:
        lon, lat = centroid
        ortho_crs = _ortho_crs_for_lonlat(lon, lat)
        if ortho_crs.isValid():
            from atlas.map_gen.data_to_qgis import _ortho_extent_for_globe_window

            xmin, ymin, xmax, ymax = _ortho_extent_for_globe_window(lon, lat)
            ortho_extent = QgsRectangle(xmin, ymin, xmax, ymax)

    clip_shape = layout.itemById("globe_clip_shape")

    for item_id, layers in (
        ("overview_inset_map", [globe_layer] if globe_layer else []),
        ("overview_inset_dot", [point_layer] if point_layer else []),
    ):
        item = layout.itemById(item_id)
        if item is None or not isinstance(item, QgsLayoutItemMap):
            continue
        if layers:
            item.setFollowVisibilityPreset(False)
            item.setKeepLayerSet(True)
            item.setLayers(layers)
        if ortho_crs is not None and ortho_crs.isValid():
            item.setCrs(ortho_crs)
        if ortho_extent is not None and not ortho_extent.isEmpty():
            item.setExtent(ortho_extent)
        item.setBackgroundEnabled(False)
        item.setVisibility(True)
        if clip_shape is not None:
            clip = item.itemClippingSettings()
            clip.setEnabled(True)
            clip.setSourceItem(clip_shape)
        item.refresh()

    layout.refresh()


def export_qgz(qgz_path: Path, dpi: int, overwrite: bool) -> bool:
    """Export the 'Ski Atlas Export' layout from qgz_path as a PNG.

    Returns True on success, False on failure.
    """
    from qgis.core import QgsProject, QgsPrintLayout, QgsLayoutExporter

    out_png = out_png_for_qgz(qgz_path)
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
    _fix_main_map_exclude_inset_layers(project, layout)
    _fix_inset_maps_for_export(project, layout, qgz_path)

    exporter = QgsLayoutExporter(layout)
    settings = QgsLayoutExporter.ImageExportSettings()
    settings.dpi = dpi

    # Write to a temp file first: GDAL raises ERROR 6 if exportToImage targets an
    # existing PNG (even after unlink on Windows / with some QGIS builds).
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".png", prefix="atlas_export_")
    os.close(tmp_fd)
    tmp_png = Path(tmp_name)
    try:
        from osgeo import gdal

        gdal.PushErrorHandler("CPLQuietErrorHandler")
        gdal_quiet = True
    except Exception:
        gdal_quiet = False
    try:
        result = exporter.exportToImage(str(tmp_png), settings)
    finally:
        if gdal_quiet:
            try:
                from osgeo import gdal

                gdal.PopErrorHandler()
            except Exception:
                pass
        project.clear()

    if result == QgsLayoutExporter.Success:
        try:
            out_png.parent.mkdir(parents=True, exist_ok=True)
            if out_png.exists():
                out_png.unlink()
            shutil.move(str(tmp_png), str(out_png))
            tmp_png = Path()  # moved; do not delete in outer finally
        except OSError as e:
            print(f"  ERROR moving export to {out_png.name}: {e}", file=sys.stderr)
            return False
        size = out_png.stat().st_size // 1024
        print(f"  exported → {out_png.name}  ({size} KB)")
        return True

    if tmp_png.exists():
        tmp_png.unlink(missing_ok=True)
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


def export_overview_qgz(qgz_path: Path, dpi: int, overwrite: bool) -> bool:
    """Export the 'Regional Overview' layout from a *_overview_map.qgz project as a PNG."""
    from qgis.core import QgsProject, QgsLayoutExporter

    out_png = out_png_for_qgz(qgz_path)
    if out_png.exists() and not overwrite:
        print(f"  skip (exists): {out_png.name}")
        return True

    project = QgsProject.instance()
    project.clear()
    if not project.read(str(qgz_path)):
        print(f"  ERROR: could not open {qgz_path.name}")
        return False

    manager = project.layoutManager()
    layout = manager.layoutByName(OVERVIEW_LAYOUT_NAME)
    if layout is None:
        layouts = [l.name() for l in manager.layouts()]
        print(
            f"  ERROR: layout '{OVERVIEW_LAYOUT_NAME}' not found. Available: {layouts}"
        )
        project.clear()
        return False

    exporter = QgsLayoutExporter(layout)
    settings = QgsLayoutExporter.ImageExportSettings()
    settings.dpi = dpi

    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".png", prefix="atlas_overview_export_")
    os.close(tmp_fd)
    tmp_png = Path(tmp_name)
    try:
        result = exporter.exportToImage(str(tmp_png), settings)
    finally:
        project.clear()

    if result == QgsLayoutExporter.Success:
        try:
            out_png.parent.mkdir(parents=True, exist_ok=True)
            if out_png.exists():
                out_png.unlink()
            shutil.move(str(tmp_png), str(out_png))
            tmp_png = Path()  # moved; do not delete below
        except OSError as e:
            print(f"  ERROR moving export to {out_png.name}: {e}", file=sys.stderr)
            return False
        size = out_png.stat().st_size // 1024
        print(f"  exported → {out_png.name}  ({size} KB)")
        return True

    if tmp_png.exists():
        tmp_png.unlink(missing_ok=True)
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
    parser.add_argument(
        "--overviews",
        action="store_true",
        help="Export *_overview_map.qgz using the 'Regional Overview' layout",
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

    if args.overviews:
        qgz_files = sorted(work_dir.rglob("*_overview_map.qgz"))
    else:
        qgz_files = sorted(work_dir.rglob("*_map.qgz"))
    if args.slug:
        want = set(args.slug)
        qgz_files = [p for p in qgz_files if p.parent.name in want]
        if not qgz_files:
            pat = "*_overview_map.qgz" if args.overviews else "*_map.qgz"
            print(f"No {pat} matched --slug {sorted(want)!r} under {work_dir}")
            sys.exit(0)

    if not qgz_files:
        pat = "*_overview_map.qgz" if args.overviews else "*_map.qgz"
        print(f"No {pat} files found under {work_dir}")
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
            if args.overviews:
                ok_this = export_overview_qgz(qgz, dpi=args.dpi, overwrite=args.overwrite)
            else:
                ok_this = export_qgz(qgz, dpi=args.dpi, overwrite=args.overwrite)
            if ok_this:
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
