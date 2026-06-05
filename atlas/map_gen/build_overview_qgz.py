#!/usr/bin/env python3
"""
Build a regional overview .qgz from exported GeoJSON (PyQGIS).

Usage:
  python -m atlas.map_gen.build_overview_qgz --dir atlas_work/overview/states/united-states-of-america/pennsylvania
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import zipfile
import math
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OVERVIEW_ICONS_DIR = REPO_ROOT / "atlas/map_gen/icons"

TIER_MM = {"small": 4.0, "medium": 5.5, "large": 7.0, "mega": 9.0}
TIER_ICON = {
    "small": "overview_tier_small.svg",
    "medium": "overview_tier_medium.svg",
    "large": "overview_tier_large.svg",
    "mega": "overview_tier_mega.svg",
}
# Per-tier label prominence: (font pt, text color, label priority, callout width mm)
TIER_LABEL = {
    "small": (5.5, "#757575", 4, 0.25),
    "medium": (6.5, "#4a4a4a", 6, 0.3),
    "large": (7.5, "#1a1a1a", 8, 0.35),
    "mega": (8.5, "#000000", 10, 0.4),
}
PAGE_W_MM = 210.0
PAGE_H_MM = 148.5
BG_COLOR = "#c8d4b8"
OUTLINE_COLOR = "#1a3d1a"
# Keep in sync with atlas.map_gen.overview_dem.DEM_STYLE_VERSION
DEM_STYLE_VERSION = 5
HILLSHADE_OVERLAY_OPACITY = 0.5
BOUNDARY_OUTLINE_MM = 1.2


def _patch_overview_qgz_xml(qgz_path: Path) -> None:
    """Force main_map to half-page size (PyQGIS sometimes omits dimensions)."""
    if not qgz_path.is_file():
        return
    with zipfile.ZipFile(qgz_path, "r") as zin:
        items: list[tuple[zipfile.ZipInfo, bytes]] = [
            (info, zin.read(info.filename)) for info in zin.infolist()
        ]

    qgs_names = [info.filename for info, _ in items if info.filename.endswith(".qgs")]
    if not qgs_names:
        return
    src_qgs = qgs_names[0]

    def patch_tag(match: re.Match[str]) -> str:
        tag = match.group(0)
        if 'id="main_map"' not in tag:
            return tag
        tag = re.sub(
            r'size="[^"]*"',
            f'size="{PAGE_W_MM},{PAGE_H_MM},mm"',
            tag,
            count=1,
        )
        tag = re.sub(r'positionOnPage="[^"]*"', 'positionOnPage="0,0,mm"', tag, count=1)
        tag = re.sub(r'position="[^"]*"', 'position="0,0,mm"', tag, count=1)
        # Avoid expensive canvas item rendering in layout preview (can appear to "hang").
        tag = tag.replace('drawCanvasItems="true"', 'drawCanvasItems="false"')
        return tag

    out_items: list[tuple[zipfile.ZipInfo, bytes]] = []
    for info, data in items:
        if info.filename == src_qgs:
            text = data.decode("utf-8")
            text = re.sub(r"<LayoutItem\b[^>]*>", patch_tag, text)
            data = text.encode("utf-8")
        out_items.append((info, data))

    with zipfile.ZipFile(qgz_path, "w", zipfile.ZIP_DEFLATED) as zout:
        for info, data in out_items:
            zout.writestr(info, data)


def _venv_subprocess_env() -> dict[str, str]:
    """Isolate venv Python from QGIS's bundled interpreter on PATH."""
    import os

    env = os.environ.copy()
    env.pop("PYTHONHOME", None)
    env.pop("PYTHONPATH", None)
    parts = [
        p
        for p in env.get("PATH", "").split(os.pathsep)
        if p and "qgis" not in p.lower() and "osgeo4w" not in p.lower()
    ]
    env["PATH"] = os.pathsep.join(parts)
    return env


def _ensure_dem_layers(
    out_dir: Path, map_crs: str, meta: dict
) -> tuple[Path, Path, Path | None] | None:
    """Build dem_color + hillshade overlay + mist in map CRS (venv / rasterio)."""
    import subprocess

    color_tif = out_dir / "dem_color.tif"
    overlay_tif = out_dir / "dem_hillshade_overlay.tif"
    mist_tif = out_dir / "dem_mist.tif"
    legacy_tif = out_dir / "dem_hillshade.tif"

    style_ok = int(meta.get("dem_style_version") or 0) >= DEM_STYLE_VERSION
    if (
        color_tif.is_file()
        and overlay_tif.is_file()
        and mist_tif.is_file()
        and meta.get("crs") == map_crs
        and style_ok
    ):
        return color_tif, overlay_tif, mist_tif

    venv_py = REPO_ROOT / ".venv/Scripts/python.exe"
    if not venv_py.is_file():
        print("  Warning: .venv not found; cannot rebuild DEM", file=sys.stderr)
        if color_tif.is_file() and overlay_tif.is_file():
            return color_tif, overlay_tif, mist_tif if mist_tif.is_file() else None
        return (legacy_tif, legacy_tif, None) if legacy_tif.is_file() else None

    print("  Building DEM color + hillshade + mist in map CRS...", flush=True)
    subprocess.run(
        [str(venv_py), "-m", "atlas.map_gen.overview_dem", str(out_dir), "--crs", map_crs],
        cwd=str(REPO_ROOT),
        check=True,
        env=_venv_subprocess_env(),
    )
    if color_tif.is_file() and overlay_tif.is_file():
        return color_tif, overlay_tif, mist_tif if mist_tif.is_file() else None
    return (legacy_tif, legacy_tif, None) if legacy_tif.is_file() else None


def _build(
    out_dir: Path,
    qgis_root: Path | None = None,
    *,
    skip_dem: bool = False,
    skip_labels: bool = False,
) -> Path:
    from atlas.map_gen.export_layouts import ensure_headless_qgis_initialized, shutdown_headless_qgis_if_initialized

    admin_path = out_dir / "admin_boundary.geojson"
    resorts_path = out_dir / "ski_resorts.geojson"
    meta_path = out_dir / "overview_meta.json"
    if not admin_path.is_file() or not resorts_path.is_file():
        raise FileNotFoundError(f"Missing GeoJSON in {out_dir} (run regional_overview first)")

    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
    slug = out_dir.name
    out_qgz = out_dir / f"{slug}_overview_map.qgz"

    proj_admin = out_dir / "admin_boundary_proj.geojson"
    proj_resorts = out_dir / "ski_resorts_proj.geojson"
    map_crs = meta.get("crs")
    vectors_ready = (
        map_crs
        and map_crs.upper() not in ("EPSG:4326", "OGC:CRS84")
        and proj_admin.is_file()
        and proj_resorts.is_file()
    )

    if vectors_ready:
        admin_path, resorts_path = proj_admin, proj_resorts
        print(f"  Map CRS: {map_crs} (pre-projected)", flush=True)
    else:
        # geopandas hangs under QGIS' Python on Windows; use the venv for vector prep.
        import subprocess

        venv_py = REPO_ROOT / ".venv/Scripts/python.exe"
        if not venv_py.is_file():
            raise RuntimeError(".venv required to project overview vectors before QGIS build")
        subprocess.run(
            [
                str(venv_py),
                "-c",
                (
                    "import json; from pathlib import Path; "
                    "import geopandas as gpd; "
                    "from atlas.map_gen.overview_crs import overview_projected_crs; "
                    "from atlas.map_gen.overview_reproject import reproject_overview_vectors; "
                    f"out_dir = Path({str(out_dir)!r}); "
                    "meta = json.loads((out_dir / 'overview_meta.json').read_text(encoding='utf-8')); "
                    "b = gpd.read_file(out_dir / 'admin_boundary.geojson'); "
                    "crs = overview_projected_crs(meta, b.set_crs('EPSG:4326') if b.crs is None else b); "
                    "reproject_overview_vectors(out_dir, meta, crs); "
                    "print(crs)"
                ),
            ],
            cwd=str(REPO_ROOT),
            check=True,
            env=_venv_subprocess_env(),
        )
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        map_crs = meta.get("crs")
        if not map_crs or not proj_admin.is_file() or not proj_resorts.is_file():
            raise RuntimeError(f"Vector reprojection failed in {out_dir}")
        admin_path, resorts_path = proj_admin, proj_resorts
        print(f"  Map CRS: {map_crs}", flush=True)

    dem_paths = None if skip_dem else _ensure_dem_layers(out_dir, map_crs, meta)
    stale = out_dir / "dem_hillshade_proj.tif"
    if stale.is_file():
        stale.unlink(missing_ok=True)

    ensure_headless_qgis_initialized(qgis_root)
    try:
        from qgis.PyQt.QtCore import Qt
        from qgis.PyQt.QtGui import QColor
        from qgis.core import Qgis

        from qgis.core import (
            QgsCoordinateReferenceSystem,
            QgsCategorizedSymbolRenderer,
            QgsFillSymbol,
            QgsLabelObstacleSettings,
            QgsLayoutItemMap,
            QgsLayoutItemPage,
            QgsLayoutPoint,
            QgsLayoutSize,
            QgsMarkerSymbol,
            QgsPalLayerSettings,
            QgsPrintLayout,
            QgsRuleBasedLabeling,
            QgsLineSymbol,
            QgsSimpleFillSymbolLayer,
            QgsSimpleLineCallout,
            QgsSvgMarkerSymbolLayer,
            QgsProject,
            QgsRectangle,
            QgsRendererCategory,
            QgsSingleSymbolRenderer,
            QgsSymbol,
            QgsTextFormat,
            QgsUnitTypes,
            QgsVectorLayer,
            QgsVectorLayerSimpleLabeling,
        )

        project = QgsProject.instance()
        project.clear()
        project.setCrs(QgsCoordinateReferenceSystem(map_crs))

        def _add_vector(path: Path, name: str) -> QgsVectorLayer:
            uri = str(path.resolve()).replace("\\", "/")
            layer = QgsVectorLayer(f"{uri}|layername={path.stem}", name, "ogr")
            if not layer.isValid():
                layer = QgsVectorLayer(uri, name, "ogr")
            if not layer.isValid():
                raise RuntimeError(f"Could not load layer {name} from {path}")
            project.addMapLayer(layer)
            return layer

        def _apply_raster_style(
            layer,
            *,
            opacity: float | None = None,
            blend: Qgis.BlendMode | None = None,
            rgba_alpha_band: int | None = None,
        ) -> None:
            if rgba_alpha_band is not None:
                from qgis.core import QgsMultiBandColorRenderer

                renderer = QgsMultiBandColorRenderer(layer.dataProvider(), 1, 2, 3)
                renderer.setAlphaBand(rgba_alpha_band)
                layer.setRenderer(renderer)
            if opacity is not None and opacity < 1.0:
                layer.setOpacity(opacity)
            if blend is not None:
                layer.setBlendMode(blend)

        dem_color_layer = None
        dem_overlay_layer = None
        dem_mist_layer = None
        if dem_paths is not None:
            from qgis.core import QgsRasterLayer

            color_path, overlay_path, mist_path = dem_paths
            if color_path == overlay_path and color_path.is_file():
                uri = str(color_path.resolve()).replace("\\", "/")
                dem_color_layer = QgsRasterLayer(uri, "dem_hillshade")
                if dem_color_layer.isValid():
                    project.addMapLayer(dem_color_layer)
                else:
                    dem_color_layer = None
                    print(f"Warning: could not load {color_path}", file=sys.stderr)
            else:
                layer_specs = [
                    (color_path, "dem_color", "dem_color_layer", None, None, None),
                    (
                        overlay_path,
                        "dem_hillshade",
                        "dem_overlay_layer",
                        HILLSHADE_OVERLAY_OPACITY,
                        Qgis.BlendMode.Multiply,
                        None,
                    ),
                ]
                if mist_path is not None and mist_path.is_file():
                    layer_specs.append(
                        (
                            mist_path,
                            "dem_mist",
                            "dem_mist_layer",
                            1.0,
                            Qgis.BlendMode.Lighten,
                            4,
                        )
                    )
                for path, name, store, opacity, blend, alpha_band in layer_specs:
                    if not path.is_file():
                        continue
                    uri = str(path.resolve()).replace("\\", "/")
                    layer = QgsRasterLayer(uri, name)
                    if layer.isValid():
                        project.addMapLayer(layer)
                        _apply_raster_style(
                            layer,
                            opacity=opacity,
                            blend=blend,
                            rgba_alpha_band=alpha_band,
                        )
                        if store == "dem_color_layer":
                            dem_color_layer = layer
                        elif store == "dem_overlay_layer":
                            dem_overlay_layer = layer
                        else:
                            dem_mist_layer = layer
                    else:
                        print(f"Warning: could not load {path}", file=sys.stderr)

        boundary = _add_vector(admin_path, "admin_boundary")
        resorts = _add_vector(resorts_path, "ski_resorts")

        # Boundary: no fill, visible outline (createSimple "no" is unreliable in saved projects)
        fill_layer = QgsSimpleFillSymbolLayer()
        fill_layer.setBrushStyle(Qt.BrushStyle.NoBrush)
        fill_layer.setStrokeColor(QColor(OUTLINE_COLOR))
        fill_layer.setStrokeWidth(BOUNDARY_OUTLINE_MM)
        fill_layer.setStrokeWidthUnit(QgsUnitTypes.RenderUnit.Millimeters)
        fill_sym = QgsFillSymbol()
        # QgsFillSymbol() starts with a default fill layer; remove it so we don't
        # accidentally render a solid fill under our NoBrush layer.
        try:
            fill_sym.deleteSymbolLayer(0)
        except Exception:
            pass
        fill_sym.appendSymbolLayer(fill_layer)
        boundary.setRenderer(QgsSingleSymbolRenderer(fill_sym))

        # Register boundary as a labeling obstacle (state outline) without drawing labels.
        b_lbl = QgsPalLayerSettings()
        b_lbl.enabled = True
        b_lbl.drawLabels = False
        b_obs = b_lbl.obstacleSettings()
        b_obs.setIsObstacle(True)
        b_obs.setType(QgsLabelObstacleSettings.ObstacleType.PolygonBoundary)
        b_obs.setFactor(3.0)
        boundary.setLabeling(QgsVectorLayerSimpleLabeling(b_lbl))
        boundary.setLabelsEnabled(True)

        # Copy tier icons beside the project so relative ./ paths resolve in QGIS and export.
        for icon_name in TIER_ICON.values():
            src = OVERVIEW_ICONS_DIR / icon_name
            if not src.is_file():
                raise FileNotFoundError(f"Missing overview icon: {src}")
            shutil.copy2(src, out_dir / icon_name)

        def _tier_marker(tier: str) -> QgsMarkerSymbol:
            mm = TIER_MM[tier]
            icon_name = TIER_ICON[tier]
            icon_path = str((out_dir / icon_name).resolve()).replace("\\", "/")
            sl = QgsSvgMarkerSymbolLayer(icon_path, mm)
            sl.setSizeUnit(QgsUnitTypes.RenderUnit.Millimeters)
            sl.setStrokeWidth(0.25)
            sl.setStrokeColor(QColor(OUTLINE_COLOR))
            if hasattr(sl, "setIsObstacle"):
                sl.setIsObstacle(True)
                sl.setObstacleFactor(2.5)
            # QgsMarkerSymbol() often comes with a default simple marker layer (can show as
            # a red dot underneath). Replace the default layer with our SVG.
            sym = QgsMarkerSymbol.createSimple({})
            try:
                sym.changeSymbolLayer(0, sl)
            except Exception:
                while sym.symbolLayerCount() > 0:
                    sym.deleteSymbolLayer(0)
                sym.appendSymbolLayer(sl)
            return sym

        categories = [
            QgsRendererCategory(tier, _tier_marker(tier), tier) for tier in TIER_MM
        ]
        resorts.setRenderer(QgsCategorizedSymbolRenderer("map_tier", categories))

        def _resort_label_settings(tier: str) -> QgsPalLayerSettings:
            size_pt, color_hex, priority, callout_mm = TIER_LABEL[tier]
            pal = QgsPalLayerSettings()
            pal.fieldName = "name"
            pal.enabled = True
            fmt = QgsTextFormat()
            fmt.setSize(size_pt)
            fmt.setSizeUnit(QgsUnitTypes.RenderUnit.Points)
            fmt.setColor(QColor(color_hex))
            if tier == "mega":
                fmt.setNamedStyle("Bold")
            pal.setFormat(fmt)
            pal.placement = QgsPalLayerSettings.Placement.OrderedPositionsAroundPoint
            pal.dist = 2.5
            pal.distUnits = QgsUnitTypes.RenderUnit.Millimeters
            if hasattr(QgsPalLayerSettings, "OffsetType"):
                pal.offsetType = QgsPalLayerSettings.OffsetType.FromSymbolBounds
            else:
                pal.dist = 7.0
            pal.priority = priority
            pal.fitInPolygonOnly = True
            pal.overlapHandling = Qgis.LabelOverlapHandling.PreventOverlap

            callout = QgsSimpleLineCallout()
            callout.setEnabled(True)
            line_sym = QgsLineSymbol.createSimple(
                {
                    "color": color_hex if tier == "small" else OUTLINE_COLOR,
                    "width": str(callout_mm),
                    "width_unit": "MM",
                }
            )
            callout.setLineSymbol(line_sym)
            callout.setMinimumLength(2.5)
            pal.setCallout(callout)
            return pal

        # Rule-based labels: small resorts are lighter/smaller and lose placement priority.
        root_pal = QgsPalLayerSettings()
        root_pal.drawLabels = False
        root_rule = QgsRuleBasedLabeling.Rule(root_pal)
        root_rule.setDescription("ski_resorts")
        for tier in TIER_MM:
            rule = QgsRuleBasedLabeling.Rule(_resort_label_settings(tier))
            rule.setFilterExpression(f'"map_tier" = \'{tier}\'')
            rule.setDescription(tier)
            root_rule.appendChild(rule)

        if not skip_labels:
            resorts.setLabeling(QgsRuleBasedLabeling(root_rule))
            resorts.setLabelsEnabled(True)

        eng = project.labelingEngineSettings()
        eng.setFlag(Qgis.LabelingFlag.UsePartialCandidates, True)

        def _finite_rect(r) -> bool:
            return all(
                math.isfinite(v)
                for v in (r.xMinimum(), r.yMinimum(), r.xMaximum(), r.yMaximum())
            )

        # Some layers can report NaN extents until extents are recalculated.
        boundary.updateExtents()
        resorts.updateExtents()
        if dem_color_layer is not None:
            dem_color_layer.dataProvider().extent()

        ext = boundary.extent()
        if ext.isEmpty() or not _finite_rect(ext):
            ext = resorts.extent()
        if ext.isEmpty() or not _finite_rect(ext):
            # fall back to union of all layer extents
            ext = boundary.extent()
            for lyr in (resorts,):
                e2 = lyr.extent()
                if not e2.isEmpty() and _finite_rect(e2):
                    ext.combineExtentWith(e2)
        if not _finite_rect(ext):
            raise RuntimeError("Could not compute finite map extent for main_map")

        pad = max(ext.width(), ext.height()) * 0.06
        ext.grow(pad)

        from atlas.map_gen.layout_constants import expand_bounds_to_main_map_aspect

        b = expand_bounds_to_main_map_aspect(
            (ext.xMinimum(), ext.yMinimum(), ext.xMaximum(), ext.yMaximum()),
            frame_width_mm=PAGE_W_MM,
            frame_height_mm=PAGE_H_MM,
        )
        ext = QgsRectangle(b[0], b[1], b[2], b[3])

        # Recreate the layout from scratch (older/corrupt layouts can hang QGIS' preview renderer).
        lm = project.layoutManager()
        existing = lm.layoutByName("Regional Overview")
        if existing is not None:
            lm.removeLayout(existing)

        layout = QgsPrintLayout(project)
        layout.initializeDefaults()
        layout.setName("Regional Overview")
        lm.addLayout(layout)

        # Ensure exactly one page, sized to half-page landscape.
        pages = layout.pageCollection()
        while pages.pageCount() > 1:
            pages.deletePage(pages.pageCount() - 1)
        pages.page(0).setPageSize(
            QgsLayoutSize(PAGE_W_MM, PAGE_H_MM, QgsUnitTypes.LayoutUnit.Millimeters)
        )

        map_layers = [boundary, resorts]
        dem_stack = [
            lyr
            for lyr in (dem_color_layer, dem_overlay_layer, dem_mist_layer)
            if lyr is not None
        ]
        if dem_stack:
            map_layers = dem_stack + map_layers

        # Map item — full half-page (210 x 148.5 mm)
        map_item = QgsLayoutItemMap(layout)
        map_item.setId("main_map")
        # Let the map use the project's current layer tree visibility.
        # This matches QGIS' default behavior and avoids layouts that can get stuck rendering.
        map_item.setKeepLayerSet(False)
        map_item.setBackgroundEnabled(False)
        map_item.setFrameEnabled(False)
        layout.addLayoutItem(map_item)
        map_item.attemptResize(
            QgsLayoutSize(PAGE_W_MM, PAGE_H_MM, QgsUnitTypes.LayoutUnit.Millimeters)
        )
        map_item.attemptMove(
            QgsLayoutPoint(0, 0, QgsUnitTypes.LayoutUnit.Millimeters)
        )
        map_item.setCrs(QgsCoordinateReferenceSystem(map_crs))
        map_item.setExtent(ext)

        # Save project (.qgz) and enforce layout dimensions in XML
        project.write(str(out_qgz))
        _patch_overview_qgz_xml(out_qgz)
    finally:
        shutdown_headless_qgis_if_initialized()

    return out_qgz


def main() -> int:
    ap = argparse.ArgumentParser(description="Build overview QGZ from GeoJSON folder")
    ap.add_argument("--dir", type=Path, required=True, help="Overview unit folder")
    ap.add_argument("--qgis-root", type=Path, default=None)
    ap.add_argument(
        "--no-dem",
        action="store_true",
        help="Skip DEM fetch/build (vectors + layout only)",
    )
    ap.add_argument(
        "--no-labels",
        action="store_true",
        help="Skip resort name labels (faster for country-scale maps)",
    )
    args = ap.parse_args()
    out_dir = args.dir
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    try:
        path = _build(
            out_dir,
            args.qgis_root,
            skip_dem=args.no_dem,
            skip_labels=args.no_labels,
        )
        print(f"Wrote {path}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
