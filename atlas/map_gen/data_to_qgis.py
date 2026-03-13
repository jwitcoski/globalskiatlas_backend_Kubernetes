#!/usr/bin/env python3
"""
Data-to-map script: Parquet/GeoJSON → per-resort layers → QGIS project.

Reads from output/combined/ (or --input-dir): ski_areas.parquet, osm_near_winter_sports.parquet,
ski_area_contours.parquet. For each resort (winter_sports_id, region), writes clipped GeoJSON
layers and a copy of the QGIS template with datasources replaced. Optionally syncs to S3.

Usage:
  python -m atlas.map_gen.data_to_qgis --all-resorts --limit 2
  python -m atlas.map_gen.data_to_qgis --resort 12345 --region europe/iceland
  python -m atlas.map_gen.data_to_qgis --all-resorts --upload
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
import yaml

# Layer names that match QGIS Processing Model output (Phase 4 plan §1.1).
LAYER_SKI_BOUNDARY = "Ski Resort Boundary"
LAYER_OSM_POLYGONS = "OSM Polygons Clipped"
LAYER_OSM_LINES = "OSM Lines Clipped"
LAYER_OSM_POINTS = "OSM Points Clipped"
LAYER_CONTOURS = "Clipped Contours"

# Filenames written per resort (relative to resort dir).
FILE_SKI_AREA = "ski_area.geojson"
FILE_OSM_POLYGONS = "osm_polygons.geojson"
FILE_OSM_LINES = "osm_lines.geojson"
FILE_OSM_POINTS = "osm_points.geojson"
FILE_CONTOURS = "contours.geojson"

# Map layer name -> filename for datasource replacement.
LAYER_TO_FILE = {
    LAYER_SKI_BOUNDARY: FILE_SKI_AREA,
    LAYER_OSM_POLYGONS: FILE_OSM_POLYGONS,
    LAYER_OSM_LINES: FILE_OSM_LINES,
    LAYER_OSM_POINTS: FILE_OSM_POINTS,
    LAYER_CONTOURS: FILE_CONTOURS,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    if config_path is None:
        config_path = _repo_root() / "config" / "atlas.yaml"
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _ensure_crs(gdf: gpd.GeoDataFrame, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    if gdf.crs is None:
        gdf = gdf.set_crs(crs)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(crs)
    return gdf


def _normalize_key(ws_id: Any, region: Any) -> tuple[str, str]:
    ws = str(int(ws_id)) if ws_id is not None and pd.notna(ws_id) else ""
    reg = str(region).strip() if region is not None and pd.notna(region) else ""
    return ws, reg


def get_resort_list(
    ski_areas_path: Path,
    limit: Optional[int] = None,
    resort_id: Optional[str] = None,
    region_filter: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Return list of (winter_sports_id, region) from ski_areas.parquet."""
    if not ski_areas_path.exists():
        return []
    gdf = gpd.read_parquet(ski_areas_path)
    # Prefer winter_sports_id; fall back to osm_way_id or osm_id for compatibility
    if "winter_sports_id" not in gdf.columns:
        if "osm_way_id" in gdf.columns:
            gdf["winter_sports_id"] = gdf["osm_way_id"]
        elif "osm_id" in gdf.columns:
            gdf["winter_sports_id"] = gdf["osm_id"]
        else:
            return []
    if "region" not in gdf.columns:
        gdf["region"] = ""
    keys: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for _, row in gdf.iterrows():
        ws, reg = _normalize_key(row["winter_sports_id"], row.get("region"))
        if not ws:
            continue
        if (ws, reg) in seen:
            continue
        seen.add((ws, reg))
        if resort_id is not None and ws != resort_id:
            continue
        if region_filter is not None and reg != region_filter:
            continue
        keys.append((ws, reg))
    if limit is not None:
        keys = keys[:limit]
    return keys


def clip_to_resort(
    gdf: gpd.GeoDataFrame,
    resort_polygon: Any,
    buffer_degrees: float = 0.0,
) -> gpd.GeoDataFrame:
    """Clip gdf to resort polygon (optionally buffered). Returns new GeoDataFrame."""
    gdf = _ensure_crs(gdf.copy())
    if resort_polygon is None or resort_polygon.is_empty:
        return gdf.iloc[0:0]
    mask = gpd.GeoSeries([resort_polygon], crs=gdf.crs)
    if buffer_degrees > 0:
        mask = mask.to_crs("EPSG:4087")  # metres
        mask = mask.buffer(buffer_degrees * 111320)  # rough metres per degree
        mask = mask.to_crs(gdf.crs)
    return gpd.clip(gdf, mask)


def split_osm_by_geometry(osm_gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Split OSM GeoDataFrame into polygons, lines, points."""
    osm_gdf = _ensure_crs(osm_gdf)
    polys = osm_gdf[osm_gdf.geometry.type.isin(("Polygon", "MultiPolygon"))].copy()
    lines = osm_gdf[osm_gdf.geometry.type.isin(("LineString", "MultiLineString"))].copy()
    points = osm_gdf[osm_gdf.geometry.type == "Point"].copy()
    return polys, lines, points


def write_resort_layers(
    resort_dir: Path,
    ski_area_gdf: gpd.GeoDataFrame,
    osm_polygons: gpd.GeoDataFrame,
    osm_lines: gpd.GeoDataFrame,
    osm_points: gpd.GeoDataFrame,
    contours_gdf: gpd.GeoDataFrame,
    driver: str = "GeoJSON",
) -> None:
    """Write per-resort GeoJSON (or GeoPackage) layers."""
    resort_dir.mkdir(parents=True, exist_ok=True)
    if not ski_area_gdf.empty:
        path = resort_dir / FILE_SKI_AREA
        ski_area_gdf.to_file(path, driver=driver)
    for gdf, name in [
        (osm_polygons, FILE_OSM_POLYGONS),
        (osm_lines, FILE_OSM_LINES),
        (osm_points, FILE_OSM_POINTS),
    ]:
        if not gdf.empty:
            gdf.to_file(resort_dir / name, driver=driver)
    if not contours_gdf.empty:
        contours_gdf.to_file(resort_dir / FILE_CONTOURS, driver=driver)


def replace_qgis_datasources(qgs_content: str, resort_dir: Path) -> str:
    """Replace layer datasources in QGS XML so they point to files in resort_dir."""
    resort_dir = resort_dir.resolve()
    # Match <maplayer>...</maplayer> blocks and replace <datasource> inside by layer name.
    maplayer_re = re.compile(
        r"<maplayer[^>]*>.*?</maplayer>",
        re.DOTALL,
    )
    layername_re = re.compile(r"<layername>([^<]*)</layername>")
    datasource_re = re.compile(r"<datasource>[^<]*</datasource>")

    def replace_block(m: re.Match) -> str:
        block = m.group(0)
        layername_m = layername_re.search(block)
        if not layername_m:
            return block
        layer_name = layername_m.group(1).strip()
        filename = LAYER_TO_FILE.get(layer_name)
        if not filename:
            return block
        new_path = (resort_dir / filename).as_posix()
        new_datasource = f"<datasource>{new_path}</datasource>"
        block = datasource_re.sub(new_datasource, block, count=1)
        return block

    return maplayer_re.sub(replace_block, qgs_content)


def copy_template_to_resort(
    template_path: Path,
    resort_dir: Path,
    resort_name: str,
) -> bool:
    """
    Copy QGIS template (.qgz) to resort dir and replace layer datasources.
    .qgz is a zip containing project.qgs (and possibly other files).
    Returns True if template was found and copied.
    """
    if not template_path.exists():
        return False
    resort_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"resort_{resort_name}.qgz"
    out_path = resort_dir / out_name
    if template_path.suffix.lower() == ".qgz":
        with zipfile.ZipFile(template_path, "r") as zin:
            with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zout:
                for info in zin.infolist():
                    data = zin.read(info.filename)
                    if info.filename == "project.qgs" or info.filename.endswith("/project.qgs"):
                        data = replace_qgis_datasources(data.decode("utf-8"), resort_dir).encode("utf-8")
                    zout.writestr(info, data)
    else:
        # .qgs single file
        content = template_path.read_text(encoding="utf-8")
        content = replace_qgis_datasources(content, resort_dir)
        out_path = resort_dir / out_name.replace(".qgz", ".qgs")
        out_path.write_text(content, encoding="utf-8")
    return True


def process_resort(
    ws_id: str,
    region: str,
    ski_areas_gdf: gpd.GeoDataFrame,
    osm_gdf: gpd.GeoDataFrame,
    contours_gdf: gpd.GeoDataFrame,
    resort_dir: Path,
    template_path: Optional[Path],
    buffer_degrees: float,
    clip_osm: bool,
) -> bool:
    """Produce one resort's layers and optional QGIS project. Returns True on success."""
    # Resort polygon
    mask_ws = ski_areas_gdf["winter_sports_id"].astype(str) == ws_id
    mask_reg = ski_areas_gdf["region"].astype(str) == region
    resort_rows = ski_areas_gdf[mask_ws & mask_reg]
    if resort_rows.empty:
        return False
    resort_geom = resort_rows.geometry.unary_union
    if resort_geom is None or resort_geom.is_empty:
        return False
    ski_area_one = resort_rows[["geometry"]].copy()
    if "winter_sports_id" in resort_rows.columns:
        ski_area_one["winter_sports_id"] = resort_rows["winter_sports_id"].iloc[0]
    if "region" in resort_rows.columns:
        ski_area_one["region"] = resort_rows["region"].iloc[0]
    ski_area_one = _ensure_crs(ski_area_one)

    # OSM: filter by resort, optionally clip, split by geometry
    osm_mask_ws = osm_gdf["winter_sports_id"].astype(str) == ws_id
    osm_mask_reg = osm_gdf["region"].astype(str) == region
    osm_resort = osm_gdf[osm_mask_ws & osm_mask_reg].copy()
    if clip_osm and not osm_resort.empty:
        osm_resort = clip_to_resort(osm_resort, resort_geom, buffer_degrees)
    osm_polygons, osm_lines, osm_points = split_osm_by_geometry(osm_resort)

    # Contours: filter by resort only (no spatial clip)
    if not contours_gdf.empty and "winter_sports_id" in contours_gdf.columns:
        c_ws = contours_gdf["winter_sports_id"].astype(str) == ws_id
        c_reg = contours_gdf["region"].astype(str) == region
        contours_one = contours_gdf[c_ws & c_reg].copy()
        contours_one = _ensure_crs(contours_one)
    else:
        contours_one = contours_gdf.iloc[0:0].copy() if not contours_gdf.empty else gpd.GeoDataFrame()

    write_resort_layers(resort_dir, ski_area_one, osm_polygons, osm_lines, osm_points, contours_one)

    resort_name = f"{region.replace('/', '_')}_{ws_id}" if region else ws_id
    if template_path:
        copy_template_to_resort(template_path, resort_dir, resort_name)
    return True


def run_resorts(
    input_dir: Path,
    work_dir: Path,
    config: dict[str, Any],
    resort_id: Optional[str] = None,
    region_filter: Optional[str] = None,
    all_resorts: bool = True,
    limit: Optional[int] = None,
) -> int:
    """Generate per-resort layers (and QGIS projects) for trail maps. Returns count of resorts processed."""
    ski_areas_path = input_dir / "ski_areas.parquet"
    osm_path = input_dir / "osm_near_winter_sports.parquet"
    contours_parquet = input_dir / "ski_area_contours.parquet"
    contours_geojson = input_dir / "ski_area_contours.geojson"

    if not ski_areas_path.exists():
        print(f"Missing {ski_areas_path}", file=sys.stderr)
        return 0

    ski_areas_gdf = gpd.read_parquet(ski_areas_path)
    ski_areas_gdf = _ensure_crs(ski_areas_gdf)
    if "region" not in ski_areas_gdf.columns:
        ski_areas_gdf["region"] = ""
    if "winter_sports_id" not in ski_areas_gdf.columns:
        if "osm_way_id" in ski_areas_gdf.columns:
            ski_areas_gdf["winter_sports_id"] = ski_areas_gdf["osm_way_id"]
        elif "osm_id" in ski_areas_gdf.columns:
            ski_areas_gdf["winter_sports_id"] = ski_areas_gdf["osm_id"]

    resorts = get_resort_list(
        ski_areas_path,
        limit=limit,
        resort_id=resort_id,
        region_filter=region_filter,
    )
    if not resorts:
        print("No resorts to process.", file=sys.stderr)
        return 0

    osm_gdf = gpd.GeoDataFrame()
    if osm_path.exists():
        osm_gdf = gpd.read_parquet(osm_path)
        osm_gdf = _ensure_crs(osm_gdf)
        if "region" not in osm_gdf.columns:
            osm_gdf["region"] = ""

    contours_gdf = gpd.GeoDataFrame()
    if contours_parquet.exists():
        contours_gdf = gpd.read_parquet(contours_parquet)
    elif contours_geojson.exists():
        contours_gdf = gpd.read_file(contours_geojson)
    if not contours_gdf.empty:
        contours_gdf = _ensure_crs(contours_gdf)
        if "region" not in contours_gdf.columns:
            contours_gdf["region"] = ""

    template_cfg = config.get("template") or {}
    template_rel = template_cfg.get("resort")
    template_path: Optional[Path] = None
    if template_rel:
        template_path = _repo_root() / template_rel
    buffer_degrees = float(config.get("buffer_map_units") or 0.0)
    clip_osm = True

    work_resorts = work_dir / "resorts"
    count = 0
    for ws_id, region in resorts:
        # Resort dir: work/resorts/<region>/<ws_id>/ (region may contain /)
        parts = [p for p in region.split("/") if p] if region else ["_"]
        resort_dir = work_resorts.joinpath(*parts) / ws_id
        try:
            if process_resort(
                ws_id,
                region,
                ski_areas_gdf,
                osm_gdf,
                contours_gdf,
                resort_dir,
                template_path,
                buffer_degrees,
                clip_osm,
            ):
                count += 1
                print(f"  {region}/{ws_id}")
        except Exception as e:
            print(f"  Error {region}/{ws_id}: {e}", file=sys.stderr)
    return count


def upload_work_to_s3(work_dir: Path, config: dict[str, Any]) -> None:
    """Sync work_dir to S3 work bucket (aws s3 sync or boto3)."""
    s3_cfg = config.get("s3") or {}
    bucket = s3_cfg.get("work_bucket")
    prefix = (s3_cfg.get("work_prefix") or "work").strip("/")
    if not bucket:
        print("No s3.work_bucket in config; skip upload.", file=sys.stderr)
        return
    dest = f"s3://{bucket}/{prefix}/"
    work_dir = work_dir.resolve()
    if not work_dir.exists():
        print("Work dir does not exist; nothing to upload.", file=sys.stderr)
        return
    try:
        subprocess.run(
            ["aws", "s3", "sync", str(work_dir), dest, "--only-show-errors"],
            check=True,
        )
        print(f"Uploaded to {dest}")
    except FileNotFoundError:
        try:
            import boto3
            for root, _dirs, files in os.walk(work_dir):
                for f in files:
                    path = Path(root) / f
                    key = f"{prefix}/{path.relative_to(work_dir).as_posix()}".replace("\\", "/")
                    boto3.client("s3").upload_file(str(path), bucket, key)
            print(f"Uploaded to s3://{bucket}/{prefix}/")
        except ImportError:
            print("Install awscli or boto3 for S3 upload.", file=sys.stderr)
        except Exception as e:
            print(f"Upload failed: {e}", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Data-to-map: Parquet → per-resort layers + QGIS project",
    )
    parser.add_argument("--input-dir", type=Path, default=None, help="Combined parquet dir (default: from config or output/combined)")
    parser.add_argument("--work-dir", type=Path, default=None, help="Output work dir (default: from config or atlas_work)")
    parser.add_argument("--resort", type=str, default=None, help="Process single resort by winter_sports_id")
    parser.add_argument("--region", type=str, default=None, help="Region for --resort (e.g. europe/iceland)")
    parser.add_argument("--all-resorts", action="store_true", help="Process all resorts from combined data")
    parser.add_argument("--overview", type=str, choices=["country", "state"], default=None, help="Generate overview maps (not implemented in this script)")
    parser.add_argument("--limit", type=int, default=None, help="Max number of resorts (for testing)")
    parser.add_argument("--upload", action="store_true", help="After writing, sync work dir to S3 work bucket")
    args = parser.parse_args()

    root = _repo_root()
    config = load_config()
    input_dir = args.input_dir or Path(config.get("input_dir", "output/combined"))
    work_dir = args.work_dir or Path(config.get("work_dir", "atlas_work"))
    if not input_dir.is_absolute():
        input_dir = root / input_dir
    if not work_dir.is_absolute():
        work_dir = root / work_dir

    if not args.all_resorts and not args.resort:
        parser.print_help()
        print("\nUse --all-resorts or --resort <id> --region <region>", file=sys.stderr)
        return 1
    if args.resort and not args.region:
        print("When using --resort, specify --region.", file=sys.stderr)
        return 1
    if args.overview:
        print("Overview maps (country/state) are not implemented in this script yet.", file=sys.stderr)
        return 0

    count = run_resorts(
        input_dir,
        work_dir,
        config,
        resort_id=args.resort,
        region_filter=args.region,
        all_resorts=args.all_resorts,
        limit=args.limit,
    )
    print(f"Processed {count} resort(s). Output: {work_dir}/resorts/")
    if args.upload and count > 0:
        upload_work_to_s3(work_dir, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
