#!/usr/bin/env python3
"""
Register combined GeoParquet outputs as Iceberg tables (Glue catalog, S3).
Run after combine_regions.py. Each run overwrites the table with current data,
creating a new Iceberg snapshot so you get monthly versioning.

Requires: pip install -r requirements-iceberg.txt
AWS: set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (or profile); Glue + S3 permissions.

Usage:
  python scripts/register_iceberg.py --s3-bucket my-bucket --input-dir output/combined
  python scripts/register_iceberg.py --s3-bucket my-bucket --tables ski_areas_analyzed
  python scripts/register_iceberg.py --s3-bucket my-bucket --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Optional deps: pyiceberg, pyarrow, boto3
try:
    import pyarrow as pa
except ImportError as e:
    print("Install pyarrow: pip install pyarrow", file=sys.stderr)
    raise SystemExit(1) from e

DEFAULT_TABLES = ["ski_areas_analyzed", "ski_areas", "lifts", "pistes"]
TABULAR_TABLES = ["ski_areas_analyzed"]  # no geometry column
GEO_TABLES = ["ski_areas", "lifts", "pistes"]  # have geometry; we store as WKB binary


def _read_tabular_to_arrow(path: Path) -> pa.Table:
    import pandas as pd

    df = pd.read_parquet(path)
    return pa.Table.from_pandas(df, preserve_index=False)


def _read_geo_to_arrow(path: Path) -> pa.Table:
    import geopandas as gpd

    gdf = gpd.read_parquet(path)
    if "geometry" not in gdf.columns:
        return pa.Table.from_pandas(gdf, preserve_index=False)
    if gdf.geometry.isna().all():
        gdf = gdf.drop(columns=["geometry"])
        return pa.Table.from_pandas(gdf, preserve_index=False)
    # Store geometry as WKB binary for Iceberg (no native geometry type)
    gdf = gdf.copy()
    gdf["geometry_wkb"] = gdf.geometry.apply(lambda g: g.wkb if g is not None and not g.is_empty else None)
    gdf = gdf.drop(columns=["geometry"])
    return pa.Table.from_pandas(gdf, preserve_index=False)


def _fix_null_columns(pa_table: pa.Table) -> pa.Table:
    """Replace pa.null() columns with optional pa.string() so Iceberg format v2 accepts the schema."""
    new_fields = []
    columns = []
    for i in range(len(pa_table.schema)):
        field = pa_table.schema.field(i)
        if field.type == pa.null():
            new_fields.append(pa.field(field.name, pa.string(), nullable=True))
            columns.append(pa.array([None] * pa_table.num_rows, type=pa.string()))
        else:
            new_fields.append(field)
            columns.append(pa_table.column(i))
    return pa.Table.from_arrays(columns, schema=pa.schema(new_fields))


def _get_iceberg_schema(pa_schema: pa.Schema):
    """Return schema for create_table. PyIceberg 0.5+ accepts PyArrow schema directly."""
    return pa_schema


def _load_catalog(catalog_type: str, s3_bucket: str, region: str):
    if catalog_type == "glue":
        from pyiceberg.catalog import load_catalog
        # PyIceberg 0.8+ requires type=glue and glue.region (not uri for Glue)
        return load_catalog(
            name="glue",
            type="glue",
            warehouse=f"s3://{s3_bucket}/iceberg",
            **{"glue.region": region},
        )
    if catalog_type == "rest":
        from pyiceberg.catalog import load_catalog
        return load_catalog(
            "rest",
            **{
                "uri": "http://localhost:8181",
                "warehouse": f"s3://{s3_bucket}/iceberg",
            },
        )
    raise ValueError(f"Unknown catalog type: {catalog_type}")


def register_table(
    catalog,
    database: str,
    table_name: str,
    input_dir: Path,
    s3_bucket: str,
    is_geo: bool,
    dry_run: bool,
) -> bool:
    path = input_dir / f"{table_name}.parquet"
    if not path.exists():
        print(f"  Skip {table_name}: {path} not found")
        return False

    if is_geo:
        pa_table = _read_geo_to_arrow(path)
    else:
        pa_table = _read_tabular_to_arrow(path)

    if pa_table.num_rows == 0:
        print(f"  Skip {table_name}: empty")
        return False

    # Iceberg format v2 does not support pa.null(); replace with optional string
    pa_table = _fix_null_columns(pa_table)

    identifier = f"{database}.{table_name}"
    location = f"s3://{s3_bucket}/iceberg/{database}/{table_name}"

    if dry_run:
        print(f"  [dry-run] Would register {identifier} <- {path} ({pa_table.num_rows} rows) -> {location}")
        return True

    try:
        iceberg_schema = _get_iceberg_schema(pa_table.schema)
        if iceberg_schema is None:
            # Fallback: let PyIceberg infer from Arrow (some versions accept pa.Schema)
            iceberg_schema = pa_table.schema
    except Exception as e:
        print(f"  Schema conversion for {table_name}: {e}", file=sys.stderr)
        return False

    try:
        # Ensure Glue database exists
        catalog.create_namespace_if_not_exists(database)
        try:
            table = catalog.load_table(identifier)
        except Exception:
                table = catalog.create_table(
                identifier,
                iceberg_schema,
                location=location,
            )
        # Overwrite = new snapshot each run (monthly versioning)
        table.overwrite(pa_table, snapshot_properties={"source": "globalskiatlas_register_iceberg"})
        print(f"  {identifier}: overwrote {pa_table.num_rows} rows (new snapshot)")
        return True
    except Exception as e:
        print(f"  {table_name}: {e}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register combined GeoParquet as Iceberg tables (Glue + S3). Each run creates a new snapshot.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("output/combined"),
        help="Directory with combined parquet files (default: output/combined)",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        required=True,
        help="S3 bucket for Iceberg table data and metadata",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="ski_atlas",
        help="Glue database name (default: ski_atlas)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="us-east-1",
        help="AWS region for Glue (default: us-east-1)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="glue",
        choices=["glue", "rest"],
        help="Catalog type: glue (AWS Glue) or rest (default: glue)",
    )
    parser.add_argument(
        "--tables",
        type=str,
        nargs="*",
        default=DEFAULT_TABLES,
        help=f"Table names to register (default: {' '.join(DEFAULT_TABLES)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be done",
    )
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        print(f"Input dir not found: {args.input_dir}", file=sys.stderr)
        return 1

    if not args.dry_run:
        try:
            catalog = _load_catalog(args.catalog, args.s3_bucket, args.region)
        except Exception as e:
            print(f"Failed to load catalog: {e}", file=sys.stderr)
            return 1
    else:
        catalog = None

    ok = 0
    for name in args.tables:
        is_geo = name in GEO_TABLES
        if catalog is not None:
            if register_table(catalog, args.database, name, args.input_dir, args.s3_bucket, is_geo, False):
                ok += 1
        else:
            if register_table(None, args.database, name, args.input_dir, args.s3_bucket, is_geo, True):
                ok += 1

    print(f"Done: {ok}/{len(args.tables)} tables registered.")
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
