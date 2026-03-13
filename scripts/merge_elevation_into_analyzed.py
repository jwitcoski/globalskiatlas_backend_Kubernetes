#!/usr/bin/env python3
"""
Merge ski_areas_elevation.parquet into ski_areas_analyzed.parquet and ski_areas_analyzed.csv.

Use when elevation was computed but the merge was skipped (e.g. region mismatch), or to backfill
elevation_low_m, elevation_high_m, elevation_source, ski_north_angle into the analyzed table/CSV.

Usage:
  python scripts/merge_elevation_into_analyzed.py -d output/europe/iceland
  python scripts/merge_elevation_into_analyzed.py --data-dir output/europe/iceland
"""
import argparse
import sys
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser(description="Merge elevation parquet into analyzed parquet and CSV")
    p.add_argument("-d", "--data-dir", type=str, default=".", help="Directory containing ski_areas_analyzed.* and ski_areas_elevation.parquet")
    args = p.parse_args()

    data_dir = Path(args.data_dir)
    analyzed_parquet = data_dir / "ski_areas_analyzed.parquet"
    analyzed_csv = data_dir / "ski_areas_analyzed.csv"
    elev_parquet = data_dir / "ski_areas_elevation.parquet"

    if not elev_parquet.exists():
        print(f"Not found: {elev_parquet}", file=sys.stderr)
        sys.exit(1)
    if not analyzed_parquet.exists():
        print(f"Not found: {analyzed_parquet}", file=sys.stderr)
        sys.exit(1)

    elev_df = pd.read_parquet(elev_parquet)
    analyzed_df = pd.read_parquet(analyzed_parquet)

    merge_cols = ["elevation_low_m", "elevation_high_m", "elevation_source", "ski_north_angle"]
    elev_cols = [c for c in merge_cols if c in elev_df.columns]
    if not elev_cols:
        print("No elevation columns in elevation parquet", file=sys.stderr)
        sys.exit(1)

    if "winter_sports_id" not in analyzed_df.columns:
        print("ski_areas_analyzed.parquet has no winter_sports_id", file=sys.stderr)
        sys.exit(1)

    # Merge on winter_sports_id (works for single-region; elevation may have region="" while analyzed has "iceland")
    elev_merge = elev_df[["winter_sports_id"] + elev_cols].drop_duplicates(subset=["winter_sports_id"])
    if analyzed_df["winter_sports_id"].dtype != elev_merge["winter_sports_id"].dtype:
        elev_merge = elev_merge.copy()
        elev_merge["winter_sports_id"] = elev_merge["winter_sports_id"].astype(analyzed_df["winter_sports_id"].dtype)
    merged = analyzed_df.drop(columns=[c for c in elev_cols if c in analyzed_df.columns], errors="ignore")
    merged = merged.merge(elev_merge, on="winter_sports_id", how="left")

    # Derived: vertical drop (m and ft)
    merged["vertical_drop_m"] = merged.apply(
        lambda r: round(float(r["elevation_high_m"]) - float(r["elevation_low_m"]), 1)
        if pd.notna(r.get("elevation_high_m")) and pd.notna(r.get("elevation_low_m"))
        else None,
        axis=1,
    )
    merged["vertical_drop_ft"] = merged["vertical_drop_m"].apply(
        lambda x: round(x * 3.28084, 1) if pd.notna(x) and x is not None else None
    )

    merged.to_parquet(analyzed_parquet, index=False)
    print(f"Updated {analyzed_parquet} with {elev_cols}")

    merged.to_csv(analyzed_csv, index=False)
    print(f"Updated {analyzed_csv} with {elev_cols}")


if __name__ == "__main__":
    main()
