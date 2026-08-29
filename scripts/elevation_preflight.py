#!/usr/bin/env python3
"""OSM-only candidate list for elevation / later game export. Does not fetch Skadi tiles.

Default playable proxy: downhill ski resort, downhill_trails >= 1, longest trail >= 75 m.
Unnamed pistes are allowed (the playable client labels them).

Usage:
  python scripts/elevation_preflight.py --report-only
  python scripts/elevation_preflight.py --region north-america/us/virginia
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
DEFAULT_IN = REPO / "output" / "combined" / "ski_areas_analyzed.parquet"
DEFAULT_OUT = REPO / "config" / "resorts" / "_playable_candidates.json"
MIN_TRAIL_M_DEFAULT = 75.0
M_TO_MI = 1.0 / 1609.344


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def load_analyzed(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    for c in ("downhill_trails", "total_lifts", "longest_trail_mi", "skiable_terrain_ha"):
        if c in df.columns:
            df[c] = _num(df[c])
    df["winter_sports_id"] = df["winter_sports_id"].astype(str)
    df["region"] = df["region"].astype(str)
    return df


def apply_filters(
    df: pd.DataFrame,
    *,
    downhill_only: bool,
    min_trails: int,
    min_trail_m: float,
    region: str | None,
    state: str | None,
    drop_unknown_length: bool,
) -> pd.DataFrame:
    out = df
    if region:
        out = out[out["region"] == region]
    if state:
        st = out["state"].astype(str) if "state" in out.columns else pd.Series("", index=out.index)
        out = out[st.str.casefold() == state.casefold()]
    if downhill_only and "resort_type" in out.columns:
        rt = out["resort_type"].fillna("").astype(str).str.strip().str.casefold()
        out = out[rt == "downhill ski resort"]
    if min_trails > 0 and "downhill_trails" in out.columns:
        out = out[out["downhill_trails"].fillna(0) >= min_trails]
    if min_trail_m > 0 and "longest_trail_mi" in out.columns:
        min_mi = min_trail_m * M_TO_MI
        longest = out["longest_trail_mi"].fillna(0)
        long_enough = longest >= min_mi
        unknown = longest <= 0
        if drop_unknown_length:
            out = out[long_enough]
        else:
            # 0.00 is often area-mapped pistes the analyzer used to skip, not a 0-length hill
            out = out[long_enough | unknown]
    return out.copy()


def main() -> int:
    ap = argparse.ArgumentParser(description="List ski areas that should get DEMs / later game export")
    ap.add_argument("-i", "--input", type=Path, default=DEFAULT_IN)
    ap.add_argument("-o", "--output", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--all-winter-sports", action="store_true", help="Do not require downhill resort_type")
    ap.add_argument("--min-trails", type=int, default=1)
    ap.add_argument(
        "--min-trail-m",
        type=float,
        default=MIN_TRAIL_M_DEFAULT,
        help="Minimum longest_trail_mi converted from meters (0 = no length filter)",
    )
    ap.add_argument(
        "--drop-unknown-length",
        action="store_true",
        help="Exclude downhill areas whose longest_trail_mi is 0 (often a mapping bug; default is to keep them)",
    )
    ap.add_argument("--region", type=str, default=None, help="Exact region path, e.g. north-america/us/virginia")
    ap.add_argument("--state", type=str, default=None)
    args = ap.parse_args()

    if not args.input.exists():
        print(f"Missing {args.input}", flush=True)
        return 1

    df = load_analyzed(args.input)
    n_all = len(df)
    n_downhill = 0
    if "resort_type" in df.columns:
        n_downhill = int(
            (df["resort_type"].fillna("").astype(str).str.strip().str.casefold() == "downhill ski resort").sum()
        )
    n_trails = int((df["downhill_trails"].fillna(0) >= 1).sum()) if "downhill_trails" in df.columns else 0

    cand = apply_filters(
        df,
        downhill_only=not args.all_winter_sports,
        min_trails=args.min_trails,
        min_trail_m=args.min_trail_m,
        region=args.region,
        state=args.state,
        drop_unknown_length=args.drop_unknown_length,
    )

    print(f"analyzed rows: {n_all}")
    print(f"downhill ski resort: {n_downhill}")
    print(f"downhill_trails >= 1: {n_trails}")
    print(
        f"candidates (downhill_only={not args.all_winter_sports}, "
        f"min_trails={args.min_trails}, min_trail_m={args.min_trail_m}"
        f"{f', region={args.region}' if args.region else ''}"
        f"{f', state={args.state}' if args.state else ''}): {len(cand)}"
    )
    if cand.empty:
        print("No candidates.")
        if args.report_only:
            return 0
        return 1

    rows = []
    for _, r in cand.iterrows():
        rows.append(
            {
                "winter_sports_id": str(r["winter_sports_id"]),
                "region": str(r.get("region") or ""),
                "name": str(r.get("english_name") or r.get("name") or ""),
                "state": str(r.get("state") or ""),
                "country": str(r.get("country") or ""),
                "downhill_trails": float(r["downhill_trails"]) if pd.notna(r.get("downhill_trails")) else None,
                "total_lifts": float(r["total_lifts"]) if pd.notna(r.get("total_lifts")) else None,
                "longest_trail_mi": float(r["longest_trail_mi"]) if pd.notna(r.get("longest_trail_mi")) else None,
            }
        )
    if len(rows) <= 30:
        for row in rows:
            line = f"  {row['region']}  {row['winter_sports_id']}  {row['name']}"
            print(line.encode("ascii", "replace").decode("ascii"))
    else:
        print(f"  (listing skipped; {len(rows)} candidates in output JSON)")

    payload = {
        "built_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": str(args.input).replace("\\", "/"),
        "filter": {
            "downhill_only": not args.all_winter_sports,
            "min_trails": args.min_trails,
            "min_trail_m": args.min_trail_m,
            "region": args.region,
            "state": args.state,
            "unnamed_trails_ok": True,
        },
        "counts": {
            "analyzed": n_all,
            "downhill_ski_resort": n_downhill,
            "downhill_trails_ge_1": n_trails,
            "candidates": len(rows),
        },
        "candidates": rows,
    }
    if args.report_only:
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
