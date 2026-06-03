"""Regional ski facts for book chapters (mirrors GlobalSkiAtlas_2 Ski*Facts pages)."""

from __future__ import annotations

import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from atlas.book_gen.constants import RESORT_CATEGORY_LABEL
from atlas.book_gen.render_resort_fields import (
    format_area_acres,
    format_distance_mi,
)
from atlas.book_gen.log_util import log
from atlas.book_gen.resort_category import is_not_downhill, resort_size_category


@dataclass(frozen=True)
class FactRecord:
    label: str
    name: str
    detail: str


@dataclass
class RegionalFacts:
    region_title: str
    resort_count: int
    total_trails: int
    total_lifts: int
    total_acres: float
    tier_counts: dict[str, int] = field(default_factory=dict)
    resort_records: list[FactRecord] = field(default_factory=list)
    trail_lift_records: list[FactRecord] = field(default_factory=list)
    lift_type_summary: str = ""
    book_resort_count: int | None = None
    chart_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["resort_records"] = [asdict(r) for r in self.resort_records]
        d["trail_lift_records"] = [asdict(r) for r in self.trail_lift_records]
        return d


def _name_col(df: pd.DataFrame) -> str:
    for c in ("name", "Ski Area", "title", "area_name"):
        if c in df.columns:
            return c
    return df.columns[0]


def _state_col(df: pd.DataFrame) -> str | None:
    for c in ("state", "State", "addr:state", "province"):
        if c in df.columns:
            return c
    return None


def _num_series(df: pd.DataFrame, *cols: str) -> pd.Series:
    for c in cols:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series([float("nan")] * len(df), index=df.index)


def _filter_region(df: pd.DataFrame, region_filter: str | None) -> pd.DataFrame:
    if not region_filter or "region" not in df.columns:
        return df
    rn = region_filter.replace("\\", "/").strip("/")
    vals = df["region"].astype(str)
    return df.loc[(vals == rn) | vals.str.startswith(rn + "/")]


def _row_at_idx(df: pd.DataFrame, idx: Any, name_col: str) -> tuple[str, pd.Series]:
    row = df.loc[idx]
    return str(row[name_col]).strip(), row


def _aggregate_lift_types(df: pd.DataFrame) -> str:
    if "lift_types" not in df.columns:
        return ""
    counts: dict[str, int] = {}
    pat = re.compile(r"([^:,]+):\s*(\d+)")
    for raw in df["lift_types"].dropna().astype(str):
        for m in pat.finditer(raw):
            kind = m.group(1).strip().lower()
            counts[kind] = counts.get(kind, 0) + int(m.group(2))
    if not counts:
        return ""
    parts = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return ", ".join(f"{k} ({v:,})" for k, v in parts[:6])


def load_state_resorts_df(
    parquet_path: Path,
    *,
    state: str,
    region_filter: str | None = None,
) -> pd.DataFrame:
    if not parquet_path.is_file():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    df = _filter_region(df, region_filter)
    state_col = _state_col(df)
    if state and state_col:
        sf = state.strip().casefold()
        df = df[df[state_col].astype(str).str.strip().str.casefold() == sf]
    if "resort_type" in df.columns:
        mask = df.apply(
            lambda r: not is_not_downhill(r.to_dict()),
            axis=1,
        )
        df = df.loc[mask]
    df = df.reset_index(drop=True)

    # Elevation enrichment (optional): join `ski_areas_elevation.parquet` if present.
    # This lets facts/charts render elevation ranges even if analyzed parquet lacks them.
    try:
        # Prefer region-specific elevation outputs when we know the region.
        # `output/<continent>/<slug>/ski_areas_elevation.parquet` has the most reliable values.
        elev_path = parquet_path.parent / "ski_areas_elevation.parquet"  # default: output/combined/
        overwrite_with_elev = False
        if region_filter:
            try:
                # parquet_path: output/combined/ski_areas_analyzed.parquet -> output/
                output_root = parquet_path.parent.parent
                region_dir = output_root / region_filter.replace("\\", "/").strip("/")
                candidate = region_dir / "ski_areas_elevation.parquet"
                if candidate.is_file():
                    elev_path = candidate
                    overwrite_with_elev = True
            except Exception:
                pass
        if elev_path.is_file() and "region" in df.columns:
            elev = pd.read_parquet(elev_path)
            keep_cols = [
                c
                for c in (
                    "winter_sports_id",
                    "region",
                    "elevation_low_m",
                    "elevation_high_m",
                    "elevation_source",
                    "ski_north_angle",
                    "name",
                )
                if c in elev.columns
            ]
            elev = elev[keep_cols].copy()
            elev["region"] = elev["region"].astype(str).str.replace("\\\\", "/", regex=False).str.strip("/")
            df["region"] = df["region"].astype(str).str.replace("\\\\", "/", regex=False).str.strip("/")

            # Some per-region elevation outputs use empty `region` values; treat as unknown and
            # avoid joining on region in that case (name-based join is still reliable for state pages).
            elev_region_blank = False
            if "region" in elev.columns:
                r = elev["region"].fillna("").astype(str).str.strip()
                elev_region_blank = bool(len(r)) and bool((r == "").all())

            # If we have a region-specific elevation file, prefer a simple name lookup
            # (those files often have blank region + different IDs than combined analyzed parquet).
            if overwrite_with_elev and "name" in df.columns and "name" in elev.columns:
                try:
                    g = elev.groupby(elev["name"].astype(str), dropna=False)[
                        ["elevation_low_m", "elevation_high_m", "elevation_source", "ski_north_angle"]
                    ].first()
                    for col in ("elevation_low_m", "elevation_high_m", "elevation_source", "ski_north_angle"):
                        if col in g.columns:
                            df[col] = df["name"].astype(str).map(g[col].to_dict())
                    return df
                except Exception:
                    # fall back to merge logic below
                    pass

            def _fill_from_merge(suffix: str) -> None:
                for c in (
                    "elevation_low_m",
                    "elevation_high_m",
                    "elevation_source",
                    "ski_north_angle",
                ):
                    ec = c + suffix
                    if ec in df.columns:
                        if c in df.columns and not overwrite_with_elev:
                            df[c] = df[c].where(df[c].notna(), df[ec])
                        else:
                            # When using a region-specific elevation file, trust it and overwrite.
                            df[c] = df[ec]
                        df.drop(columns=[ec], inplace=True)

            # Primary join: winter_sports_id + region (when IDs line up).
            matched = 0
            if "winter_sports_id" in df.columns and "winter_sports_id" in elev.columns:
                elev["winter_sports_id"] = elev["winter_sports_id"].astype(str)
                df["winter_sports_id"] = df["winter_sports_id"].astype(str)
                on_cols = ["winter_sports_id"] if elev_region_blank else ["winter_sports_id", "region"]
                df = df.merge(elev, on=on_cols, how="left", suffixes=("", "_elev"))
                if "elevation_low_m_elev" in df.columns:
                    matched = int(pd.to_numeric(df["elevation_low_m_elev"], errors="coerce").notna().sum())
                _fill_from_merge("_elev")

            # Fallback join: name + region (works when analyzed winter_sports_id differs).
            if matched == 0 and "name" in df.columns and "name" in elev.columns:
                if elev_region_blank:
                    df = df.merge(
                        elev.rename(columns={"name": "name_elev"}),
                        left_on=["name"],
                        right_on=["name_elev"],
                        how="left",
                        suffixes=("", "_elev2"),
                    )
                else:
                    df = df.merge(
                        elev.rename(columns={"name": "name_elev"}),
                        left_on=["name", "region"],
                        right_on=["name_elev", "region"],
                        how="left",
                        suffixes=("", "_elev2"),
                    )
                if "name_elev" in df.columns:
                    df.drop(columns=["name_elev"], inplace=True)
                _fill_from_merge("_elev2")
    except Exception:
        # Never fail chapter build because elevation join is missing/broken.
        pass

    return df


def compute_regional_facts(
    df: pd.DataFrame,
    *,
    region_title: str,
    book_resort_count: int | None = None,
) -> RegionalFacts:
    name_col = _name_col(df)
    acres = _num_series(
        df,
        "skiable_terrain_acres",
        "skiableTerrainAcres",
        "total_area_acres",
    )
    trails = _num_series(df, "downhill_trails", "downhillTrails")
    lifts = _num_series(df, "total_lifts", "totalLifts")
    lat = _num_series(df, "centroid_lat", "latitude", "lat")
    longest_trail = _num_series(df, "longest_trail_mi", "longestTrailMi")
    longest_lift = _num_series(df, "longest_lift_mi", "longestLiftMi")

    tier_counts: dict[str, int] = {}
    for _, row in df.iterrows():
        cat = resort_size_category(row.to_dict())
        if cat == "unknown":
            continue
        tier_counts[cat] = tier_counts.get(cat, 0) + 1

    facts = RegionalFacts(
        region_title=region_title,
        resort_count=len(df),
        total_trails=int(trails.fillna(0).sum()),
        total_lifts=int(lifts.fillna(0).sum()),
        total_acres=float(acres.fillna(0).sum()),
        tier_counts=tier_counts,
        book_resort_count=book_resort_count,
        lift_type_summary=_aggregate_lift_types(df),
    )

    if df.empty:
        return facts

    def add_resort(label: str, idx: Any, detail_fn) -> None:
        name, row = _row_at_idx(df, idx, name_col)
        facts.resort_records.append(FactRecord(label=label, name=name, detail=detail_fn(row)))

    if acres.notna().any():
        idx = acres.idxmax()
        a = acres.loc[idx]
        add_resort(
            "Largest by skiable terrain",
            idx,
            lambda r: format_area_acres(float(a)),
        )

    small = df.loc[trails.notna() & (trails >= 1)]
    if not small.empty:
        idx = trails.loc[small.index].idxmin()
        t = trails.loc[idx]
        add_resort(
            "Smallest downhill resort",
            idx,
            lambda r: f"{int(t)} trail(s)",
        )
    elif acres.notna().any():
        pos = acres[acres > 0]
        if not pos.empty:
            idx = pos.idxmin()
            a = acres.loc[idx]
            add_resort(
                "Smallest downhill resort",
                idx,
                lambda r: format_area_acres(float(a)),
            )

    if trails.notna().any():
        idx = trails.idxmax()
        t = trails.loc[idx]
        add_resort(
            "Most trails",
            idx,
            lambda r: f"{int(t):,} trails",
        )

    if lifts.notna().any():
        idx = lifts.idxmax()
        lv = lifts.loc[idx]
        add_resort(
            "Most lifts",
            idx,
            lambda r: f"{int(lv):,} lifts",
        )

    if lat.notna().any():
        idx = lat.idxmax()
        la = lat.loc[idx]
        add_resort(
            "Most northern",
            idx,
            lambda r: f"{la:.1f}° latitude",
        )
        idx = lat.idxmin()
        la = lat.loc[idx]
        add_resort(
            "Most southern",
            idx,
            lambda r: f"{la:.1f}° latitude",
        )

    if longest_trail.notna().any():
        idx = longest_trail.idxmax()
        mi = longest_trail.loc[idx]
        name, _ = _row_at_idx(df, idx, name_col)
        facts.trail_lift_records.append(
            FactRecord(
                label="Longest named trail",
                name=name,
                detail=format_distance_mi(float(mi)),
            )
        )

    if longest_lift.notna().any():
        idx = longest_lift.idxmax()
        mi = longest_lift.loc[idx]
        name, _ = _row_at_idx(df, idx, name_col)
        facts.trail_lift_records.append(
            FactRecord(
                label="Longest lift",
                name=name,
                detail=format_distance_mi(float(mi)),
            )
        )

    return facts


def compute_regional_facts_from_parquet(
    parquet_path: Path,
    *,
    state: str,
    region_filter: str | None = None,
    book_resort_count: int | None = None,
    charts_dir: Path | None = None,
) -> RegionalFacts:
    df = load_state_resorts_df(
        parquet_path,
        state=state,
        region_filter=region_filter,
    )
    facts = compute_regional_facts(
        df,
        region_title=state.strip(),
        book_resort_count=book_resort_count,
    )
    if charts_dir is not None:
        try:
            from atlas.book_gen.regional_facts_charts import generate_regional_facts_charts

            facts.chart_paths = generate_regional_facts_charts(
                df,
                charts_dir,
                region_title=facts.region_title,
                tier_counts=facts.tier_counts,
            )
        except ImportError as exc:
            log(
                f"Regional facts charts skipped ({exc}). "
                "Install matplotlib for chart page: pip install matplotlib",
                file=sys.stderr,
            )
        except Exception as exc:
            log(f"Regional facts charts failed: {exc}", file=sys.stderr)
    _attach_elevation_chart_path(facts, charts_dir)
    return facts


def _attach_elevation_chart_path(
    facts: RegionalFacts, charts_dir: Path | None
) -> None:
    """Use existing chart_elevation_range.png when present (e.g. atlas_work/book/.../_facts_charts/)."""
    if charts_dir is None:
        return
    path = charts_dir / "chart_elevation_range.png"
    if path.is_file():
        facts.chart_paths["elevation_range"] = str(path.resolve()).replace("\\", "/")
