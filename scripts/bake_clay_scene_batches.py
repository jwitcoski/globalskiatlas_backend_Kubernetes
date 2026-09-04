#!/usr/bin/env python3
"""Export clay 3D scenes in batches (wiki 3D Map + homepage hero).

Picks a mix of large, medium, and small downhill resorts from analyzed OSM data,
exports mesh-only scenes under output/clay_scenes/, merges config/clay_scenes/catalog.json,
and optionally uploads to S3.

Designed to scale toward the full ~2k resort set in batches of 10.

  python scripts/bake_clay_scene_batches.py --pick-only
  python scripts/bake_clay_scene_batches.py --dry-run
  python scripts/bake_clay_scene_batches.py --upload
  python scripts/bake_clay_scene_batches.py --max-batches 0   # until queue empty
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from game_export.config import config_from_candidate, load_resort_config
from game_export.homepage import export_homepage_scene
from game_export.s3_inputs import default_s3_bucket

log = logging.getLogger("bake_clay")

ANALYZED = REPO / "output" / "combined" / "ski_areas_analyzed.parquet"
CANDIDATES = REPO / "config" / "resorts" / "_playable_candidates.json"
CATALOG = REPO / "config" / "clay_scenes" / "catalog.json"
PROGRESS = REPO / "config" / "clay_scenes" / "_progress.json"
SCENES_OUT = REPO / "output" / "clay_scenes"
GAME_CATALOG = REPO / "output" / "game_scenes" / "catalog.json"

DEFAULT_SEED = 20260903
SIZE_TIERS = ("large", "medium", "small")

# Atlantic seaboard + Appalachian east (excludes Midwest / Rockies / West Coast).
US_EAST_STATE_SLUGS = frozenset(
    {
        "maine",
        "new-hampshire",
        "vermont",
        "massachusetts",
        "rhode-island",
        "connecticut",
        "new-york",
        "new-jersey",
        "pennsylvania",
        "delaware",
        "maryland",
        "virginia",
        "west-virginia",
        "north-carolina",
        "south-carolina",
        "georgia",
        "florida",
        "tennessee",
        "alabama",
    }
)

CONTINENT_MAP = {
    "north-america": ("north_america", "North America"),
    "south-america": ("south_america", "South America"),
    "australia-oceania": ("oceania", "Oceania"),
    "europe": ("europe", "Europe"),
    "asia": ("asia", "Asia"),
    "africa": ("africa", "Africa"),
}


def _setup_logging() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def load_analyzed(path: Path = ANALYZED) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Missing analyzed parquet: {path}")
    df = pd.read_parquet(path)
    df["winter_sports_id"] = df["winter_sports_id"].astype(str).str.strip()
    df["region"] = df["region"].astype(str)
    df["country"] = df["country"].astype(str)
    df["name"] = df["name"].astype(str)
    for col in ("downhill_trails", "total_lifts", "skiable_terrain_ha", "longest_trail_mi"):
        if col in df.columns:
            df[col] = _num(df[col])
    if "resort_type" in df.columns:
        rt = df["resort_type"].fillna("").astype(str).str.strip().str.casefold()
        df = df[rt == "downhill ski resort"].copy()
    df = df[df["downhill_trails"] >= 1].copy()
    df = df[df["name"].str.len() > 2].copy()
    df = df[df["country"].astype(str).str.strip().astype(bool)].copy()
    df = df[~df["name"].str.fullmatch(r"\d+")].copy()
    bad = {"ski area", "skiing", "winter sports"}
    df = df[~df["name"].str.casefold().isin(bad)].copy()
    return df


def continent_from_region(region: str) -> tuple[str, str]:
    key = region.split("/")[0].strip().lower()
    return CONTINENT_MAP.get(key, (key.replace("-", "_"), key.replace("-", " ").title()))


def short_display_name(name: str, *, max_len: int = 24) -> str:
    s = re.sub(r"\s+", " ", name.strip())
    for cut in (" Ski Resort", " Ski Area", " Ski Centre", " Ski Center", " Ski Park"):
        if s.endswith(cut):
            s = s[: -len(cut)].strip()
    if len(s) <= max_len:
        return s
    parts = s.split()
    if len(parts) >= 2 and len(parts[0]) + len(parts[1]) + 1 <= max_len:
        return f"{parts[0]} {parts[1]}"
    return s[: max_len - 1].rstrip() + "…" if len(s) > max_len else s


def load_catalog_ids() -> set[str]:
    if not CATALOG.is_file():
        return set()
    payload = json.loads(CATALOG.read_text(encoding="utf-8"))
    ids = set()
    for row in payload.get("resorts") or []:
        wid = str(row.get("winter_sports_id") or "").strip()
        if wid:
            ids.add(wid)
    return ids


def load_progress(path: Path = PROGRESS) -> dict[str, Any]:
    if not path.is_file():
        return {"done": [], "failed": [], "batches": []}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {
        "done": [str(x) for x in raw.get("done") or []],
        "failed": list(raw.get("failed") or []),
        "batches": list(raw.get("batches") or []),
    }


def save_progress(progress: dict[str, Any], path: Path = PROGRESS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def blocked_ids(progress: dict[str, Any], *, include_failed: bool) -> set[str]:
    out = set(load_catalog_ids())
    out.update(progress.get("done") or [])
    if include_failed:
        for row in progress.get("failed") or []:
            if isinstance(row, dict):
                wid = str(row.get("winter_sports_id") or "").strip()
            else:
                wid = str(row).strip()
            if wid:
                out.add(wid)
    return out


def size_score(row: pd.Series) -> float:
    trails = float(row.get("downhill_trails") or 0)
    ha = float(row.get("skiable_terrain_ha") or 0)
    lifts = float(row.get("total_lifts") or 0)
    return float(np.log1p(ha) + 0.35 * np.log1p(trails) + 0.15 * np.log1p(lifts))


def assign_size_tiers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scores = out.apply(size_score, axis=1)
    # Clay mesh target is <1 MB; skip the top ~5% megaresorts in automated batches.
    cap = scores.quantile(0.95)
    out = out[scores <= cap].copy()
    scores = out.apply(size_score, axis=1)
    q25, q75 = scores.quantile(0.25), scores.quantile(0.75)
    tier = np.where(scores >= q75, "large", np.where(scores <= q25, "small", "medium"))
    out["size_score"] = scores
    out["size_tier"] = tier
    return out


def _diverse_pick(pool: pd.DataFrame, n: int, rng: np.random.Generator) -> list[pd.Series]:
    if pool.empty or n <= 0:
        return []
    countries = sorted(pool["country"].dropna().unique(), key=lambda c: (c, rng.random()))
    by_country: dict[str, list[pd.Series]] = {}
    for _, row in pool.iterrows():
        by_country.setdefault(str(row["country"]), []).append(row)
    for rows in by_country.values():
        rng.shuffle(rows)
    picked: list[pd.Series] = []
    used_ids: set[str] = set()
    while len(picked) < n:
        progressed = False
        for country in countries:
            rows = by_country.get(country) or []
            while rows:
                row = rows.pop(0)
                wid = str(row["winter_sports_id"])
                if wid in used_ids:
                    continue
                picked.append(row)
                used_ids.add(wid)
                progressed = True
                break
            if len(picked) >= n:
                break
        if not progressed:
            break
    return picked[:n]


def pick_clay_batch(
    df: pd.DataFrame,
    *,
    blocked: set[str],
    batch_size: int,
    large_n: int,
    small_n: int,
    seed: int,
) -> list[dict[str, Any]]:
    pool = df[~df["winter_sports_id"].isin(blocked)].copy()
    if pool.empty:
        return []
    pool = assign_size_tiers(pool)
    medium_n = max(0, batch_size - large_n - small_n)
    quotas = {"large": large_n, "medium": medium_n, "small": small_n}
    rng = np.random.default_rng(seed)
    picked_rows: list[pd.Series] = []
    for tier, quota in quotas.items():
        tier_pool = pool[~pool["winter_sports_id"].isin({str(r["winter_sports_id"]) for r in picked_rows})]
        tier_pool = tier_pool[tier_pool["size_tier"] == tier].sort_values("size_score", ascending=False)
        picked_rows.extend(_diverse_pick(tier_pool, quota, rng))
    if len(picked_rows) < batch_size:
        need = batch_size - len(picked_rows)
        rest = pool[~pool["winter_sports_id"].isin({str(r["winter_sports_id"]) for r in picked_rows})]
        rest = rest.sort_values("size_score", ascending=False)
        picked_rows.extend(_diverse_pick(rest, need, rng))
    out = []
    for row in picked_rows[:batch_size]:
        out.append(
            {
                "winter_sports_id": str(row["winter_sports_id"]),
                "name": str(row.get("english_name") or row["name"]),
                "english_name": str(row.get("english_name") or ""),
                "region": str(row["region"]),
                "state": str(row.get("state") or ""),
                "country": str(row.get("country") or ""),
                "downhill_trails": float(row.get("downhill_trails") or 0),
                "skiable_terrain_ha": float(row.get("skiable_terrain_ha") or 0),
                "size_tier": str(row.get("size_tier") or "medium"),
                "size_score": round(float(row.get("size_score") or 0), 3),
            }
        )
    return out


def playable_ver_for(winter_sports_id: str) -> str | None:
    if not GAME_CATALOG.is_file():
        return None
    try:
        catalog = json.loads(GAME_CATALOG.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    for row in catalog.get("resorts") or []:
        if str(row.get("winter_sports_id") or "") == winter_sports_id:
            path = str(row.get("path") or "")
            if "/" in path:
                return path.rsplit("/", 1)[-1]
    return None


def catalog_entry(cfg, row: dict[str, Any]) -> dict[str, Any]:
    continent, region_label = continent_from_region(cfg.region)
    return {
        "id": cfg.resort_id,
        "winter_sports_id": cfg.winter_sports_id,
        "display_name": cfg.display_name,
        "short_name": short_display_name(cfg.display_name),
        "continent": continent,
        "region_label": region_label,
        "country": cfg.country or row.get("country") or "",
        "playable_ver": playable_ver_for(cfg.winter_sports_id),
        "size_tier": row.get("size_tier"),
        "downhill_trails": row.get("downhill_trails"),
        "skiable_terrain_ha": row.get("skiable_terrain_ha"),
    }


def merge_catalog_entries(existing: list[dict], incoming: list[dict]) -> list[dict]:
    by_id = {str(r.get("id")): r for r in existing if r.get("id")}
    by_ws = {str(r.get("winter_sports_id")): r for r in existing if r.get("winter_sports_id")}
    merged = list(existing)
    for row in incoming:
        rid = str(row.get("id") or "")
        wid = str(row.get("winter_sports_id") or "")
        if rid in by_id or wid in by_ws:
            continue
        merged.append(row)
        by_id[rid] = row
        by_ws[wid] = row
    return merged


def write_catalog(entries: list[dict]) -> None:
    base = {
        "schema_version": "1.0",
        "description": (
            "Clay 3D resort scenes for homepage hero and wiki 3D Map tab. "
            "Join key: winter_sports_id → id (folder clay_scenes/{id}/). Do not use wiki pageId."
        ),
        "path_contract": {
            "asset_root": "clay_scenes/{resort_id}/",
            "catalog": "clay_scenes/catalog.json",
            "wiki_join_key": "winter_sports_id",
            "scale_note": (
                "At ~2k–4k resorts, prefer /clay_scenes/by-ws/{winter_sports_id}.json "
                "instead of loading the full catalog on every page."
            ),
        },
    }
    clean = []
    for row in entries:
        clean.append(
            {k: v for k, v in row.items() if k not in {"size_tier", "downhill_trails", "skiable_terrain_ha"}}
        )
    payload = {**base, "resorts": clean}
    CATALOG.parent.mkdir(parents=True, exist_ok=True)
    CATALOG.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def config_for_row(row: dict[str, Any], used_ids: list[str]):
    wid = str(row["winter_sports_id"])
    # Reuse stable catalog folder id when re-exporting an existing resort.
    if CATALOG.is_file():
        try:
            for entry in json.loads(CATALOG.read_text(encoding="utf-8")).get("resorts") or []:
                if str(entry.get("winter_sports_id") or "") == wid and entry.get("id"):
                    from game_export.config import config_from_mapping, _default_gameplay_mapping

                    data = dict(_default_gameplay_mapping())
                    data.update(
                        {
                            "resort_id": str(entry["id"]),
                            "display_name": str(entry.get("display_name") or row.get("name") or entry["id"]),
                            "winter_sports_id": wid,
                            "region": str(row.get("region") or ""),
                            "state": str(row.get("state") or ""),
                            "country": str(row.get("country") or entry.get("country") or ""),
                            "approximate_location_name": str(row.get("state") or row.get("country") or ""),
                        }
                    )
                    return config_from_mapping(data, source=f"catalog:{wid}")
        except Exception:
            pass
    for path in sorted((REPO / "config" / "resorts").glob("*.yaml")):
        try:
            cfg = load_resort_config(path)
        except Exception:
            continue
        if cfg.winter_sports_id == wid:
            return cfg
    return config_from_candidate(row, used_ids=used_ids)


def print_batch(rows: list[dict[str, Any]]) -> None:
    print(f"{'tier':<8} {'trails':>6} {'ha':>8}  {'country':<14} {'ws_id':<12} name")
    for row in rows:
        print(
            f"{row['size_tier']:<8} {row['downhill_trails']:>6.0f} {row['skiable_terrain_ha']:>8.1f}  "
            f"{row['country']:<14} {row['winter_sports_id']:<12} {row['name']}"
        )


def rows_from_winter_sports_ids(df: pd.DataFrame, wids: list[str]) -> list[dict[str, Any]]:
    by_id = {str(r["winter_sports_id"]): r for _, r in df.iterrows()}
    out: list[dict[str, Any]] = []
    for wid in wids:
        row = by_id.get(str(wid))
        if row is None:
            log.error("winter_sports_id %s not in analyzed parquet", wid)
            continue
        scored = assign_size_tiers(pd.DataFrame([row]))
        r = scored.iloc[0]
        out.append(
            {
                "winter_sports_id": str(r["winter_sports_id"]),
                "name": str(r.get("english_name") or r["name"]),
                "english_name": str(r.get("english_name") or ""),
                "region": str(r["region"]),
                "state": str(r.get("state") or ""),
                "country": str(r.get("country") or ""),
                "downhill_trails": float(r.get("downhill_trails") or 0),
                "skiable_terrain_ha": float(r.get("skiable_terrain_ha") or 0),
                "size_tier": str(r.get("size_tier") or "medium"),
                "size_score": round(float(r.get("size_score") or 0), 3),
            }
        )
    return out


def failed_winter_sports_ids(progress: dict[str, Any]) -> list[str]:
    out = []
    for row in progress.get("failed") or []:
        if isinstance(row, dict):
            wid = str(row.get("winter_sports_id") or "").strip()
        else:
            wid = str(row).strip()
        if wid and wid not in out:
            out.append(wid)
    return out


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument("--large", type=int, default=3, help="Large resorts per batch")
    p.add_argument("--small", type=int, default=3, help="Small resorts per batch")
    p.add_argument("--max-batches", type=int, default=1, help="0 = until queue empty")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--analyzed", type=Path, default=ANALYZED)
    p.add_argument("--data-root", type=Path, default=REPO / "output")
    p.add_argument("--cache-dir", type=Path, default=REPO / "cache")
    p.add_argument("--from-s3", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--s3-bucket", default=None)
    p.add_argument("--force", action="store_true", help="Rebuild scenes even if output exists")
    p.add_argument(
        "--fetch-skadi",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Download Mapzen Skadi DEM tiles when local DEM is missing (default: on)",
    )
    p.add_argument("--pick-only", action="store_true", help="Print selection and exit")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--upload", action="store_true", help="Run scripts/upload_clay_scenes.py after export")
    p.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-export winter_sports_ids listed in config/clay_scenes/_progress.json failed[]",
    )
    p.add_argument(
        "--winter-sports-ids",
        default=None,
        help="Comma-separated winter_sports_ids to export (skips stratified pick)",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    _setup_logging()
    args = parse_args(argv)
    if args.large + args.small > args.batch_size:
        log.error("--large + --small must be <= --batch-size")
        return 2
    df = load_analyzed(args.analyzed)
    progress = load_progress()
    blocked = blocked_ids(progress, include_failed=True)
    s3_bucket = args.s3_bucket or default_s3_bucket()
    batches_done = 0
    seed = args.seed

    explicit_ids: list[str] = []
    if args.winter_sports_ids:
        explicit_ids = [x.strip() for x in args.winter_sports_ids.split(",") if x.strip()]
    elif args.retry_failed:
        explicit_ids = failed_winter_sports_ids(progress)
        if not explicit_ids:
            log.info("No failed ids to retry")
            return 0
        # Allow re-export of failed ids
        blocked -= set(explicit_ids)
        # Clear failed entries we're about to retry
        progress["failed"] = [
            f
            for f in (progress.get("failed") or [])
            if str((f.get("winter_sports_id") if isinstance(f, dict) else f) or "") not in set(explicit_ids)
        ]

    while True:
        if args.max_batches and batches_done >= args.max_batches:
            break
        if explicit_ids:
            rows = rows_from_winter_sports_ids(df, explicit_ids)
            explicit_ids = []  # one shot
        else:
            rows = pick_clay_batch(
                df,
                blocked=blocked,
                batch_size=args.batch_size,
                large_n=args.large,
                small_n=args.small,
                seed=seed + batches_done,
            )
        if not rows:
            log.info("No more resorts to pick (blocked=%s)", len(blocked))
            break
        batches_done += 1
        log.info("=== clay batch %s (%s resorts, seed=%s) ===", batches_done, len(rows), seed + batches_done - 1)
        print_batch(rows)
        if args.pick_only:
            continue

        used_ids = [str(r.get("id") or "") for r in json.loads(CATALOG.read_text(encoding="utf-8")).get("resorts") or []] if CATALOG.is_file() else []
        new_catalog_entries: list[dict] = []
        batch_meta = {
            "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "seed": seed + batches_done - 1,
            "winter_sports_ids": [r["winter_sports_id"] for r in rows],
        }

        for row in rows:
            wid = row["winter_sports_id"]
            try:
                cfg = config_for_row(row, used_ids)
            except Exception as exc:
                log.error("Config failed %s: %s", wid, exc)
                progress["failed"].append({"winter_sports_id": wid, "reason": f"config:{exc}"})
                blocked.add(wid)
                continue
            used_ids.append(cfg.resort_id)
            log.info(
                "Export %s (%s) tier=%s trails=%.0f ha=%.1f",
                cfg.resort_id,
                cfg.display_name,
                row["size_tier"],
                row["downhill_trails"],
                row["skiable_terrain_ha"],
            )
            if args.dry_run:
                new_catalog_entries.append(catalog_entry(cfg, row))
                if wid not in progress["done"]:
                    progress["done"].append(wid)
                blocked.add(wid)
                continue
            try:
                export_homepage_scene(
                    cfg,
                    data_root=args.data_root,
                    cache_dir=args.cache_dir,
                    out_root=args.data_root,
                    from_s3=args.from_s3,
                    s3_bucket=s3_bucket,
                    fetch_skadi=args.fetch_skadi,
                    force=args.force,
                )
            except Exception as exc:
                log.exception("Export failed %s", cfg.resort_id)
                progress["failed"].append(
                    {"winter_sports_id": wid, "resort_id": cfg.resort_id, "reason": str(exc)}
                )
                blocked.add(wid)
                continue
            new_catalog_entries.append(catalog_entry(cfg, row))
            if wid not in progress["done"]:
                progress["done"].append(wid)
            blocked.add(wid)

        if new_catalog_entries:
            existing = json.loads(CATALOG.read_text(encoding="utf-8")).get("resorts") or [] if CATALOG.is_file() else []
            merged = merge_catalog_entries(existing, new_catalog_entries)
            if args.dry_run:
                log.info("dry-run: would write %s catalog entries (+%s)", len(merged), len(new_catalog_entries))
            else:
                write_catalog(merged)
                log.info("Catalog now has %s resorts (+%s)", len(merged), len(new_catalog_entries))

        progress["batches"].append(batch_meta)
        if not args.dry_run:
            save_progress(progress)
        if args.pick_only:
            break
        # Explicit id lists are one-shot; stop after that batch.
        if args.retry_failed or args.winter_sports_ids:
            break

    if args.pick_only:
        return 0

    if args.upload and not args.dry_run:
        cmd = [sys.executable, str(REPO / "scripts" / "upload_clay_scenes.py")]
        log.info("Uploading clay scenes: %s", " ".join(cmd))
        return subprocess.call(cmd)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
