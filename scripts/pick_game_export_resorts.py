"""Pick 10 mid-size named ski areas from combined parquet (reproducible RNG)."""
from __future__ import annotations

import json
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SEED = 20260826


def slugify(name: str, country: str) -> str:
    base = f"{name} {country}"
    s = re.sub(r"[^a-zA-Z0-9]+", "_", base).strip("_").lower()
    return s[:48] or "resort"


def main() -> None:
    rng = np.random.default_rng(SEED)
    areas = gpd.read_parquet(ROOT / "output" / "combined" / "ski_areas.parquet")
    pistes = pd.read_parquet(
        ROOT / "output" / "combined" / "pistes.parquet",
        columns=["Ski Area", "Country"],
    )
    cnt = pistes.groupby(["Ski Area", "Country"], dropna=False).size().reset_index(name="piste_n")
    areas = areas[areas.geometry.notna() & ~areas.geometry.is_empty].copy()
    areas["name"] = areas["name"].astype(str)
    b = areas.geometry.bounds
    lat = areas.geometry.centroid.y
    w_km = (b.maxx - b.minx) * 111.0 * np.cos(np.radians(lat))
    h_km = (b.maxy - b.miny) * 111.0
    areas["diag_km"] = np.hypot(w_km, h_km)
    m = areas.merge(cnt, left_on=["name", "Country"], right_on=["Ski Area", "Country"], how="inner")
    m = m[(m["piste_n"] >= 18) & (m["piste_n"] <= 90) & (m["diag_km"] >= 0.8) & (m["diag_km"] <= 7.5)]
    m = m[~m["name"].str.contains("Montage", case=False, na=False)]
    m = m[m["name"].str.len() > 3]
    m = m[~m["name"].str.lower().isin(["ski area", "skiing", "winter sports"])]
    countries = list(m["Country"].dropna().unique())
    rng.shuffle(countries)
    picks = []
    used = set()
    for c in countries:
        pool = m[(m["Country"] == c) & (~m["osm_way_id"].astype(str).isin(used))]
        if pool.empty:
            continue
        row = pool.sample(1, random_state=int(rng.integers(1, 1e9))).iloc[0]
        picks.append(row)
        used.add(str(row["osm_way_id"]))
        if len(picks) >= 10:
            break
    out = []
    for r in picks[:10]:
        slug = slugify(str(r["name"]), str(r["Country"] or r["region"]))
        out.append(
            {
                "resort_id": slug,
                "display_name": str(r["name"]),
                "winter_sports_id": str(int(float(r["osm_way_id"]))),
                "region": str(r["region"]),
                "state": str(r.get("State") or ""),
                "country": str(r.get("Country") or ""),
                "approximate_location_name": str(r.get("State") or r.get("Country") or ""),
                "piste_n": int(r["piste_n"]),
                "diag_km": round(float(r["diag_km"]), 2),
            }
        )
    dest = ROOT / "config" / "resorts" / "_picked_batch.json"
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    tpl = (ROOT / "config" / "resorts" / "montage_mountain_pa.yaml").read_text(encoding="utf-8")
    body = tpl.split("status: prototype", 1)[1]
    for r in out:
        text = (
            f"# OSM winter_sports way {r['winter_sports_id']}\n"
            f"resort_id: {r['resort_id']}\n"
            f"display_name: {json.dumps(r['display_name'], ensure_ascii=True)}\n"
            f"winter_sports_id: \"{r['winter_sports_id']}\"\n"
            f"region: {r['region']}\n"
            f"state: {json.dumps(r['state'], ensure_ascii=True)}\n"
            f"country: {json.dumps(r['country'], ensure_ascii=True)}\n"
            f"approximate_location_name: {json.dumps(r['approximate_location_name'], ensure_ascii=True)}\n"
            f"game_style: classic_arcade\n"
            f"seed: 20260826\n"
            f"status: prototype"
            f"{body}"
        )
        ypath = ROOT / "config" / "resorts" / f"{r['resort_id']}.yaml"
        ypath.write_text(text, encoding="utf-8")
        print("wrote", ypath.name)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
