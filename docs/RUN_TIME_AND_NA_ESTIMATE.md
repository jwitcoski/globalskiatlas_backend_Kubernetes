# Iceland Run Time & North America Estimate

## Iceland: before vs after fixes

| Phase | Before fixes | After both fixes |
|-------|--------------|------------------|
| Download | ~5 s | ~4–5 s |
| Extract (winter_sports) | seconds | seconds |
| osm_nearby | seconds (1 extract) | seconds (1 merged extract, then distance filter) |
| lifts_and_pistes | seconds | seconds |
| **enrich_geojson** | **~12.5 min (749 s)** | **~7 s** |
| analyze / export_parquet | seconds | seconds |
| **Total pipeline** | **~13–14 min** | **~2 min** |

So Iceland is about **7× faster** end-to-end; enrich alone went from ~12.5 min to ~7 s (~115×).

---

## OOM and clustering

The pipeline runs **per region** (from `config/regions.yaml`). Each region downloads its PBF, extracts winter_sports, then runs **osm_nearby** — extracting OSM data within ~2 km of each ski area.

- **Issue**: If a region has many ski areas spread across a large PBF, a single merged bbox can cover too much area → osmium extracts nearly the full PBF → OOM.
- **Fix**: **Cluster by proximity** (default 300 km). Ski areas within `cluster_dist_m` share one extract; distant clusters get separate extracts. Memory = max(cluster extract size), not region-sized.
- **Config**: `cluster_dist_m` in `regions.yaml` overrides the default. Smaller = more clusters = less memory. Used for Italy nord-est/nord-ovest (30 km), Russia central-fed-district (20 km), US ski states and Canadian provinces (25 km).

Regions with no sub-regions and large PBFs (e.g. Baden-Württemberg 601 MB, Bayern 793 MB, Netherlands 1.3 GB) were split into Geofabrik sub-regions to avoid OOM.

---

## Calibration: New Zealand (actual run)

| Phase | New Zealand (373 MB PBF) |
|-------|---------------------------|
| Download | ~21 s |
| Extract | 28 winter_sports |
| osm_nearby | 3,959 elements (28 areas) |
| lifts_and_pistes | 947 lifts, 676 pistes |
| enrich_geojson | **12.2 s** (all 4 steps) |
| analyze + export_parquet | seconds |
| **Total** | **9m 44s** |

~10 min for 373 MB and 28 ski areas.

---

## North America: per-region runs

North America is split by **Canada** (provinces) and **US** (states). Each run processes one PBF (e.g. California ~600 MB, Colorado ~250 MB).

| Region type | PBF size (approx) | Est. runtime | Notes |
|-------------|-------------------|--------------|-------|
| Small state (Vermont, NH) | ~50–150 MB | 5–15 min | |
| Medium state (Colorado, Utah) | ~200–350 MB | 15–45 min | `cluster_dist_m: 25000` |
| Large state (California, NY) | ~500–650 MB | 30–90 min | `cluster_dist_m: 25000` |
| Canadian province (AB, BC, ON, QC) | ~200–600 MB | 20–60 min | `cluster_dist_m: 25000` |

**Total North America**: Sum of all region runtimes; sequential runs can take 4–7+ hours for the full continent. Use `--from-slug` to resume after a region.

---

## Summary

- **Iceland**: ~2 min end-to-end (enrich fix + single extract).
- **New Zealand**: ~10 min for 373 MB PBF.
- **North America**: Per-state/province; 5–90 min per region depending on size; 4–7+ hours for full continent. OOM avoided via `cluster_dist_m` and Geofabrik sub-regions where applicable.
