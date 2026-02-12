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

(Original enrich breakdown before fix: step 1 4.8 s, step 2 (lifts) 347.7 s, step 3 (pistes) 396.9 s, step 4 0.1 s.)

---

## Where to fix (performance issues)

### 1. Enrich step — **fixed**

- **Issue**: For every lift and piste feature, `_lookup_country_state()` was called and **read both Natural Earth shapefiles from disk** and ran a spatial join. So 145 lifts ⇒ 290 shapefile reads; 184 pistes ⇒ 368 more. That’s why lifts took ~348 s and pistes ~397 s (~2 s per feature).
- **Fix (implemented)**: Boundaries are loaded **once** via `_load_boundaries()`; `_batch_lookup_country_state()` does a single sjoin of all centroids against countries and states. In `_run_enrich_all`, the same cache is passed to all four steps. Ski-area lookups use an STRtree when available (`_build_ski_area_index` / `_ski_area_at_point_indexed`) so many areas scale well.

### 2. osm_nearby (extract_nearby_from_pbf.py) — **fixed**

- **Issue**: It looped **per ski area** and ran `osmium extract -b <bbox> <full_pbf>`. With 500 areas (e.g. North America) that was 500 full PBF reads. That did not scale.
- **Fix (implemented)**: One **merged bbox** over all ski areas (with radius buffer) → **single** `osmium extract` and **single** ogr2ogr per layer. Then in Python, for each extracted feature we compute its centroid, check which ski area(s) are within `radius_m` (haversine), and emit one element per (feature, ski area) so output shape is unchanged.

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

So **~10 min for 373 MB** and 28 ski areas. North America is **~43× the PBF size** (16 GB vs 0.37 GB) and **~15–25× the ski areas** (500 vs 28); PBF-bound steps (extract, osm_nearby, lifts_pistes) dominate and scale with file size. That supports a **4–7 h** total for NA (see below).

---

## North America estimate

Rough scale:

- **Geofabrik**: `north-america-latest.osm.pbf` **~15–17 GB** (2024–25); subregions e.g. us-west ~1.2 GB, us-northeast ~400 MB, canada ~2.2 GB.
- **Ski areas**: order of **300–700** (US + Canada).
- **Lifts/pistes**: order of **15k–30k** lifts and **25k–50k+** pistes (and osm_near elements can be 100k+).

### If we had **no** optimizations (for comparison)

| Phase | North America (e.g. 500 areas) |
|-------|---------------------------------|
| Download | ~10–30 min |
| Extract (winter_sports) | ~30–60 min |
| osm_nearby | **100+ hours (infeasible)** — 500 × full PBF read |
| enrich_geojson | **~1–3 days** — shapefile read per feature |
| **Total** | **Days to a week+** |

### With **both fixes** (current pipeline, calibrated from NZ)

| Phase | North America (rough) |
|-------|------------------------|
| Download | **~15–45 min** | 15–17 GB at 5–20 MB/s |
| Extract (winter_sports) | **~30–60 min** | One pass over full PBF |
| osm_nearby | **~1.5–3.5 hours** | One merged bbox extract + filter (scales with PBF size) |
| lifts_and_pistes | **~30–90 min** | One pass over full PBF |
| enrich_geojson | **~10–30 min** | Boundaries once, batch sjoin, STRtree (scales with feature count) |
| analyze + export_parquet | **~2–5 min** |
| **Total** | **~4–7 hours** | Overnight run; run a subregion first (e.g. us-northeast) to get real timings |

### Practical approach

1. **Optimize enrich** (load shapefiles once, index/batch lookups).
2. **Optimize osm_nearby** (one or few PBF extracts, then tag by ski area).
3. **Test on a subregion** first (e.g. `us-northeast` or `us-west` or a single state if available) to get real timings and validate.
4. Then run full North America (or continent in one go) with the optimized pipeline.

---

## Summary

- **Iceland**: **Before** ~13–14 min (enrich ~12.5 min). **After both fixes** ~30 s (enrich ~7 s). ~7× faster end-to-end.
- **New Zealand**: **9m 44s** for 373 MB PBF, 28 areas, 947 lifts, 676 pistes (enrich 12.2 s). Calibration point for larger regions.
- **North America**: **Before** fixes: infeasible (days to a week+). **With both fixes**: **~4–7 hours** (15–17 GB PBF; download 15–45 min, extract 30–60 min, osm_nearby 1.5–3.5 h, lifts_pistes 30–90 min, enrich 10–30 min). Run a subregion first (e.g. us-northeast) to calibrate.
