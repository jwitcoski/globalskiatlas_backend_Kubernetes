# Regional overview maps (country / state / territory)

One **overview map per country** and **one per state/province/territory** that has downhill ski areas in the atlas dataset. Style target: Pennsylvania reference — sage page background, thick admin outline, winter DEM inside the boundary, mountain icons scaled by resort size, every resort labeled.

These are separate from **per-resort** plates (`data_to_qgis`); they use a single shared QGIS template and batch data export.

---

## What you need

| Piece | Location / action |
|-------|-------------------|
| Ski area dataset | `output/combined/ski_areas_analyzed.parquet` (or per-region `ski_areas.parquet` after `combine_regions.py`) |
| Admin boundaries | [Natural Earth 10m](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/) → extract into `boundaries/` (`ne_10m_admin_0_countries.shp`, `ne_10m_admin_1_states_provinces.shp`) |
| QGIS template | `atlas/map_gen/templates/atlas_overview_template.qgz` (create once in QGIS — see below) |
| Data + projects | `python -m atlas.map_gen.regional_overview --all` |
| PNG export | `atlas\map_gen\run_export_layouts.bat --work-dir atlas_work/overview` (layout name **Regional Overview**) |

Resort **icon tier** (`small` / `medium` / `large` / `mega`) matches the book/wiki buckets in `atlas/book_gen/constants.py` (`resortSizeCategory` → `map_tier`), not the trail-count tiers used for per-resort layout templates.

---

## QGIS template (one-time)

Create **`atlas/map_gen/templates/atlas_overview_template.qgz`** with these layers (exact names — the patch script matches on them):

| Layer name | Type | Role |
|------------|------|------|
| `admin_boundary` | Polygon | Single country or state outline (from export) |
| `dem_hillshade` | Raster | Winter hillshade clipped to boundary (`dem_hillshade.tif`) |
| `ski_resorts` | Point | All resorts in the unit; fields `name`, `map_tier` |

### Styling (Pennsylvania look)

1. **Print layout** named `Regional Overview` (landscape ~210×148 mm or half-page per `config/atlas.yaml` `overview.page_size`).
2. **Layout background**: sage green (~`#c8d4b8` or sample from reference).
3. **`admin_boundary`**: fill white; stroke dark green ~2–2.5 mm; no fill outside (mask DEM to boundary in QGIS: raster clip or set raster extent + boundary on top).
4. **`dem_hillshade`**: single-band gray or pale blue ramp (white = high, light blue = low); low contrast; optional slight blur. Build from SRTM/Mapzen in QGIS (**Raster → Analysis → Hillshade**) or drop in pre-built `dem_hillshade.tif` from the export folder.
5. **`ski_resorts`**: SVG mountain marker (create once under `atlas/map_gen/icons/mountain.svg`). **Rule-based** or data-defined size on `map_tier`:

   | `map_tier` | Marker size (mm) |
   |-----------|------------------|
   | `small` | 4 |
   | `medium` | 6 |
   | `large` | 8 |
   | `mega` | 10 |

6. **Labels** on `ski_resorts`: field `name`, dark sans-serif, ~7–8 pt, placement around point (offset 2–3 mm), allow small overlap only in dense clusters (Poconos-style).
7. **Title** on layout (not map canvas): two lines, e.g. `PENNSYLVANIA` / `SKI AREAS`, bold caps, dark green, upper-right.

Save the template. The batch script copies it per unit and rewrites datasource paths to each folder under `atlas_work/overview/`.

---

## Data export and QGZ generation

```powershell
# List countries/states that would get a map (needs parquet + boundaries)
python -m atlas.map_gen.regional_overview --list

# One state (names must match Natural Earth ADMIN / name fields)
python -m atlas.map_gen.regional_overview --state "Pennsylvania" --country "United States of America"

# One country
python -m atlas.map_gen.regional_overview --country "Austria"

# All units with at least one downhill resort
python -m atlas.map_gen.regional_overview --all

# Skip QGZ if template not ready yet
python -m atlas.map_gen.regional_overview --all --data-only
```

Output layout:

```
atlas_work/overview/
  countries/<country-slug>/
    admin_boundary.geojson
    ski_resorts.geojson
    dem_hillshade.tif          # optional; add manually or future --fetch-dem
    <slug>_overview_map.qgz
  states/<country-slug>/<state-slug>/
    ...
```

Export PNGs:

```powershell
atlas\map_gen\run_export_layouts.bat --work-dir atlas_work/overview --dpi 300
```

---

## Scale

- **Countries**: one map per distinct `Country` in parquet (Natural Earth `ADMIN` match).
- **States/territories**: one map per distinct `Country` + `State` where the state is non-empty (admin-1 polygons).
- Units with **zero** downhill resorts (or `resort_type` = “not a downhill ski resort”) are skipped.
- Expect **~100+ countries** and **~500+ states/provinces** worldwide once the combined dataset is complete; generate in batches (`--country`, `--continent` filter on parquet `region` column if needed).

---

## Book / wiki

- Wiki region pages use layout tier `large` (~half page) — see `REGION_WIKI_LAYOUT_TIER` in `scripts/generate_resort_copy_bedrock.py`.
- Scribus book chapters can place `overview_export.png` on the chapter opener; wire that in `atlas/book_gen` when overview PNGs exist.

---

## Related

- Per-resort maps: `python -m atlas.map_gen.data_to_qgis`
- Mapper upload paths: `docs/MAPPER_WORKFLOW.md` § Overview Maps
- `config/atlas.yaml` → `overview` section
