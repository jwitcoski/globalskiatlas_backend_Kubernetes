# Atlas book generation (Scribus)

Automates print-ready **state chapters** for the Global Ski Atlas coffee-table book. Layout mirrors the [wiki resort page](https://github.com/GlobalSkiAtlas_2/wiki/resort.html): **wiki colors and typography** (`wiki/css/style.css`), light-blue stats panel, label/value stats grid, location/title/subtitle hierarchy, drop-cap body on larger tiers, and static map side-by-side (alternating left/right per tier).

## Trim size

Default **8.5 × 11 in** (KDP large paperback). Configure in `config/book.yaml`.

## Pipeline

```powershell
# From repo root — local parquet + local atlas_work maps only (default, fast)
py -m atlas.book_gen.run_scribus_book --state Virginia --region north-america/us/virginia

# Only resorts that already have *_export.png under atlas_work/.../virginia/
# (Virginia S3/wiki maps not required)

# Optional: wiki API + S3 when you need remote content/maps
py -m atlas.book_gen.run_scribus_book --state Virginia --no-local-only

# Steps individually
py -m atlas.book_gen.build_chapter_manifest --state Virginia --parquet-only
py -m atlas.book_gen.pack_pages  # via run_scribus_book
```

Outputs under `atlas_work/book/{state}/`:

| File | Purpose |
|------|---------|
| `manifest.json` | Resorts + Scribus text fields + map paths |
| `layout_plan.json` | Physical pages and ¼/½/1/2 placements |
| `chapter.sla` | Scribus document |
| `chapter.pdf` | Scribus 1.6.x + **pypdf** (`pip install pypdf`) to merge `_pdf_pages/` |
| `chapter_data.csv` | Optional ScribusGenerator input |

## Prerequisites

1. **Map PNGs** — `py -m atlas.map_gen.run_resort_maps_pipeline --region north-america/us/virginia`, or maps on S3 (`globalskiatlas-resort-maps`).
2. **Wiki content** — GlobalSkiAtlas_2 server (`wiki_api_base` in `book.yaml`) or parquet via `parquet_url`.
3. **Scribus 1.6.x** (optional) — exports each page to `_pdf_pages/page_*.pdf`.
4. **pypdf** — `pip install pypdf` merges those parts into `chapter.pdf` (without it, `_pdf_pages` updates but `chapter.pdf` stays stale).

### PDF export (Windows)

```powershell
py -m atlas.book_gen.run_scribus_book --state Virginia --region north-america/us/virginia

# If Scribus already wrote _pdf_pages but chapter.pdf is old (missing pypdf):
py -m atlas.book_gen.run_scribus_book --state Virginia --merge-pdf-only
```

## Page allocation

| Wiki `resortSizeCategory` | Book pages | Map tier |
|---------------------------|------------|----------|
| `small_hill` | ¼ | small |
| `ski_mountain` | ½ | medium |
| `multiple_mountains` | 1 | large |
| `mega_resort` | 2 (spread) | mega |
| `unknown` | skipped | — |

### Chapter order and layout

Within each state chapter, resorts are ordered **by size tier**, then **alphabetically by title** within that tier:

1. **Small** (`small_hill`) — horizontal rows sized to the **small QGIS plate** (105×74.25 mm at export DPI); **map alternates right / left** for each resort (1st right, 2nd left, …); counter **resets at medium**. **100% scale** (no shrink in Scribus). Up to four rows per page when they fit; taller maps may yield three per sheet.
2. **Medium** (`ski_mountain`) — half page (two per sheet when packed).
3. **Large** (`multiple_mountains`) — full page, text left / map right.
4. **Mega** (`mega_resort`) — two-page spread.

Set `quarters_per_sheet: 4` in `config/book.yaml` (default). Use `1` only for debugging one resort per page.

Map frames use `map_export_dpi` in `book.yaml` (must match QGIS `run_export_layouts --dpi`). Plate mm sizes come from `atlas/map_gen/layout_constants.py` (small / medium / large / mega).

## Templates

Regenerate placeholder SLAs:

```powershell
py -m atlas.book_gen.create_templates
```

Files land in `atlas/book_gen/templates/` (`entry_quarter.sla`, `entry_half.sla`, `entry_full.sla`, `chapter_shell.sla`).
