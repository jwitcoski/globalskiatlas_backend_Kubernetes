# QGIS templates for atlas map generation

Place the resort template project here so the data-to-qgis script can copy it per resort.

## `atlas_resort_template.qgz` (included)

The **bryce.qgz** project (from `Ski Atlas\GIS Data\VA\Bryce\`) has been copied here as the default template. It includes ResortOutline, ski slopes, PolygonsClipped, LinesClipped, Contours, and other layers with full styling. The script replaces datasource paths by layer name.

| Layer name (exact)       | GeoPackage file               | Geometry |
|--------------------------|-------------------------------|----------|
| Ski Resort Boundary      | `ski_area.gpkg`               | Polygon  |
| Ski Area 1000ft Buffer   | `ski_area_1000ft_buffer.gpkg` | Polygon  |
| OSM Polygons Clipped     | `osm_polygons.gpkg`           | Polygon  |
| OSM Lines Clipped        | `osm_lines.gpkg`              | Line     |
| OSM Points Clipped       | `osm_points.gpkg`             | Point    |
| Clipped Contours         | `contours.gpkg`               | Line     |
| **Bryce template**       |                               |          |
| ResortOutline            | `ski_area_1000ft_buffer.gpkg` | Polygon  |
| WinterSport Layer        | `ski_area.gpkg`               | Polygon  |
| ski slopes               | `osm_polygons.gpkg`           | Polygon  |
| PolygonsClipped          | `osm_polygons.gpkg`           | Polygon  |
| LinesClipped             | `osm_lines.gpkg`              | Line     |
| Contours                 | `contours.gpkg`               | Line     |

### CRS

Use a **worldwide CRS** (e.g. **EPSG:4326** WGS 84). Configure `config/atlas.yaml` to match.

### Layouts

Add layouts for page sizes as in `config/atlas.yaml`:

- **small** — 105×148 mm (¼ page)
- **medium** — 210×148 mm (½ page)
- **large** — 420×297 mm (full A3)
- **mega** — 420×594 mm (2-page spread)

Include a scale bar and north arrow on each layout.

---

## Step-by-step: Create the template in QGIS

### 1. Generate sample resort data

Run the data-to-qgis script with a limit to write GeoPackage layers for one resort:

```powershell
# From repo root
py -m atlas.map_gen.data_to_qgis --all-resorts --limit 1
```

This creates `atlas_work/resorts/<region>/<id>/` with GeoPackage files (`.gpkg`) per layer.

Or use the seed script to put sample data in the templates folder. Use a larger resort (e.g. Montage Mountain) for better template building:

```powershell
py -m atlas.map_gen.create_template_seed --resort 45096232 --region north-america/us/pennsylvania
```

Without `--resort`/`--region`, the script uses the first resort in the data (often a small one like AfriSki).

### 2. Create a new QGIS project

1. Open QGIS 3.x.
2. **Project → Properties → CRS**: Set to **WGS 84 (EPSG:4326)**.
3. **Layer → Add Layer → Add Vector Layer**: Add each `.gpkg` from `templates/seed/`.  
   Rename each layer **exactly** as in the table above (the script matches by name).

### 3. Apply styles from `SkiOutputOSM07182024.model3`

The `styles/` folder contains QML files from your processing model. Apply each to the matching layer:

| Layer name              | Style file             |
|-------------------------|------------------------|
| Ski Resort Boundary     | (fill/outline as desired) |
| Ski Area 1000ft Buffer  | `styles/Outline.qml`   |
| OSM Polygons Clipped    | `styles/Polygons.qml`  |
| OSM Lines Clipped       | `styles/Lines.qml`     |
| OSM Points Clipped      | `styles/Points.qml`    |
| Clipped Contours        | `styles/Contours.qml`  |

In the model, Outline.qml was applied to the 1000 ft buffer polygon (map extent/frame).

For each layer: right-click → **Properties** → **Symbology** → **Style** (dropdown) → **Load Style…** → select the corresponding `.qml` from `atlas/map_gen/templates/styles/`.

Lift line markers use raster/SVG icons from `atlas/map_gen/icons/` (copied next to each resort QGZ as `./icons/` during `data_to_qgis`).

### 4. Add layouts

1. **Project → New Print Layout** for each page size (small, medium, large, mega).
2. Add a map, scale bar, north arrow, and any title block.
3. Name layouts as in `config/atlas.yaml` if you want them easy to find.

### 5. Save as template

1. **Project → Save As**.
2. Save to `atlas/map_gen/templates/atlas_resort_template.qgz`.
3. The script will copy this template per resort and replace the datasource paths.

---

## Optional

- **atlas_overview_template.qgz** — For country/state overview maps (admin bounds + DEM + ski points). See [docs/REGIONAL_OVERVIEW_MAPS.md](../../../docs/REGIONAL_OVERVIEW_MAPS.md) and `python -m atlas.map_gen.regional_overview`.

### `ski_atlas_small_medium_template.qgz` (Ski Atlas Export — small / medium book maps)

Working snapshot of the **Wintergreen** parquet project: layout **Ski Atlas Export** (book-style page), orthographic globe inset, host-country styling on Natural Earth, lightened tree fills, and label rules tuned for dense maps at small-to-medium print sizes. Layer URIs in the file still reference the Wintergreen work folder under `atlas_work/`; use it as a **layout and symbology template** (open, duplicate, repoint data), or temporarily point `template.resort` in `config/atlas.yaml` here once your `data_to_qgis` layer names and paths align with this project.

To refresh after editing in QGIS: save `atlas_work/wintergreen/wintergreen_map.qgz`, then copy it over `atlas/map_gen/templates/ski_atlas_small_medium_template.qgz`. You can run `atlas_work/restore_wintergreen_globe_inset_and_labels.py` from the QGIS Python console on the working project before copying if you need to rebuild inset styling in one step.

## `styles/` (included)

QML style files from `SkiOutputOSM07182024.model3` — Outline, Polygons, Lines, Points, Contours. Used when building the template (see step 3 above).

---

## Until the template exists

The script skips copying the project file and only writes the per-resort GeoPackage layers. You can still open the `.gpkg` files manually in QGIS.
