# QGIS templates for atlas map generation

Place the resort template project here so the data-to-qgis script can copy it per resort.

## Required

- **atlas_resort_template.qgz** — Create in QGIS 3.x with layers that expect these datasources (script will replace paths):
  - **Ski Resort Boundary** — one polygon (e.g. from `ski_area.geojson`).
  - **OSM Polygons Clipped** — polygons from `osm_polygons.geojson`.
  - **OSM Lines Clipped** — lines from `osm_lines.geojson`.
  - **OSM Points Clipped** — points from `osm_points.geojson`.
  - **Clipped Contours** — contour lines from `contours.geojson`.

Use a **worldwide CRS** (e.g. EPSG:4326 WGS 84). Add scale bar, north arrow, and layout(s) for page sizes (small/medium/large/mega) as in `config/atlas.yaml`.

If you use the reference processing model `SkiOutputOSM07182024.model3`, the script writes layer names that match the model output (see Phase 4 plan §1.1).

## Optional

- **atlas_overview_template.qgz** — For country/state overview maps (admin bounds + ski area points).
- **styles/** — QML style files referenced by the template for consistent symbology.

Until the template is added, the script will skip copying the project file and only write the per-resort GeoJSON/GeoPackage layers.
