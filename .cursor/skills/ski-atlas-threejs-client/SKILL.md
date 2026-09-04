---
name: ski-atlas-threejs-client
description: >-
  Constraints for Three.js work related to this GIS repo. The playable skier
  lives in GlobalSkiAtlas_2, not here. Use this overlay only if the user asks
  to change how this pipeline exports scene cakes for that client. Do not apply
  to OSM/DEM/export internals unless they asked for export changes.
---

# Ski Atlas Three.js client (this repo)

This repository is the **GIS data pipeline** and **game scene export**. The browser game is in **GlobalSkiAtlas_2**.

## Scope

**In scope here**

- Python `game_export` scene cakes: `scene-manifest.json`, heightfield, GLB, GeoJSON, catalog.json
- Coordinate frame and OSM attribution that the frontend client depends on

**Out of scope here**

- Rebuilding `game_export/playable` or a debug viewer in this repo
- Regenerating terrain in the browser with GDAL, GeoPandas, or OSM raster tiles
- Scraping official resort trail maps or copyrighted map art

## Hard constraints

1. **Terrain and routes come from the exporter.** The website client loads `scene-manifest.json` and the exported heightfield / GLB / GeoJSON.
2. **Coordinate frame:** local meters, Three.js **X east, Y elevation, Z −north**.
3. **Legal:** OSM ODbL attribution. Not an official resort product. Not for navigation or avalanche safety.
4. Do not copy a Three.js client back into this GIS repo; change GlobalSkiAtlas_2 instead.
