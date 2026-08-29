# Agent notes

## GIS vs game client

Python/GDAL/OSM/DEM work stays in this repo (`game_export` Python package, atlas, scripts). Do not replace it with a generic Three.js terrain generator.

The browser skier is **not** in this repository. It lives in **GlobalSkiAtlas_2** and consumes exported `scene-manifest.json` plus OSM-attributed scene assets from `s3://globalskiatlas-backend-k8s-output/game_scenes/`. Do not scrape official trail maps. Do not re-add a Three.js client under `game_export/`.
