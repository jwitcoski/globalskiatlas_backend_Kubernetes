# World-Scale Ski Atlas: Roadmap

Goal: run the pipeline for **the entire world** and serve a map that stays fast and usable at planet scale (thousands of ski areas, hundreds of thousands of lifts/pistes, millions of OSM nearby features).

---

## 1. Data: OSM at planet scale

| Source | Size (approx) | Use case |
|--------|----------------|----------|
| **Geofabrik** | Continental extracts (e.g. europe ~25 GB, north-america ~8 GB) | Run pipeline per continent, then merge or tile per region |
| **Protomaps / BBBike** | Custom extracts by bbox | Smaller regions |
| **Planet** | Full planet ~70+ GB (or smaller “planet with history”) | Single global run if you have the hardware and time |

**Practical path:** Run the pipeline **per continent** (or large region) using Geofabrik PBFs. Each run produces `output/` (GeoJSON + Parquet). Then either:

- **Option A (simplest):** Keep one dataset per region; frontend or router chooses region (e.g. by URL or viewport).
- **Option B (single globe):** Merge all regional Parquet files into one set (e.g. `ski_areas.parquet`, `lifts.parquet`, … for the world) and feed that into the **serving** step below.

---

## 2. Pipeline: already built for scale

Current design is ready for large inputs:

- **One merged bbox extract** per run → one PBF read per region, not per ski area.
- **Batch enrich** → boundaries and ski-area index loaded once; no per-feature shapefile reads.
- **Parquet output** → compact storage and columnar reads.

For world you will:

- Run the pipeline **once per continent** (or per Geofabrik region), then optionally merge Parquet.
- Expect **hours per continent** (see RUN_TIME_AND_NA_ESTIMATE.md); planet = sum of continents or one very long run.
- Consider **parallel runs** (e.g. one container/job per continent) and merge Parquet at the end.

No fundamental pipeline redesign needed; scale is about how you chunk the planet and how you **serve** the result.

---

## 3. Serving: why “dump full GeoJSON” does not scale

Sending the **entire** world dataset as GeoJSON to the browser:

- Blows up memory and network (hundreds of MB to GB).
- Makes the map slow and the tab prone to crashes.

So at world scale you **do not** serve one big GeoJSON for the whole globe. You either:

- **Send only what’s in view** (viewport/bbox API), or  
- **Serve vector tiles** (only tiles for the current viewport are loaded).

Both are in scope; tiles are the target for “entire world, smooth map.”

---

## 4. Serving strategy A: Viewport / bbox API (stepping stone)

**Idea:** Client sends the current map bbox; server returns GeoJSON only for features that intersect that bbox.

- **Backend:** `GET /api/ski-areas?bbox=minLon,minLat,maxLon,maxLat` (and same for lifts, pistes, osm-nearby). Server reads Parquet (or uses a spatial index), filters by bbox, returns GeoJSON.
- **Frontend:** On load and on `moveend`, request data for `map.getBounds()` and update the GeoJSON source.

**Pros:** Same GeoJSON/Leaflet (or MapLibre) stack; only visible data is transferred.  
**Cons:** Server may still read a lot of Parquet per request unless you add a spatial index or spatial DB. Good for **regional** world (e.g. one continent) or as an intermediate step before tiles.

**Implemented:** All layer endpoints accept optional `?bbox=minLon,minLat,maxLon,maxLat`. The server returns only features whose geometry intersects that bbox (e.g. viewport). Use from the frontend on `moveend`: `fetch(\`/api/ski-areas?bbox=${bounds.toBBoxString()}\`)`. For true planet scale, combine with vector tiles or a spatial backend so the server doesn’t load full Parquet on every request.

---

## 5. Serving strategy B: Vector tiles (target for entire world)

**Idea:** Prebuild **vector tiles** (e.g. PMTiles) from your global Parquet/GeoJSON. The map requests only the tiles that cover the current viewport (e.g. zoom 0–14). No single “full globe” GeoJSON response.

**Steps:**

1. **Produce global GeoJSON (or keep Parquet)**  
   From merged continental runs: one `ski_areas`, one `lifts`, one `pistes`, one `osm_nearby` (or equivalent Parquet).

2. **Build vector tiles**  
   - [tippecanoe](https://github.com/felt/tippecanoe): GeoJSON → vector tiles (e.g. `.mbtiles` or direct to directory).  
   - [PMTiles](https://github.com/protomaps/PMTiles): single-file tile archive, good for hosting (e.g. S3, Cloudflare R2) and no tile server needed.  
   - Example:  
     `tippecanoe -o ski_areas.pmtiles -z14 -L ski_areas:ski_areas.geojson -L lifts:lifts.geojson …`

3. **Serve tiles**  
   - Host the PMTiles file (or a directory of tiles) on a CDN or static host.  
   - Or use a small server that responds to `/tiles/z/x/y.pbf` from PMTiles (e.g. `pmtiles serve` or your own handler).

4. **Frontend**  
   - **MapLibre GL** (or Leaflet with a vector-tile layer): add a **vector source** pointing at your tiles (e.g. `pmtiles://` or `https://.../tiles/{z}/{x}/{y}.pbf`).  
   - No “load whole GeoJSON” step; the client only fetches tiles for the current view and zoom.

**Pros:** Scales to billions of features; smooth pan/zoom; industry standard for world-scale web maps.  
**Cons:** Requires a one-off (or periodic) tile build step and a hosting story for the tile file(s).

---

## 6. Frontend evolution

| Stage | Data size | Serving | Frontend |
|-------|-----------|---------|----------|
| **Now** | Iceland / one region | Full GeoJSON or full Parquet → GeoJSON | Leaflet + GeoJSON source |
| **Next** | One continent | Parquet + **viewport bbox API** (optional ?bbox=) | Leaflet or MapLibre; request by bounds |
| **World** | Planet / all continents | **Vector tiles** (PMTiles) from global Parquet/GeoJSON | **MapLibre** + vector source |

MapLibre is the right long-term choice for a global map: it’s built for vector tiles and large datasets. Leaflet can still work with vector tiles via a plugin, but MapLibre gives the best performance and control.

---

## 7. Checklist: from “one region” to “entire world”

- [ ] Run pipeline for each continent (or desired regions); merge or keep per-region Parquet.
- [ ] Add **viewport bbox** support to API and frontend (request data only for current view).
- [ ] (Optional) Add DuckDB spatial or PostGIS so bbox queries don’t load full Parquet into memory.
- [ ] **Tile pipeline:** GeoJSON/Parquet → tippecanoe → PMTiles (or MBTiles); host the result.
- [ ] **Frontend:** MapLibre + vector source for tiles; retire “load entire GeoJSON” for world view.
- [ ] (Optional) Keep a “full GeoJSON” or “bbox GeoJSON” API for small regions or exports.

---

## 8. Cost estimate: monthly tile build + S3 hosting (AWS)

Ballpark for **build vector tiles once a month** (world scale) and **host in S3** (US regions, 2024–25 list pricing). Exact numbers depend on data size and build time.

| Item | Assumption | Rough monthly cost |
|------|------------|--------------------|
| **Build compute** | Run tippecanoe (or similar) once/month. Option A: **EC2** (e.g. c5.xlarge) for 2–4 hours. Option B: **AWS Batch** or a small always-on instance. | **$2–8** (e.g. 4 hr × $0.17/hr ≈ $0.70; add buffer for larger instance or longer run). Spot/right-sizing can cut this. |
| **S3 storage** | One (or a few) PMTiles files; world ski areas + lifts + pistes + osm_near ≈ **1–5 GB**. | **$0.02–0.12** (≈ $0.023/GB). |
| **S3 requests** | One PUT per month for the new file; GETs for tile serving (depends on traffic). | **&lt; $1** for light/moderate traffic. |
| **Data transfer (egress)** | Users download only tiles in view (small). First 100 GB out to internet ≈ $0.09/GB. | **$0–5** for small/medium traffic; scales with users. |

**Total (hosting + monthly rebuild): about $3–15/month**, with the low end if you use a short-lived EC2 or Batch for the build and keep the PMTiles file small (e.g. 1–2 GB).

**Ways to reduce cost**

- Use **Spot** (or Batch Spot) for the monthly build.
- Build **outside AWS** (e.g. your laptop or a small VPS) and only upload the PMTiles to S3; then you pay mainly S3 storage + egress.
- Put **CloudFront** in front of S3 for caching; can reduce origin egress and improve latency (adds a few dollars unless you’re in the free tier).

---

## 9. Summary

- **Data:** Planet = multiple continental PBF runs (or one planet run); merge Parquet if you want one global dataset.
- **Pipeline:** Already scalable (single extract, batch enrich, Parquet); run per region, then merge.
- **Serving:** At world scale, **do not** send the whole globe as one GeoJSON. Use a **viewport/bbox API** for a stepping stone and **vector tiles (e.g. PMTiles)** for production.
- **Frontend:** Move to **MapLibre** and a **vector tile source** for the “entire world” experience.

This roadmap gets you from “Iceland works” to “the whole world works” without redesigning the pipeline—only how you serve and consume the data.
