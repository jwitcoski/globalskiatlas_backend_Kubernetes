# Mapper Workflow — Download, Edit, Export, Upload

This guide is for mappers who produce trail map images from the atlas data: download a resort’s QGIS project from the work bucket, edit in QGIS, export to PNG/PDF, and upload to the final bucket.

---

## Prerequisites

- **QGIS 3.x** installed.
- **AWS CLI** configured with read access to the **work** bucket and write access to the **final** bucket (or use an S3 browser with the same permissions).
- Bucket names and prefixes are in [config/atlas.yaml](../config/atlas.yaml) under `s3.work_bucket`, `s3.work_prefix`, `s3.final_bucket`, `s3.final_prefix`. Defaults:
  - Work: `globalskiatlas-atlas-work` (prefix `work`)
  - Final: `globalskiatlas-atlas-images` (prefix `final`)

---

## 1. Download

Download one resort (or a whole region) from the work bucket.

**One resort** (replace `<region>` and `<winter_sports_id>` with the resort’s path, e.g. `europe/iceland` and `12345`):

```powershell
aws s3 sync "s3://<work-bucket>/<work-prefix>/resorts/<region>/<winter_sports_id>" "./local/<region>/<winter_sports_id>"
```

Example:

```powershell
aws s3 sync "s3://globalskiatlas-atlas-work/work/resorts/europe/iceland/12345" "./local/europe/iceland/12345"
```

**A full region**:

```powershell
aws s3 sync "s3://globalskiatlas-atlas-work/work/resorts/europe/iceland" "./local/europe/iceland"
```

---

## 2. Open in QGIS

1. Open the folder you synced (e.g. `./local/europe/iceland/12345/`).
2. Open the QGIS project file:
   - `resort_<region>_<id>.qgz` (or `.qgs` if the template was saved as uncompressed).
3. Confirm that layers load:
   - **Ski Resort Boundary** — one polygon.
   - **OSM Polygons Clipped** / **OSM Lines Clipped** / **OSM Points Clipped** — OSM features clipped to the resort.
   - **Clipped Contours** — contour lines.
4. Check the map extent and CRS (e.g. EPSG:4326). Adjust the map view if needed.

---

## 3. Edit

- Adjust **symbology**, **labels**, **scale**, or **layout** (page size) as needed.
- Page sizes (small/medium/large/mega) are defined in [config/atlas.yaml](../config/atlas.yaml); use the layout that matches the desired size.
- Save the project (Ctrl+S).

---

## 4. Export

1. Open the **Print Layout** that matches your target page size.
2. Export the map:
   - **Layout → Export as Image** (PNG) or **Export as PDF**.
3. Use the naming convention so the pipeline can find the file:
   - **PNG**: `map_300dpi.png`
   - **PDF**: `map_300dpi.pdf`
   - Resolution: **300 DPI** (or as set in `config/atlas.yaml` under `dpi`).
4. Save the file in the same folder as the project (or note where you saved it for the upload step).

---

## 5. Upload

Upload the exported image(s) to the **final** bucket under the same path structure as the work bucket.

**One file** (from the folder where you exported):

```powershell
aws s3 cp "./local/europe/iceland/12345/map_300dpi.png" "s3://<final-bucket>/<final-prefix>/<region>/<winter_sports_id>/map_300dpi.png"
```

Example:

```powershell
aws s3 cp "./local/europe/iceland/12345/map_300dpi.png" "s3://globalskiatlas-atlas-images/final/europe/iceland/12345/map_300dpi.png"
```

**PDF as well**:

```powershell
aws s3 cp "./local/europe/iceland/12345/map_300dpi.pdf" "s3://globalskiatlas-atlas-images/final/europe/iceland/12345/map_300dpi.pdf"
```

Updating DynamoDB with the final map URL (e.g. `map_image_s3_key`) is **Phase 5** and will be automated later.

---

## 6. Overview Maps (Country / State)

For **country** or **state** overview maps, the data-to-map script can generate overview projects (optional). The workflow is the same:

1. Download the overview project from the work bucket (e.g. `work/overview/countries/<country>/` or `work/overview/states/<country>/<state>/`).
2. Open in QGIS, edit, export at 300 DPI.
3. Upload to the final bucket under the same path (e.g. `final/countries/<country>/overview.png`, `final/states/<country>/<state>/overview.png`).

---

## Checklist

| Step      | Action |
|----------|--------|
| Download | `aws s3 sync s3://<work-bucket>/<work-prefix>/resorts/<region>/<id> ./local/...` |
| Open     | Open the `.qgz` (or `.qgs`) in QGIS; confirm layers and extent |
| Edit     | Adjust symbology, labels, scale, layout; save project |
| Export   | Export as PNG/PDF at 300 DPI; name `map_300dpi.png` / `map_300dpi.pdf` |
| Upload   | `aws s3 cp ... map_300dpi.png s3://<final-bucket>/<final-prefix>/<region>/<id>/` |

---

## Troubleshooting

- **Missing layers**: Ensure all layer files (e.g. `ski_area.geojson`, `osm_polygons.geojson`, `contours.geojson`) are in the same folder as the project. Re-download from the work bucket if needed.
- **CRS / extent**: The template uses a worldwide CRS (e.g. EPSG:4326). If the map is blank, check that layer CRS matches the project and zoom to layer extent.
- **Fonts**: Use fonts installed on your system; avoid fonts that are not available on other mappers’ machines if you share the project.
- **Bucket names**: If sync or upload fails, confirm bucket names and your IAM permissions (`s3:GetObject`, `s3:ListBucket` on work; `s3:PutObject` on final).
