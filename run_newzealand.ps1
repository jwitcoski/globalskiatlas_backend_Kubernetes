# New Zealand flow: Download → Extract (osmium) → OSM nearby (local PBF) → Parquet
# Same pipeline as Iceland. Usage: .\run_newzealand.ps1

Write-Host "=== Ski Atlas New Zealand ===" -ForegroundColor Cyan
Write-Host "1. Download New Zealand PBF (~350MB)"
Write-Host "2. Extract winter_sports with osmium"
Write-Host "3. Extract OSM data near ski areas from PBF (local)"
Write-Host "4. Extract lifts and pistes (output/lifts.geojson, output/pistes.geojson)"
Write-Host "5. Enrich GeoJSON with State, Country, Ski Area"
Write-Host "6. Analyze ski areas (area, lifts, trails)"
Write-Host "7. Export to Parquet; remove large GeoJSON/JSON (keep parquet + ski_areas_analyzed.csv)"
Write-Host ""

if (-not (Test-Path output)) { New-Item -ItemType Directory -Path output | Out-Null }

$sw = [System.Diagnostics.Stopwatch]::StartNew()
try {
    docker compose -f docker-compose.newzealand.yml up --build --remove-orphans
} finally {
    $sw.Stop()
    $ts = $sw.Elapsed
    Write-Host ""
    Write-Host ("Total run time: {0:N0}m {1:N0}s" -f $ts.TotalMinutes, $ts.Seconds) -ForegroundColor Cyan
}
