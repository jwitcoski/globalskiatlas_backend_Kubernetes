@echo off
REM Rebuild ski_area_contours.parquet + ski_area_elevation_points.parquet for one region.
REM Requires rasterio in the Python used below (NOT used by atlas/map_gen).
REM
REM Usage:
REM   scripts\rebuild_region_elevation.bat north-america\us\colorado

setlocal
set REGION=%~1
if "%REGION%"=="" (
  echo Usage: scripts\rebuild_region_elevation.bat north-america\us\colorado
  exit /b 1
)

cd /d "%~dp0\.."
set DATA=output\%REGION%
set CACHE=%DATA%\cache

if not exist "%DATA%\ski_areas.parquet" (
  echo Missing %DATA%\ski_areas.parquet
  exit /b 1
)

echo Rebuilding contours and elevation for %REGION% ...
python scripts\ski_area_elevation_contours.py ^
  -i "%DATA%\ski_areas.parquet" ^
  -o "%DATA%" ^
  --cache-dir "%CACHE%" ^
  --boundaries "%DATA%\ski_areas.parquet"

echo Done. Re-run map pipeline with:
echo   atlas\map_gen\run_resort_maps_pipeline.bat --region %REGION% --input-dir %DATA% --generate-only
