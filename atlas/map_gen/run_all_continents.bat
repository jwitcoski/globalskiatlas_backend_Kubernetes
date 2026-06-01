@echo off
setlocal enabledelayedexpansion

:: Generate + export all resorts in output/combined, one continent at a time.
:: Smallest continents first; europe last (~2665 resorts, longest).
::
:: Usage:
::   atlas\map_gen\run_all_continents.bat
::   atlas\map_gen\run_all_continents.bat --upload-only
::   atlas\map_gen\run_all_continents.bat --generate-only
::
:: Resume a single continent:
::   atlas\map_gen\run_resort_maps_pipeline.bat --all-resorts --region europe --input-dir output/combined --generate-only
::
:: Requires QGIS (same as run_resort_maps_pipeline.bat).

cd /d "%~dp0..\.."
if not exist logs mkdir logs

for /f "tokens=1-3 delims=/ " %%a in ('echo %date% %time%') do set STAMP=%%c-%%a-%%b
set STAMP=%STAMP: =0%
set STAMP=%STAMP::=-%
set "LOG=logs\resort_maps_all_continents_%STAMP%.log"

echo Logging to %LOG%
echo Started %date% %time% > "%LOG%"

set EXTRA=%*
if "%EXTRA%"=="" set EXTRA=--generate-only

for %%C in (
  africa
  south-america
  australia-oceania
  asia
  north-america
  europe
) do (
  echo.>> "%LOG%"
  echo ===== %%C ===== %date% %time%>> "%LOG%"
  echo.
  echo ===== %%C =====
  call atlas\map_gen\run_resort_maps_pipeline.bat --all-resorts --region %%C --input-dir output/combined %EXTRA% >> "%LOG%" 2>&1
  if errorlevel 1 (
    echo ERROR: %%C failed with exit !ERRORLEVEL!>> "%LOG%"
    echo ERROR: %%C failed — see %LOG%
    exit /b !ERRORLEVEL!
  )
)

echo.>> "%LOG%"
echo Finished all continents %date% %time%>> "%LOG%"
echo Done. Log: %LOG%
endlocal
