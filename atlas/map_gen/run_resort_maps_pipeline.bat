@echo off
setlocal enabledelayedexpansion

:: Full resort map pipeline (generate + export + S3 upload).
:: Requires QGIS — same discovery as run_export_layouts.bat.
::
:: Usage:
::   atlas\map_gen\run_resort_maps_pipeline.bat --region north-america/us/virginia --limit 2
::   atlas\map_gen\run_resort_maps_pipeline.bat --upload-only

set QGIS_ROOT=

for %%V in (3.44 3.40 3.38 3.36 3.34 3.32 3.28) do (
    for %%P in ("C:\Program Files\QGIS %%V" "C:\Program Files\QGIS %%V LTR") do (
        if exist %%P\bin\qgis_process.exe (set QGIS_ROOT=%%~P& goto :found)
        if exist "%%P\bin\qgis-ltr-bin.exe" (set QGIS_ROOT=%%~P& goto :found)
        if exist "%%P\bin\qgis-bin.exe" (set QGIS_ROOT=%%~P& goto :found)
    )
)
for /d %%D in ("C:\Program Files\QGIS*") do (
    if exist "%%D\bin\qgis_process.exe" (set QGIS_ROOT=%%D& goto :found)
    if exist "%%D\bin\qgis-ltr-bin.exe" (set QGIS_ROOT=%%D& goto :found)
)
for %%O in (C:\OSGeo4W64 C:\OSGeo4W) do (
    if exist %%O\bin\qgis_process.exe (set QGIS_ROOT=%%O& goto :found)
)

echo ERROR: QGIS not found. Set QGIS_ROOT in this script or install QGIS.
exit /b 1

:found
echo Using QGIS at: %QGIS_ROOT%
cd /d "%~dp0..\.."
set "QT_QPA_PLATFORM=offscreen"
set "PYTHONUNBUFFERED=1"
if exist "C:\Windows\Fonts" set "QT_QPA_FONTDIR=C:\Windows\Fonts"

echo Launching pipeline (QGIS init can take 1-3 minutes before the first resort)...
echo Region must use hyphens, e.g. north-america  not "north america"

if exist "%QGIS_ROOT%\bin\python-qgis.bat" (
    call "%QGIS_ROOT%\bin\python-qgis.bat" atlas\map_gen\run_resort_maps_pipeline.py %*
    exit /b %ERRORLEVEL%
)
if exist "%QGIS_ROOT%\bin\python-qgis-ltr.bat" (
    call "%QGIS_ROOT%\bin\python-qgis-ltr.bat" atlas\map_gen\run_resort_maps_pipeline.py %*
    exit /b %ERRORLEVEL%
)

echo ERROR: python-qgis.bat not found under %QGIS_ROOT%\bin
exit /b 1
