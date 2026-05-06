@echo off
setlocal enabledelayedexpansion

:: ============================================================
:: run_export_layouts.bat
:: Exports every atlas_work QGZ as a PNG using the QGIS layout
:: engine.  Finds the QGIS installation automatically.
::
:: Usage:  atlas\map_gen\run_export_layouts.bat [--dpi 150] [--overwrite]
:: ============================================================

:: -- Find QGIS installation ---------------------------------
set QGIS_ROOT=

:: Check common standalone installer paths
for %%V in (3.44 3.40 3.38 3.36 3.34 3.32 3.28) do (
    for %%P in ("C:\Program Files\QGIS %%V" "C:\Program Files\QGIS %%V LTR") do (
        if exist %%P\bin\qgis_process.exe (
            set QGIS_ROOT=%%~P
            goto :found
        )
        if exist "%%P\bin\qgis-ltr-bin.exe" (
            set QGIS_ROOT=%%~P
            goto :found
        )
        if exist "%%P\bin\qgis-bin.exe" (
            set QGIS_ROOT=%%~P
            goto :found
        )
    )
)
:: Wildcard scan of Program Files
for /d %%D in ("C:\Program Files\QGIS*") do (
    if exist "%%D\bin\qgis_process.exe" (
        set QGIS_ROOT=%%D
        goto :found
    )
    if exist "%%D\bin\qgis-ltr-bin.exe" (
        set QGIS_ROOT=%%D
        goto :found
    )
    if exist "%%D\bin\qgis-bin.exe" (
        set QGIS_ROOT=%%D
        goto :found
    )
)
:: OSGeo4W
for %%O in (C:\OSGeo4W64 C:\OSGeo4W) do (
    if exist %%O\bin\qgis_process.exe (
        set QGIS_ROOT=%%O
        goto :found
    )
    if exist %%O\bin\qgis-ltr-bin.exe (
        set QGIS_ROOT=%%O
        goto :found
    )
    if exist %%O\bin\qgis-bin.exe (
        set QGIS_ROOT=%%O
        goto :found
    )
)

echo ERROR: QGIS not found in Program Files or OSGeo4W.
echo Set QGIS_ROOT manually at the top of this script.
exit /b 1

:found
echo Using QGIS at: %QGIS_ROOT%

:: Repo root (script paths are relative to here)
cd /d "%~dp0..\.."
set "QT_QPA_PLATFORM=offscreen"

:: Prefer python-qgis.bat — loads o4w_env + Qt/GDAL DLL paths so PyQGIS imports work on Windows.
if exist "%QGIS_ROOT%\bin\python-qgis.bat" (
    echo Using Python-QGIS wrapper: %QGIS_ROOT%\bin\python-qgis.bat
    call "%QGIS_ROOT%\bin\python-qgis.bat" atlas\map_gen\export_layouts.py %*
    endlocal & exit /b %ERRORLEVEL%
)

:: Fallback: manual env (minimal installs without python-qgis.bat)
if exist "%QGIS_ROOT%\bin\o4w_env.bat" (
    call "%QGIS_ROOT%\bin\o4w_env.bat"
) else (
    set "PATH=%QGIS_ROOT%\bin;%QGIS_ROOT%\apps\qgis\bin;%PATH%"
    set "PYTHONPATH=%QGIS_ROOT%\apps\qgis\python;%QGIS_ROOT%\apps\qgis\python\plugins;%PYTHONPATH%"
    set "QGIS_PREFIX_PATH=%QGIS_ROOT%\apps\qgis"
    set "QT_PLUGIN_PATH=%QGIS_ROOT%\apps\qt5\plugins"
)

set PYTHON=
if exist "%QGIS_ROOT%\apps\Python313\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python313\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\apps\Python312\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python312\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\apps\Python311\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python311\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\apps\Python310\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python310\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\apps\Python39\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python39\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\apps\Python38\python.exe" set PYTHON=%QGIS_ROOT%\apps\Python38\python.exe
if "%PYTHON%"=="" if exist "%QGIS_ROOT%\bin\python3.exe" set PYTHON=%QGIS_ROOT%\bin\python3.exe
if "%PYTHON%"=="" set PYTHON=python

echo Using Python: %PYTHON%
"%PYTHON%" atlas\map_gen\export_layouts.py %*

endlocal
