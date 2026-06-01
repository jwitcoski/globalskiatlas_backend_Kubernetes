@echo off
setlocal
cd /d "%~dp0..\.."

set QGIS_ROOT=
for /d %%D in ("C:\Program Files\QGIS*") do (
    if exist "%%D\bin\python-qgis-ltr.bat" set QGIS_ROOT=%%D
    if exist "%%D\bin\python-qgis.bat" if not defined QGIS_ROOT set QGIS_ROOT=%%D
)
if not defined QGIS_ROOT (
    echo ERROR: QGIS not found.
    exit /b 1
)

if exist "%QGIS_ROOT%\bin\python-qgis-ltr.bat" (
    call "%QGIS_ROOT%\bin\python-qgis-ltr.bat" -m atlas.map_gen.build_overview_qgz %*
) else (
    call "%QGIS_ROOT%\bin\python-qgis.bat" -m atlas.map_gen.build_overview_qgz %*
)
endlocal & exit /b %ERRORLEVEL%
