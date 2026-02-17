# Run pipeline for all 6 regions locally, then combine.
# Regions: North America, South America, Africa, Europe, Australia/Oceania, Asia
# Usage: .\run_all_regions.ps1
# Or run specific regions: .\run_all_regions.ps1 -Regions south-america,africa

param(
    [string[]]$Regions = @("north-america", "south-america", "africa", "europe", "australia-oceania", "asia"),
    [switch]$SkipCombine
)

$ErrorActionPreference = "Stop"
$composeFiles = @{
    "north-america"     = "docker-compose.north-america.yml"
    "south-america"     = "docker-compose.south-america.yml"
    "africa"            = "docker-compose.africa.yml"
    "europe"            = "docker-compose.europe.yml"
    "australia-oceania" = "docker-compose.australia-oceania.yml"
    "asia"              = "docker-compose.asia.yml"
}

Write-Host "=== Ski Atlas Local Pipeline ===" -ForegroundColor Cyan
Write-Host "Regions: $($Regions -join ', ')"
Write-Host ""

foreach ($region in $Regions) {
    $compose = $composeFiles[$region]
    if (-not $compose) {
        Write-Host "Unknown region: $region (skipping)" -ForegroundColor Yellow
        continue
    }
    Write-Host "--- $region ---" -ForegroundColor Green
    docker compose -f $compose up --build --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Pipeline failed for $region" -ForegroundColor Red
        exit 1
    }
}

if (-not $SkipCombine) {
    Write-Host ""
    Write-Host "--- Combining regions ---" -ForegroundColor Green
    python scripts/combine_regions.py -o output -r $Regions
}

Write-Host ""
Write-Host "Done. Output in output/<region>/ and output/combined/" -ForegroundColor Cyan
