# Run the full world pipeline locally (all regions by continent/country/state).
# Uses run_region_local.py + Docker. Run overnight; use -PreventSleep to keep PC awake.
#
# Prereqs: Docker running, image built: docker build -f Dockerfile.aws -t globalskiatlas-pipeline .
#
# Run in PowerShell (not cmd):
#   powershell -ExecutionPolicy Bypass -File .\scripts\run_all_regions_local.ps1 -PreventSleep
# Or from PowerShell:
#   .\scripts\run_all_regions_local.ps1 -PreventSleep
# Optional (only some continents):
#   .\scripts\run_all_regions_local.ps1 -PreventSleep -Continents europe,north_america

param(
    [switch]$PreventSleep,
    [string[]]$Continents = @("africa", "south-america", "australia-oceania", "north_america", "europe", "asia")
)

$ErrorActionPreference = "Stop"

# Resolve paths (works when run from any location)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $scriptDir) { $scriptDir = ".\scripts" }
$repoRoot = Split-Path -Parent $scriptDir
if (-not $repoRoot) { $repoRoot = (Get-Location).Path }

Write-Host "Starting world pipeline script..." -ForegroundColor Gray
Push-Location $repoRoot
try {
    Write-Host "=== Ski Atlas World Pipeline (local, all regions) ===" -ForegroundColor Cyan
    Write-Host "Continents: $($Continents -join ', ')"
    Write-Host "Output: output\<continent>\ and output\<continent>\<slug>\"
    Write-Host ""

    if ($PreventSleep) {
        Write-Host "Preventing system sleep until script exits (display may still turn off)..." -ForegroundColor Yellow
        $code = @"
[DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
public static extern void SetThreadExecutionState(uint esFlags);
"@
        $ES_CONTINUOUS = [uint32]"0x80000000"
        $ES_SYSTEM_REQUIRED = [uint32]"0x00000001"
        $ES_DISPLAY_REQUIRED = [uint32]"0x00000002"
        try {
            $util = Add-Type -MemberDefinition $code -Name "Util" -Namespace "Win32" -PassThru
            $util::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED -bor $ES_DISPLAY_REQUIRED)
        } catch {
            Write-Host "Could not set execution state. Keep PC awake manually: Settings > System > Power > Screen and sleep = Never, or run: powercfg /change standby-timeout-ac 0" -ForegroundColor Yellow
        }
    }

    foreach ($c in $Continents) {
        Write-Host ""
        Write-Host "--- Continent: $c ---" -ForegroundColor Green
        python scripts/run_region_local.py --continent $c --no-prune
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Pipeline failed for continent: $c" -ForegroundColor Red
            exit 1
        }
    }

    Write-Host ""
    Write-Host "--- Combining regions ---" -ForegroundColor Green
    python scripts/combine_regions.py -o output
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Combine failed (optional)." -ForegroundColor Yellow
    }

    # Optional: register combined output as Iceberg tables (Glue + S3). Set REGISTER_ICEBERG=1 and S3_BUCKET.
    if ($env:REGISTER_ICEBERG -eq "1" -and $env:S3_BUCKET) {
        Write-Host ""
        Write-Host "--- Register Iceberg tables ---" -ForegroundColor Green
        python scripts/register_iceberg.py --s3-bucket $env:S3_BUCKET --input-dir output/combined
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Iceberg registration failed (optional)." -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "Done. Output in output\ and output\combined\" -ForegroundColor Cyan
} catch {
    Write-Host "ERROR: $_" -ForegroundColor Red
    exit 1
} finally {
    Pop-Location
}
