$ErrorActionPreference = "Continue"
Set-Location "c:\Users\jwitc\Documents\GitHub\globalskiatlas_data"
$ids = @(
  "vogel_slovenia",
  "skigebiet_oberwiesenthal_germany",
  "glenshee_ski_center_united_kingdom",
  "levin_hiihtokeskus_finland",
  "ski_telg_rt_slovakia",
  "cerro_perito_moreno_argentina",
  "brezovica_brezovic_kosovo",
  "kvitfjell_norway",
  "perisher_australia",
  "pal_arinsal_andorra"
)
$log = "c:\Users\jwitc\Documents\GitHub\globalskiatlas_data\output\game_scenes\batch-export.log"
"=== batch start $(Get-Date -Format o) ===" | Out-File $log
foreach ($id in $ids) {
  "=== $id $(Get-Date -Format o) ===" | Tee-Object -FilePath $log -Append
  docker compose -f docker-compose.game-export.yml run --rm game-export --resort $id --fetch-skadi --data-root /data --cache-dir /cache 2>&1 | Tee-Object -FilePath $log -Append
  "exit $LASTEXITCODE $id" | Tee-Object -FilePath $log -Append
}
"=== batch done $(Get-Date -Format o) ===" | Tee-Object -FilePath $log -Append
