# FiscalGuard: crea las zonas del Data Lake y sube la zona raw (y la verdad terreno) a HDFS.
# Equivale a scripts/load_to_hdfs.sh, pero en PowerShell nativo. Desde la raíz del proyecto:
#     .\scripts\load_to_hdfs.ps1
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host 'Esperando a que HDFS salga de safe mode...'
docker compose exec -T namenode hdfs dfsadmin -safemode wait
$rep = docker compose exec -T namenode hdfs dfsadmin -report 2>$null
$vivos = ($rep | Select-String 'Live datanodes \((\d+)\)').Matches
if (-not $vivos -or [int]$vivos[0].Groups[1].Value -lt 1) {
  Write-Host 'ERROR: no hay datanodes vivos. Espera 1 minuto o revisa: docker compose logs datanode1 --tail 40' -ForegroundColor Red
  exit 1
}
Write-Host "Datanodes vivos: $($vivos[0].Groups[1].Value)"

docker compose exec -T namenode bash -c 'hdfs dfs -mkdir -p /fiscalguard/raw/cfdi /fiscalguard/raw/padron /fiscalguard/raw/lista_69b /fiscalguard/raw/catalogos /fiscalguard/curated /fiscalguard/analytics /fiscalguard/models /fiscalguard/ground_truth'
foreach ($z in 'cfdi', 'padron', 'lista_69b') {
  Write-Host "Subiendo raw/$z ..."
  docker compose exec -T namenode bash -c "hdfs dfs -put -f /data/raw/$z/* /fiscalguard/raw/$z/"
  if ($LASTEXITCODE -ne 0) { Write-Host "ERROR al subir raw/$z" -ForegroundColor Red; exit 1 }
}
if (Test-Path 'data\ground_truth') {
  Write-Host 'Subiendo ground_truth (solo para evaluar los modelos, nunca para entrenar)...'
  docker compose exec -T namenode bash -c 'hdfs dfs -put -f /data/ground_truth/* /fiscalguard/ground_truth/'
}
docker compose exec -T namenode hdfs dfs -du -h /fiscalguard/raw /fiscalguard/ground_truth
