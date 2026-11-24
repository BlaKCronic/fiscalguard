# FiscalGuard: corre el pipeline batch completo (jobs 01 a 06) en orden y mide cuánto tarda cada uno.
# Es lo mismo que hace el DAG de Airflow, para correrlo a mano. Desde la raíz del proyecto:
#     .\scripts\pipeline.ps1                  todo, con validación cruzada en el clasificador
#     .\scripts\pipeline.ps1 -SinCV           más rápido (sin validación cruzada)
#     .\scripts\pipeline.ps1 -Desde 03        retoma desde el job 03
#     .\scripts\pipeline.ps1 -Solo 06         solo un job
# Requisitos: clúster arriba, raw cargada en HDFS y MONGO_URI en el archivo .env de la raíz.
# Resultados: logs\pipeline_<fecha>\NN.log  y  data\resultados\tiempos_pipeline.csv
param(
  [string]$Desde = '01',
  [string]$Solo = '',
  [switch]$SinCV
)
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
$base = 'hdfs://namenode:9000/fiscalguard'

# ---------- MONGO_URI desde .env ----------
$uri = $env:MONGO_URI
if (-not $uri -and (Test-Path '.env')) {
  $l = Get-Content '.env' | Where-Object { $_ -match '^\s*MONGO_URI\s*=' } | Select-Object -First 1
  if ($l) { $uri = ($l -replace '^\s*MONGO_URI\s*=\s*', '').Trim().Trim('"').Trim("'") }
}

$jobs = [ordered]@{
  '01' = @{ f = '01_etl_curated.py';       n = 'ETL raw -> curated (Parquet)';      a = @() }
  '02' = @{ f = '02_features.py';          n = 'Features por contribuyente';        a = @() }
  '03' = @{ f = '03_train_classifier.py';  n = 'Clasificador RF/GBT';               a = @(if ($SinCV) { '--no-cv' }) }
  '04' = @{ f = '04_kmeans_anomalies.py';  n = 'K-Means (anomalías)';               a = @() }
  '05' = @{ f = '05_graph_cycles.py';      n = 'Ciclos de facturación circular';    a = @() }
  '06' = @{ f = '06_export_mongo.py';      n = 'Exportación a MongoDB';             a = @() }
}
$sel = if ($Solo) { @($Solo) } else { $jobs.Keys | Where-Object { $_ -ge $Desde } }
if ($sel -contains '06' -and -not $uri) {
  Write-Host 'ERROR: falta MONGO_URI (en .env o como variable de entorno) para el job 06.' -ForegroundColor Red; exit 1
}

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logDir = "logs\pipeline_$stamp"
New-Item -ItemType Directory -Force -Path $logDir, 'data\resultados' | Out-Null

# pymongo en el contenedor del driver (se pierde si el contenedor se recrea)
if ($sel -contains '06') {
  Write-Host 'Verificando pymongo en spark-master...'
  docker compose exec -T -u root spark-master bash -c 'pip show pymongo >/dev/null 2>&1 || pip install -q pymongo[srv] || pip install -q --break-system-packages pymongo[srv]'
}

$tiempos = @()
$t0 = Get-Date
foreach ($k in $sel) {
  $j = $jobs[$k]
  Write-Host "`n==== Job $k  $($j.n) ====" -ForegroundColor Cyan
  $cmd = @('compose', 'exec', '-T')
  if ($k -eq '06') { $cmd += @('-e', "MONGO_URI=$uri") }
  $cmd += @('spark-master', '/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077',
            '--driver-memory', '2g', "/opt/jobs/$($j.f)", $base) + $j.a
  $sw = [Diagnostics.Stopwatch]::StartNew()
  & docker @cmd 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath "$logDir\$k.log"
  $rc = $LASTEXITCODE
  $sw.Stop()
  $seg = [math]::Round($sw.Elapsed.TotalSeconds, 1)
  $tiempos += [pscustomobject]@{ fecha = $stamp; job = $k; nombre = $j.n; segundos = $seg; ok = ($rc -eq 0) }
  if ($rc -ne 0) {
    Write-Host "Job $k FALLÓ (código $rc). Log: $logDir\$k.log" -ForegroundColor Red
    break
  }
  Write-Host "Job $k listo en $seg s" -ForegroundColor Green
}
$tiempos | Export-Csv 'data\resultados\tiempos_pipeline.csv' -NoTypeInformation -Append -Encoding UTF8
Write-Host "`n==== Tiempos ====" -ForegroundColor Cyan
$tiempos | Format-Table job, nombre, segundos, ok -AutoSize
Write-Host ("Total: {0:N1} min   (logs en {1})" -f ((Get-Date) - $t0).TotalMinutes, $logDir)
