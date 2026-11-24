# FiscalGuard - Fase 5: comparativa MapReduce (YARN) vs Spark sobre la misma zona raw de HDFS.
# Ejecutar desde la raíz del proyecto, en PowerShell:
#     .\scripts\compare_mr_spark.ps1
# Requiere que HDFS ya tenga /fiscalguard/raw/cfdi (scripts\load_to_hdfs.ps1).
# Levanta solo resourcemanager y nodemanager (perfil "yarn"); el resto del clúster debe estar arriba.
param(
  [int]$Reducers = 2,
  [switch]$SinLevantarYarn
)
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
$base = 'hdfs://namenode:9000/fiscalguard'
New-Item -ItemType Directory -Force -Path 'data\resultados' | Out-Null

function Paso($t) { Write-Host "`n==== $t ====" -ForegroundColor Cyan }
function Falla($t) { Write-Host "ERROR: $t" -ForegroundColor Red; exit 1 }

# ---------- 1. YARN ----------
if (-not $SinLevantarYarn) {
  Paso '1/4  Levantando YARN (resourcemanager + nodemanager)'
  docker compose --profile yarn up -d resourcemanager nodemanager
  if ($LASTEXITCODE -ne 0) { Falla 'no se pudo levantar YARN' }
}
Write-Host 'Esperando a que el NodeManager se registre (hasta 3 min)...'
$ok = $false
for ($i = 0; $i -lt 36; $i++) {
  $nodes = docker compose exec -T resourcemanager yarn node -list 2>$null
  if ($nodes -match 'RUNNING') { $ok = $true; break }
  Start-Sleep -Seconds 5
}
if (-not $ok) { Falla 'el NodeManager no aparece como RUNNING. Revisa: docker compose logs nodemanager --tail 40' }
$nodes | Select-String 'RUNNING' | ForEach-Object { Write-Host "  $_" }

# ---------- 2. Compilar el job Java dentro del contenedor ----------
Paso '2/4  Compilando mapreduce\MontoPorEmisor.java'
docker compose exec -T resourcemanager bash -c 'cd /mapreduce && rm -rf build && mkdir -p build && javac -encoding UTF-8 -cp $(hadoop classpath) -d build MontoPorEmisor.java && jar cf montoporemisor.jar -C build . && ls -l montoporemisor.jar'
if ($LASTEXITCODE -ne 0) { Falla 'no compiló el job MapReduce' }

# ---------- 3. MapReduce sobre YARN ----------
Paso '3/4  MapReduce sobre YARN (los logs de Hadoop salen en pantalla)'
docker compose exec -T resourcemanager hdfs dfs -rm -r -f -skipTrash /fiscalguard/analytics/mr_monto_emisor 2>$null | Out-Null
$sw = [Diagnostics.Stopwatch]::StartNew()
$mrOut = docker compose exec -T resourcemanager hadoop jar /mapreduce/montoporemisor.jar MontoPorEmisor /fiscalguard/raw/cfdi /fiscalguard/analytics/mr_monto_emisor $Reducers
$mrExit = $LASTEXITCODE
$sw.Stop()
$mrTotal = [math]::Round($sw.Elapsed.TotalSeconds, 1)
$mrOut | ForEach-Object { Write-Host $_ }
if ($mrExit -ne 0) { Falla 'el job MapReduce falló (revisa http://localhost:8088)' }
$m = $mrOut | Select-String 'MAPREDUCE_SEGUNDOS=([\d.]+)' | Select-Object -First 1
$mrJob = if ($m) { $m.Matches[0].Groups[1].Value -as [double] } else { $mrTotal }

# ---------- 4. Spark (misma lógica) + verificación ----------
Paso '4/4  Spark con la misma lógica y verificación de resultados'
$sw = [Diagnostics.Stopwatch]::StartNew()
$spOut = docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  /opt/jobs/08_spark_vs_mapreduce.py $base --mr-seg $mrJob
$spExit = $LASTEXITCODE
$sw.Stop()
$spTotal = [math]::Round($sw.Elapsed.TotalSeconds, 1)
$spOut | ForEach-Object { Write-Host $_ }
if ($spExit -ne 0) { Falla 'el job Spark falló' }
$m = $spOut | Select-String 'SPARK_SEGUNDOS=([\d.]+)' | Select-Object -First 1
$spJob = if ($m) { $m.Matches[0].Groups[1].Value -as [double] } else { $spTotal }
$iguales = [bool]($spOut | Select-String 'IDÉNTICOS|IDENTICOS')

# ---------- Resumen ----------
$fila = [pscustomobject]@{
  fecha = (Get-Date -Format 's'); mapreduce_job_s = $mrJob; mapreduce_total_s = $mrTotal
  spark_job_s = $spJob; spark_total_s = $spTotal
  aceleracion_job = if ($spJob) { [math]::Round($mrJob / $spJob, 2) } else { $null }
  aceleracion_total = if ($spTotal) { [math]::Round($mrTotal / $spTotal, 2) } else { $null }
  resultados_identicos = $iguales
}
$fila | Export-Csv -Path 'data\resultados\comparativa_tiempos.csv' -NoTypeInformation -Append -Encoding UTF8
Paso 'Resultado'
$fila | Format-List
Write-Host 'job   = desde que el motor empieza a leer hasta que escribe el resultado'
Write-Host 'total = comando completo (incluye arranque de la JVM / SparkContext y la petición a YARN)'
Write-Host 'Guardado en data\resultados\comparativa_tiempos.csv y data\resultados\comparativa_mr_spark.json'
Write-Host "`nPara liberar memoria al terminar:  docker compose --profile yarn stop resourcemanager nodemanager"
