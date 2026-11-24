# Pipeline completo de FiscalGuard con cronometro, para pruebas de volumen.
# Ejecutar desde la raiz del proyecto, con Docker arriba (docker compose up -d):
#   powershell -ExecutionPolicy Bypass -File .\benchmark.ps1 -Name v2_5m -Cfdi 5000000 -Contribuyentes 100000 -Models
#   powershell -ExecutionPolicy Bypass -File .\benchmark.ps1 -Name v2_20m -Cfdi 20000000 -Contribuyentes 250000
#   powershell -ExecutionPolicy Bypass -File .\benchmark.ps1 -Name v2_50m -Cfdi 50000000 -Contribuyentes 500000 -ExecMem 2g
# Cada escala usa su propia ruta en HDFS (/fiscalguard_<Name>) y su carpeta data\<Name>.
# Resultado: benchmark_<Name>.csv con los segundos de cada paso, y log_<Name>_<job>.txt con la salida completa.
param(
  [Parameter(Mandatory = $true)][string]$Name,
  [long]$Cfdi = 5000000,
  [int]$Contribuyentes = 100000,
  [string]$ExecMem = "1g",
  [switch]$Models,
  [switch]$SkipGenerate,
  [switch]$SkipLoad
)
$ErrorActionPreference = "Continue"
$hdfs = "/fiscalguard_$Name"
$base = "hdfs://namenode:9000$hdfs"
$csv = "benchmark_$Name.csv"
"paso,segundos" | Set-Content -Path $csv -Encoding ascii
$global:fallo = $false

function Medir([string]$paso, [scriptblock]$bloque) {
  if ($global:fallo) { return }
  Write-Host ""
  Write-Host ">> $paso" -ForegroundColor Cyan
  $t = Measure-Command { & $bloque | Out-Host }
  if ($LASTEXITCODE -ne 0) { Write-Host "   FALLO (codigo $LASTEXITCODE). Revisa el log." -ForegroundColor Red; $global:fallo = $true; return }
  $seg = [math]::Round($t.TotalSeconds, 1)
  Write-Host ("   {0} s  ({1:N1} min)" -f $seg, ($seg / 60)) -ForegroundColor Green
  "$paso,$seg" | Add-Content -Path $csv -Encoding ascii
}

function Spark([string]$job, [string[]]$extra = @()) {
  $a = @("compose", "exec", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
         "--driver-memory", "2g", "--executor-memory", $ExecMem, "/opt/jobs/$job", $base) + $extra
  & docker @a 2>&1 | Tee-Object -FilePath "log_${Name}_$job.txt" | Select-String -NotMatch "INFO|WARN"
}

if (-not $SkipGenerate) {
  Medir "1_generar_datos" {
    python data_generator\generate_data.py --contribuyentes $Contribuyentes --cfdi $Cfdi --out "data\$Name"
  }
}
if (-not $SkipLoad) {
  Medir "2_cargar_a_hdfs" {
    docker compose exec -T namenode bash -c "hdfs dfs -rm -r -f -skipTrash $hdfs > /dev/null 2>&1; hdfs dfs -mkdir -p $hdfs/raw/cfdi $hdfs/raw/padron $hdfs/raw/lista_69b $hdfs/curated $hdfs/analytics $hdfs/models $hdfs/ground_truth && hdfs dfs -put -f /data/$Name/raw/cfdi/* $hdfs/raw/cfdi/ && hdfs dfs -put -f /data/$Name/raw/padron/* $hdfs/raw/padron/ && hdfs dfs -put -f /data/$Name/raw/lista_69b/* $hdfs/raw/lista_69b/ && hdfs dfs -put -f /data/$Name/ground_truth/* $hdfs/ground_truth/ && hdfs dfs -du -h $hdfs/raw" 2>&1 | Select-String -NotMatch "INFO|WARN"
  }
}
Medir "3_etl_raw_a_curated" { Spark "01_etl_curated.py" }
Medir "4_features_por_contribuyente" { Spark "02_features.py" }
if ($Models) {
  Medir "5_clasificador_sin_cv" { Spark "03_train_classifier.py" @("--no-cv") }
  Medir "6_kmeans" { Spark "04_kmeans_anomalies.py" }
  Medir "7_grafos_ciclos" { Spark "05_graph_cycles.py" @("--max-len", "6") }
}

Write-Host ""
Write-Host "================ RESUMEN ($Name) ================" -ForegroundColor Yellow
Import-Csv $csv | Format-Table -AutoSize
$tot = (Import-Csv $csv | Measure-Object -Property segundos -Sum).Sum
$etl = (Import-Csv $csv | Where-Object { $_.paso -like "3_*" -or $_.paso -like "4_*" } | Measure-Object -Property segundos -Sum).Sum
Write-Host ("Total: {0:N1} min | ETL + features (lo que mide la meta de 50 M en < 2 h): {1:N1} min" -f ($tot / 60), ($etl / 60))
Write-Host "CFDI en curated:"; docker compose exec -T namenode hdfs dfs -du -s -h $hdfs/curated/cfdi 2>&1 | Select-String -NotMatch "INFO|WARN"
if ($global:fallo) { Write-Host "El pipeline se detuvo por un error. Ver log_${Name}_*.txt" -ForegroundColor Red }
