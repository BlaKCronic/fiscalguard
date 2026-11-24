#!/usr/bin/env bash
# Crea las zonas del Data Lake y sube la zona raw. Ejecutar desde la raíz del proyecto.
set -euo pipefail
docker compose exec -T namenode bash -c '
  hdfs dfsadmin -safemode wait
  hdfs dfs -mkdir -p /fiscalguard/raw/cfdi /fiscalguard/raw/padron /fiscalguard/raw/lista_69b /fiscalguard/raw/catalogos
  hdfs dfs -mkdir -p /fiscalguard/curated /fiscalguard/analytics /fiscalguard/models
  hdfs dfs -put -f /data/raw/cfdi/*        /fiscalguard/raw/cfdi/
  hdfs dfs -put -f /data/raw/padron/*      /fiscalguard/raw/padron/
  hdfs dfs -put -f /data/raw/lista_69b/*   /fiscalguard/raw/lista_69b/
  hdfs dfs -du -h /fiscalguard/raw
'
