# FiscalGuard: crea el tópico cfdi-nuevos en Kafka (equivale a create_topics.sh). Desde la raíz:
#     .\scripts\create_topics.ps1
# message.timestamp.type=LogAppendTime: la marca de tiempo la pone el broker, así la latencia
# que mide el job de streaming no depende del reloj de Windows.
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 `
  --create --if-not-exists --topic cfdi-nuevos --partitions 3 --replication-factor 1 `
  --config message.timestamp.type=LogAppendTime
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 `
  --alter --entity-type topics --entity-name cfdi-nuevos --add-config message.timestamp.type=LogAppendTime | Out-Null
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic cfdi-nuevos
