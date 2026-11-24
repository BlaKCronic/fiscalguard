# FiscalGuard

**FiscalGuard** es un proyecto de Big Data para la detección y visualización de posibles contribuyentes de facturación ilícita. Procesa CFDI sintéticos, perfiles de contribuyentes y listas públicas simuladas para identificar EFOS, anomalías estadísticas y redes de facturación circular.

Desarrollado para la materia de Big Data del Tecnológico Nacional de México en Celaya.

> Todos los datos y RFC incluidos son sintéticos. No representan contribuyentes reales ni constituyen una determinación legal de evasión fiscal.

## Objetivo del proyecto

El sistema combina procesamiento por lotes, streaming, almacenamiento distribuido y una API segura para construir una plataforma completa de análisis:

- Validar y limpiar grandes volúmenes de CFDI.
- Generar características por contribuyente.
- Entrenar modelos de clasificación para localizar candidatos EFOS no publicados.
- Detectar ciclos de facturación entre contribuyentes.
- Identificar comportamientos anómalos.
- Procesar CFDI nuevos en casi tiempo real con Apache Kafka y Spark Structured Streaming.
- Guardar resultados consolidados en MongoDB.
- Exponer resultados mediante una API REST y un tablero web.
- Comparar el rendimiento de Spark con MapReduce sobre Hadoop YARN.

## Arquitectura

```text
Generador sintético
        |
        v
CSV raw -> HDFS
             |
             v
HDFS raw -> Spark ETL -> HDFS curated/analytics
                           |              |
                           v              v
                    Spark ML / Graph   MapReduce
                           |              |
                           +------+-------+
                                  v
                                MongoDB
                                  |
                           API REST + Dashboard

Kafka (cfdi-nuevos) -> Spark Structured Streaming -> MongoDB

Airflow -> orquesta el pipeline batch
```

### Tecnologías

| Capa | Tecnología |
|---|---|
| Contenedores | Docker Compose |
| Almacenamiento distribuido | Hadoop HDFS |
| Procesamiento batch | Apache Spark 3.5 / PySpark |
| ML | Spark MLlib: Random Forest y Gradient-Boosted Trees |
| Detección de grafos | DataFrames y algoritmos iterativos sobre los CFDI |
| Comparativa MapReduce | Java, Hadoop YARN |
| Streaming | Apache Kafka KRaft y Spark Structured Streaming |
| Orquestación | Apache Airflow |
| Persistencia de resultados | MongoDB Atlas o MongoDB local |
| API | Node.js, Express y JWT |
| Interfaz | JavaScript, HTML, CSS y Chart.js |

## Estructura del repositorio

```text
airflow/dags/                 DAG del pipeline batch
api/                          API REST, autenticación y pruebas
dashboard/                    Tablero web
data/                         Datos sintéticos y resultados
data_generator/               Generador de CFDI y RFCS sintéticos
docs/                         Guías adicionales
logs/                         Logs generados por ejecuciones
mapreduce/                    Programa Java MapReduce y JAR
mongo/                        Índices e inicialización de MongoDB
scripts/                      Carga HDFS, tópicos Kafka y automatización
spark_jobs/                   Trabajos Spark 00 al 08
streaming/                    Productor Kafka
docker-compose.yml            Infraestructura del proyecto
requirements.txt              Dependencias Python
```

## Componentes principales

### 1. Generador de datos sintéticos

`data_generator/generate_data.py` crea contribuyentes, padrón, CFDI y una lista 69-B simulada. También puede generar EFOS y redes circulares conocidas para validar el sistema.

Se excluye del pipeline de producción la información de verdad terreno utilizada únicamente para medir precisión y recall.

### 2. HDFS

HDFS conserva el Data Lake organizado en:

- `raw`: datos originales.
- `curated`: datos validados y normalizados en Parquet.
- `analytics`: features, puntajes, modelos, ciclos y redes.
- `models`: modelo clasificador Spark persistido.
- `ground_truth`: referencias sintéticas usadas para evaluación.

### 3. Pipeline Spark

| Job | Función |
|---|---|
| `00_smoke_test.py` | Verifica Spark y HDFS |
| `01_etl_curated.py` | Limpia CFDI y transforma CSV a Parquet particionado |
| `02_features.py` | Calcula características por contribuyente |
| `03_train_classifier.py` | Entrena y selecciona el clasificador |
| `04_kmeans_anomalies.py` | Agrupa contribuyentes y detecta anomalías |
| `05_graph_cycles.py` | Encuentra ciclos y redes circulares |
| `06_export_mongo.py` | Exporta contribuyentes, alertas, redes y KPIs |
| `07_streaming_scoring.py` | Puntúa CFDI recibidos por Kafka |
| `08_spark_vs_mapreduce.py` | Compara operación equivalente con MapReduce |

### 4. Clasificación

El clasificador usa como etiqueta la presencia simulada en la lista 69-B, pero nunca usa esa columna como feature. Se comparan Random Forest y GBT mediante AUC-PR y AUC-ROC.

Las variables incluyen montos emitidos y recibidos, número de clientes y proveedores, concentración HHI, CFDI cancelados, montos redondos, desviaciones de Benford, antigüedad, actividad mensual y relaciones con contribuyentes listados.

El umbral se ajusta buscando el puntaje más alto que conserve al menos 85 % de recall en validación. Los resultados se convierten a:

- `alto`: probabilidad mayor o igual al umbral.
- `medio`: probabilidad mayor o igual a la mitad del umbral.
- `bajo`: probabilidad inferior.

### 5. Detección de redes

El job de grafos construye aristas dirigidas emisor-receptor. Después busca rutas cerradas de longitud 3 a 6, elimina duplicados y agrupa los ciclos compartidos en redes.

`monto_ratio` representa el cociente entre el CFDI agregado de menor y mayor monto dentro de un ciclo. Valores cercanos a uno indican montos más equilibrados y, por tanto, mayor similitud con esquemas circulares sintéticos.

### 6. Streaming

El productor lee CFDI y los publica en el tópico `cfdi-nuevos`. El trabajo Spark los consume en micro-lotes, los enriquece con las features y genera alertas tipo `streaming` en MongoDB.

Una alerta puede activarse por:

- probabilidad de EFOS de un emisor superior al umbral; o
- receptor presente en la lista 69-B simulada.

### 7. API y dashboard

La API implementa:

- autenticación con JWT;
- roles `auditor`, `analista` y `admin`;
- enmascaramiento parcial de RFC para auditoría;
- filtros y paginación;
- consulta de contribuyentes, alertas y redes;
- actualización de estatus y comentarios;
- registro de accesos;
- limitación de intentos de inicio de sesión;
- expiración TTL de alertas descartadas.

El dashboard consume la API y muestra KPIs, tendencias, riesgo por entidad y sector, distribución de puntajes, factores de riesgo, alertas y redes.

### 8. Airflow

El DAG `fiscalguard_batch` ejecuta diariamente el ETL, features, clasificador, anomalías y detección de ciclos. Las ramas convergen antes de exportar a MongoDB y verificar la carga.

`07_streaming_scoring.py` no forma parte del DAG porque es un proceso continuo.

## Requisitos

- Windows 10/11 con Docker Desktop o un entorno Linux con Docker Engine.
- Docker Compose v2.
- PowerShell 5.1 o posterior en Windows.
- Python 3.10 o posterior.
- Node.js 18 o posterior.
- MongoDB Atlas o MongoDB local.
- 8 GB de RAM como mínimo; se recomienda 16 GB para datos grandes.

## Configuración

Copia el ejemplo de variables de entorno:

```powershell
Copy-Item .env.example .env
```

Edita `.env` con valores propios:

```dotenv
MONGO_URI=mongodb+srv://USUARIO:PASSWORD@CLUSTER.mongodb.net/fiscalguard
JWT_SECRET=una-cadena-secreta-de-al-menos-32-caracteres
MONGO_DB=fiscalguard
```

No agregues `.env` ni credenciales reales al repositorio.

Instala las dependencias locales:

```powershell
py -m pip install -r requirements.txt
cd api
npm install
cd ..
```

## Ejecución rápida

### Levantar la infraestructura

```powershell
docker compose up -d
docker compose --profile yarn up -d
docker compose --profile airflow up -d
docker compose --profile local-mongo up -d
```

Interfaces principales:

- HDFS: <http://localhost:9870>
- Spark: <http://localhost:8080>
- YARN: <http://localhost:8088>
- Airflow: <http://localhost:8090>
- MongoDB local: `mongodb://localhost:27017`

### Generar datos

Prueba pequeña:

```powershell
python data_generator/generate_data.py --contribuyentes 100000 --cfdi 5000000
```

Escala completa:

```powershell
python data_generator/generate_data.py --contribuyentes 500000 --cfdi 50000000
```

La generación requiere el tamaño adecuado de disco y puede tardar varias horas dependiendo del equipo.

### Cargar datos al HDFS

PowerShell:

```powershell
.\scripts\load_to_hdfs.ps1
```

Linux/macOS o Git Bash:

```bash
./scripts/load_to_hdfs.sh
```

### Ejecutar el pipeline batch

```powershell
.\scripts\pipeline.ps1
```

Opciones disponibles:

```powershell
.\scripts\pipeline.ps1 -SinCV
.\scripts\pipeline.ps1 -Desde 03
.\scripts\pipeline.ps1 -Solo 06
```

Los logs se guardan en `logs/pipeline_<fecha>/` y los tiempos en `data/resultados/tiempos_pipeline.csv`.

### Ejecutar el streaming

Crear los tópicos:

```powershell
.\scripts\create_topics.ps1
```

Iniciar el productor:

```powershell
python streaming/producer.py --rate 50 --limit 1000
```

Iniciar el consumidor Spark:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /opt/jobs/07_streaming_scoring.py hdfs://namenode:9000/fiscalguard `
  --uri "$env:MONGO_URI" --trigger 2
```

### Inicializar MongoDB

Para MongoDB local:

```powershell
docker compose exec mongo mongosh -u admin -p admin --authenticationDatabase admin fiscalguard /docker-entrypoint-initdb.d/init_collections.js
```

El script `mongo/init_collections.js` crea colecciones e índices. Ajusta la cadena de conexión si usas MongoDB Atlas.

### Iniciar la API y el dashboard

```powershell
cd api
npm start
```

Abre <http://localhost:3000>. La API sirve los archivos estáticos del dashboard desde `dashboard/`.

En una instalación nueva, crea los usuarios iniciales:

```powershell
cd api
npm run seed
```

### Ejecutar pruebas

```powershell
cd api
npm test
```

## Ejemplo de consumo de API

Inicio de sesión:

```bash
curl -X POST http://localhost:3000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"usuario":"analista","password":"CAMBIAR"}'
```

Consulta de alertas usando el token obtenido:

```bash
curl "http://localhost:3000/alertas?page=1&limit=25&estatus=nueva" \
  -H "Authorization: Bearer TOKEN_JWT"
```

## Validación y benchmarks

El proyecto incluye scripts y archivos de resultados para:

- comparar Spark contra MapReduce;
- medir tiempos del pipeline;
- comparar velocidades con 5 y 20 millones de registros;
- validar latencia del streaming;
- medir el rendimiento de la API.

Ejemplos:

```powershell
.\scripts\compare_mr_spark.ps1
.\benchmark.ps1
```

Los resultados se conservan en `data/resultados/`.

## Consideraciones

- El uso de la lista 69-B es un componente experimental. En datos reales se requieren controles legales y una investigación humana.
- El modelo no sustituye la decisión de autoridades fiscales.
- Los puntajes pueden contener sesgos derivados de los datos sintéticos o de los criterios de generación.
- Los secretos deben administrarse con variables de entorno o un gestor de secretos.
- Para entornos compartidos se recomienda reducir los permisos RBAC y reemplazar las credenciales de desarrollo.

## Créditos

Proyecto académico de Big Data desarrollado en el Tecnológico Nacional de México en Celaya.
