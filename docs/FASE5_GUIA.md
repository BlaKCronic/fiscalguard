# FiscalGuard: Fase 5 (orquestación, comparativa y mediciones)

Esta fase completa lo que la propuesta dejó pendiente:

| Pendiente de la propuesta | Qué se agregó |
|---|---|
| Orquestación con Apache Airflow | `airflow/dags/fiscalguard_batch.py` y el servicio `airflow` (perfil `airflow`) |
| YARN y comparativa con MapReduce | servicios `resourcemanager`/`nodemanager` (perfil `yarn`), `mapreduce/MontoPorEmisor.java`, `spark_jobs/08_spark_vs_mapreduce.py` y `scripts/compare_mr_spark.ps1` |
| Tiempos del pipeline | `scripts/pipeline.ps1` (corre los jobs 01 a 06 y mide cada uno) |
| Tiempos de la API | `api/scripts/bench.js` (`npm run bench`) |
| Scripts para Windows | `scripts/load_to_hdfs.ps1` y `scripts/create_topics.ps1` (ya no hace falta Git Bash) |

Todos los resultados quedan en `data\resultados\`.

> **Memoria:** con todo encendido (HDFS, Spark, Kafka, YARN y Airflow) se necesitan unos 10 GB.
> Haz los pasos en orden y apaga lo que ya no uses. Con 8 GB para Docker alcanza si no
> enciendes YARN y Airflow al mismo tiempo.

---

## 0. Copiar los archivos

Descomprime `fiscalguard_fase5.zip` **encima** de la carpeta del proyecto y acepta reemplazar.
Solo cambian `docker-compose.yml` (se agregaron servicios con perfiles y los que ya existían
siguen igual) y `api/package.json` (nuevo script `bench`). Todo lo demás es nuevo.

Si PowerShell bloquea los scripts, en la misma ventana corre:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## 1. Núcleo arriba y datos en HDFS

```powershell
docker compose up -d
.\scripts\load_to_hdfs.ps1      # solo si HDFS está vacío; ahora también sube ground_truth
.\scripts\create_topics.ps1     # deja el tópico con LogAppendTime (latencia medida por el broker)
```

## 2. Pipeline completo con tiempos (validación cruzada incluida)

En la presentación quedó dicho que el clasificador se había corrido **sin** validación cruzada
completa. Esta corrida la incluye:

```powershell
.\scripts\pipeline.ps1           # 01 -> 06, con validación cruzada
# .\scripts\pipeline.ps1 -SinCV  # versión rápida
# .\scripts\pipeline.ps1 -Desde 03
```

- Lee `MONGO_URI` del `.env` de la raíz, el mismo que usa la API.
- Los logs quedan en `logs\pipeline_<fecha>\NN.log` y los tiempos en `data\resultados\tiempos_pipeline.csv`.
- **Guarda el log `03.log`**: ahí están recall, precisión, AUC-ROC y AUC-PR con validación cruzada,
  más la evaluación contra la verdad terreno.

## 3. Comparativa MapReduce (YARN) vs Spark

```powershell
docker compose stop kafka            # libera memoria; no se usa en esta prueba
.\scripts\compare_mr_spark.ps1
docker compose --profile yarn stop resourcemanager nodemanager
docker compose start kafka
```

El script hace lo siguiente:
1. Levanta YARN y espera a que el NodeManager quede en estado RUNNING. La UI de YARN está en http://localhost:8088.
2. Compila `MontoPorEmisor.java` dentro del contenedor, que ya trae Java 8 y Hadoop 3.2.1.
3. Corre el job MapReduce sobre YARN: monto y número de CFDI de ingreso vigentes por RFC emisor, leyendo `raw/cfdi`.
4. Corre la misma lógica en Spark y **verifica que los dos resultados sean idénticos**: mismos RFC, mismos conteos y mismos centavos.
5. Guarda `data\resultados\comparativa_tiempos.csv` y `comparativa_mr_spark.json`.

Mide dos tiempos:
- **job:** desde que el motor empieza a leer hasta que escribe el resultado.
- **total:** el comando completo, que incluye el arranque de la JVM o del SparkContext y la negociación con YARN.

**Para el reporte, cómo es la comparación:** MapReduce corre en YARN con 1 NodeManager (3 GB y 2 vcores).
Spark corre en su clúster standalone (2 workers de 2 cores y 2 GB). Spark no se lanzó sobre YARN porque los
contenedores del NodeManager (bde2020, Debian 9) no traen Python, y PySpark en YARN lo necesita
en cada nodo. Los recursos no son idénticos y conviene decirlo así. Lo que sí es idéntico son
los datos, la lógica y el resultado.

## 4. Tiempos de la API

Con la API corriendo (`npm start` en `api`), abre **otra** ventana:

```powershell
cd api
$env:BENCH_USER = "analista"; $env:BENCH_PASS = "<su contraseña>"
npm run bench
```

Hace un solo login, porque el límite es de 10 intentos cada 15 min. Luego mide 50 peticiones en serie y 50 con 10
simultáneas por cada endpoint: `/kpis`, `/alertas` (incluye streaming), `/contribuyentes`, `/contribuyentes/:rfc` y `/red/:rfc`.
El resultado queda en `data\resultados\bench_api.json` y `bench_api.csv`.

Opcionales: `$env:BENCH_N = "100"` y `$env:BENCH_CONC = "20"`.

## 5. Airflow (orquestación)

```powershell
docker compose --profile airflow up -d airflow
docker compose logs -f airflow       # espera "Airflow is ready" (1-3 min la primera vez); Ctrl+C
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

1. Entra a http://localhost:8090 con el usuario `admin` y la contraseña que mostró el último comando.
2. El DAG **fiscalguard_batch** aparece pausado. Actívalo con el interruptor.
3. Lánzalo con ▶ **Trigger DAG w/ config**. `validacion_cruzada` está en `false` por defecto, para que sea rápido.
4. En la vista **Graph** se ven las dependencias:
   `preparar_driver → etl_curated → features → {clasificador, kmeans_anomalias}`, `etl_curated → ciclos_circulares`, y las tres → `exportar_mongo → verificar_mongo`.
5. Para el reporte, toma captura de la vista Graph con todo en verde, de la vista Grid y del log de `verificar_mongo` (muestra el conteo de documentos en Atlas).

Detalles:
- Se programa diario a las 02:00 (hora de México) y no ejecuta corridas atrasadas (catchup).
- Corre una tarea a la vez, porque el clúster tiene 4 cores.
- Cada tarea ejecuta `spark-submit` **dentro** de `spark-master` a través del socket de Docker, y el log de Spark se ve en Airflow.
- Volver a exportar no pisa el trabajo del analista: las alertas conservan su estatus y sus comentarios.
- El streaming (job 07) no va en el DAG porque es un proceso continuo.

Para apagarlo: `docker compose --profile airflow stop airflow`.

---

## 6. Qué mandar para cerrar la documentación

Con esto se actualizan el guion, el resumen técnico, el reporte final y la presentación con cifras reales:

1. La carpeta `data\resultados\` completa: `tiempos_pipeline.csv`, `comparativa_tiempos.csv`, `comparativa_mr_spark.json`, `bench_api.json` y `bench_api.csv`.
2. `logs\pipeline_<fecha>\03.log`, que tiene las métricas del clasificador con validación cruzada, y `05.log` (ciclos y redes).
3. Lo que imprime `verificar_mongo` en Airflow, o una captura de Atlas con el número de documentos de cada colección.
4. Las capturas de Airflow (Graph y Grid) y de YARN (http://localhost:8088, job en estado FINISHED).
