"""
FiscalGuard - Fase 5: orquestación del pipeline batch con Apache Airflow.

    preparar_driver -> etl_curated -> features -> clasificador ---------\
                                              \-> kmeans_anomalias ----+-> exportar_mongo -> verificar_mongo
                           etl_curated -> ciclos_circulares -----------/

Cada tarea ejecuta `spark-submit` DENTRO del contenedor spark-master (a través del socket de
Docker), así Airflow no necesita su propia instalación de Spark ni de Java. El log de Spark se ve
en la UI de Airflow y la tarea falla si el job termina con código distinto de 0.

- Programación: diaria a las 02:00 (hora de México), sin catchup. Se puede lanzar a mano con
  "Trigger DAG w/ config" y el parámetro `validacion_cruzada` (por defecto false, más rápido).
- max_active_tasks=1: el clúster de desarrollo tiene 4 cores; correr dos jobs a la vez solo haría
  que uno espere recursos. El grafo conserva las dependencias reales.
- El job 07 (streaming) NO está aquí: es un proceso continuo, no un batch.
- MONGO_URI se toma del .env de la raíz (docker-compose lo pasa al contenedor de Airflow).
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

SPARK_CONTAINER = os.environ.get("FG_SPARK_CONTAINER", "spark-master")
BASE = os.environ.get("FG_BASE", "hdfs://namenode:9000/fiscalguard")
SUBMIT = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--driver-memory", "2g"]


def docker_exec(cmd: list[str], user: str = "", env: dict | None = None) -> None:
    """Ejecuta cmd en el contenedor de Spark, reenvía su salida al log y falla si el código != 0."""
    import docker  # incluido en la imagen oficial de Airflow (provider docker)

    api = docker.from_env().api
    ex = api.exec_create(SPARK_CONTAINER, cmd, user=user, environment=env or {}, stdout=True, stderr=True)
    buf = b""
    for chunk in api.exec_start(ex["Id"], stream=True):
        buf += chunk
        *lines, buf = buf.split(b"\n")
        for ln in lines:
            print(ln.decode("utf-8", "replace"))
    if buf:
        print(buf.decode("utf-8", "replace"))
    code = api.exec_inspect(ex["Id"])["ExitCode"]
    if code != 0:
        raise AirflowException(f"{' '.join(cmd[:6])}... terminó con código {code}")


def spark_job(script: str, *extra: str, mongo: bool = False):
    def _run(**ctx):
        args = list(extra)
        if script == "03_train_classifier.py" and not ctx["params"].get("validacion_cruzada", False):
            args.append("--no-cv")
        env = {}
        if mongo:
            uri = os.environ.get("MONGO_URI")
            if not uri:
                raise AirflowException("Falta MONGO_URI en el .env de la raíz del proyecto")
            env["MONGO_URI"] = uri
        docker_exec(SUBMIT + [f"/opt/jobs/{script}", BASE] + args, env=env)
    return _run


def preparar_driver(**_):
    """pymongo en el driver (se pierde si el contenedor spark-master se recrea)."""
    docker_exec(["bash", "-c", "pip show pymongo >/dev/null 2>&1 || pip install -q 'pymongo[srv]' "
                 "|| pip install -q --break-system-packages 'pymongo[srv]'"], user="root")
    docker_exec(["test", "-f", "/opt/jobs/01_etl_curated.py"])  # los jobs están montados en /opt/jobs


VERIFICAR = r"""
import os, pymongo
db = pymongo.MongoClient(os.environ['MONGO_URI'], serverSelectionTimeoutMS=15000)[os.environ.get('MONGO_DB', 'fiscalguard')]
c = {k: db[k].estimated_document_count() for k in ('contribuyentes', 'alertas', 'redes', 'kpis_agregados')}
print('Documentos en MongoDB:', c)
st = db.command('dbStats'); print('Tamaño de datos: %.1f MB' % (st['dataSize'] / 1e6))
vacias = [k for k in ('contribuyentes', 'alertas', 'kpis_agregados') if c[k] == 0]
raise SystemExit('Colecciones vacías: %s' % vacias if vacias else 0)
"""


def verificar_mongo(**_):
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise AirflowException("Falta MONGO_URI")
    docker_exec(["python3", "-c", VERIFICAR], env={"MONGO_URI": uri, "MONGO_DB": os.environ.get("MONGO_DB", "fiscalguard")})


with DAG(
    dag_id="fiscalguard_batch",
    description="ETL, features, modelos y exportación a MongoDB (FiscalGuard)",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Mexico_City"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "fiscalguard", "retries": 1, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(hours=2)},
    params={"validacion_cruzada": Param(False, type="boolean",
                                        description="Validación cruzada (3 folds) en el clasificador; tarda más")},
    tags=["fiscalguard", "big-data"],
) as dag:
    prep = PythonOperator(task_id="preparar_driver", python_callable=preparar_driver)
    etl = PythonOperator(task_id="etl_curated", python_callable=spark_job("01_etl_curated.py"))
    feat = PythonOperator(task_id="features", python_callable=spark_job("02_features.py"))
    clf = PythonOperator(task_id="clasificador", python_callable=spark_job("03_train_classifier.py"))
    km = PythonOperator(task_id="kmeans_anomalias", python_callable=spark_job("04_kmeans_anomalies.py"))
    cyc = PythonOperator(task_id="ciclos_circulares", python_callable=spark_job("05_graph_cycles.py"))
    exp = PythonOperator(task_id="exportar_mongo", python_callable=spark_job("06_export_mongo.py", mongo=True),
                         retries=2)
    ver = PythonOperator(task_id="verificar_mongo", python_callable=verificar_mongo, retries=0)

    prep >> etl >> feat >> [clf, km]
    etl >> cyc
    [clf, km, cyc] >> exp >> ver
