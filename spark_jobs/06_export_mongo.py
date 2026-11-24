"""
FiscalGuard - Fase 4 / Job 6: exporta SOLO resultados resumidos a MongoDB (capa de servicio).

Lee analytics/* y curated/* de HDFS y escribe en MongoDB (Atlas M0 o local):
  contribuyentes   riesgo medio/alto (razón social enmascarada) + factores_riesgo[]
  alertas          clasificador | red_circular | anomalia   (upsert: NO pisa estatus ni comentarios del analista)
  redes            redes de facturación circular (con sus ciclos, para dibujar el grafo)
  kpis_agregados   periodo x entidad x sector
El detalle completo (CFDI, features de los 100k) se queda en HDFS.

Nivel final de riesgo = nivel del clasificador, subido un escalón si el RFC está en una red circular
(bajo->medio, medio->alto). Los factores_riesgo son reglas de explicación sobre las features
(umbrales heurísticos), no la lógica interna del modelo.

Requiere pymongo en el contenedor del driver:  pip install "pymongo[srv]"
Uso: spark-submit ... 06_export_mongo.py [BASE] --uri "mongodb+srv://..." [--db fiscalguard]
     (--uri por defecto: variable MONGO_URI o mongodb://mongo:27017)
"""
import argparse
import datetime as dt
import hashlib
import os

import pymongo
from pymongo import UpdateOne
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("base", nargs="?", default="hdfs://namenode:9000/fiscalguard")
ap.add_argument("--uri", default=os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
ap.add_argument("--db", default="fiscalguard")
ap.add_argument("--max-anomalias", type=int, default=2000)
a = ap.parse_args()
BASE = a.base
NOW = dt.datetime.utcnow()
RUN = NOW.strftime("%Y%m%dT%H%M%S")

spark = (SparkSession.builder.appName("fiscalguard-export-mongo")
         .config("spark.sql.shuffle.partitions", "24").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
rd = lambda p: spark.read.parquet(f"{BASE}/{p}")

# ---------------- ensamble de contribuyentes ----------------
sc = rd("analytics/scores_clasificador").select("rfc", "puntaje_riesgo", F.col("nivel_riesgo").alias("nivel_modelo"),
                                                "en_lista_69b", "modelo")
an = rd("analytics/scores_anomalia").select("rfc", "puntaje_anomalia", "es_outlier", "cluster_pequeno")
nod = rd("analytics/nodos_en_ciclo").select("rfc", "id_red", "num_ciclos")
ft = rd("analytics/features_contribuyente").select(
    "rfc", "monto_emitido", "num_cfdi_emitidos", "num_clientes", "pct_montos_redondos", "pct_clave_fuera_sector",
    "antiguedad_dias", "benford_mad", "num_contrapartes_69b")
pad = rd("curated/padron").select("rfc", "razon_social", "regimen", "sector", "estado", "municipio",
                                  "fecha_inicio_operaciones")

b = (sc.join(an, "rfc", "left").join(nod, "rfc", "left").join(ft, "rfc", "left").join(pad, "rfc", "left"))
b = b.withColumn("nivel_final",
                 F.when((F.col("nivel_modelo") == "alto"), "alto")
                 .when((F.col("nivel_modelo") == "medio") & F.col("id_red").isNotNull(), "alto")
                 .when(F.col("nivel_modelo") == "medio", "medio")
                 .when(F.col("id_red").isNotNull(), "medio").otherwise("bajo"))

factores = F.filter(F.array(
    F.when(F.col("pct_montos_redondos") >= 0.2,
           F.format_string("Montos redondos frecuentes (%d%% de sus CFDI)", F.round(F.col("pct_montos_redondos") * 100).cast("int"))),
    F.when(F.col("pct_clave_fuera_sector") >= 0.3,
           F.format_string("Factura conceptos ajenos a su sector (%d%% de sus CFDI)", F.round(F.col("pct_clave_fuera_sector") * 100).cast("int"))),
    F.when(F.col("antiguedad_dias") < 365,
           F.format_string("Empezó a facturar poco después de su alta (%d días)", F.col("antiguedad_dias"))),
    F.when((F.col("num_clientes") <= 5) & (F.col("num_cfdi_emitidos") >= 30),
           F.format_string("Factura mucho a pocos clientes (%d clientes, %d CFDI)", F.col("num_clientes"), F.col("num_cfdi_emitidos"))),
    F.when(F.col("benford_mad") >= 0.05, F.format_string("Montos que se desvían de la Ley de Benford (MAD %.3f)", F.col("benford_mad"))),
    F.when(F.col("num_contrapartes_69b") >= 1, F.format_string("Opera con %d contribuyente(s) de la lista 69-B", F.col("num_contrapartes_69b"))),
    F.when(F.col("id_red").isNotNull(), F.format_string("Participa en %d ciclo(s) de facturación circular", F.col("num_ciclos"))),
    F.when(F.col("cluster_pequeno") == 1, F.lit("Su comportamiento cae en un grupo atípico y poco común (K-Means)")),
), lambda x: x.isNotNull())
mask = F.regexp_replace("razon_social", r"([^\s]{3})[^\s]+", "$1***")
riesgosos = (b.filter("nivel_final in ('alto', 'medio')").withColumn("factores", factores)
             .withColumn("razon_social_m", mask).cache())
n_riesgo = riesgosos.count()
print(f"Contribuyentes de riesgo medio/alto a exportar: {n_riesgo:,}")


def dtm(d):
    return dt.datetime(d.year, d.month, d.day) if d is not None else None


client = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=15000)
db = client[a.db]
client.admin.command("ping")


def flush(coll, ops):
    if ops:
        coll.bulk_write(ops, ordered=False)


# ---------------- contribuyentes ----------------
ops, n = [], 0
for r in riesgosos.toLocalIterator():
    d = r.asDict()
    doc = {"rfc": d["rfc"], "razon_social": d["razon_social_m"], "regimen": d["regimen"], "actividad": d["sector"],
           "fecha_alta": dtm(d["fecha_inicio_operaciones"]), "municipio": d["municipio"], "estado": d["estado"],
           "en_lista_69b": bool(d["en_lista_69b"]), "puntaje_riesgo": d["puntaje_riesgo"],
           "nivel_riesgo": d["nivel_final"], "nivel_modelo": d["nivel_modelo"], "factores_riesgo": d["factores"],
           "id_red": d["id_red"], "monto_emitido": d["monto_emitido"], "fecha_evaluacion": NOW, "corrida_id": RUN}
    ops.append(UpdateOne({"rfc": d["rfc"]}, {"$set": doc}, upsert=True))
    n += 1
    if len(ops) >= 1000:
        flush(db.contribuyentes, ops); ops = []
flush(db.contribuyentes, ops)
borrados = db.contribuyentes.delete_many({"corrida_id": {"$ne": RUN}}).deleted_count
print(f"contribuyentes: {n:,} escritos, {borrados:,} obsoletos eliminados")

# ---------------- alertas ----------------
def alert_op(rfc, tipo, puntaje, desc):
    aid = "ALR-" + hashlib.sha1(f"{rfc}|{tipo}".encode()).hexdigest()[:12].upper()
    return UpdateOne({"id_alerta": aid},
                     {"$set": {"rfc": rfc, "tipo": tipo, "puntaje": puntaje, "descripcion": desc, "fecha_actualizacion": NOW},
                      "$setOnInsert": {"estatus": "nueva", "analista_asignado": None, "fecha_creacion": NOW,
                                       "comentarios": []}}, upsert=True)


ops, cnt = [], {"clasificador": 0, "red_circular": 0, "anomalia": 0}
for r in riesgosos.toLocalIterator():
    d = r.asDict()
    if d["nivel_modelo"] in ("alto", "medio"):
        top = "; ".join(d["factores"][:3]) or "sin factores destacados"
        ops.append(alert_op(d["rfc"], "clasificador", d["puntaje_riesgo"],
                            f"Riesgo {d['nivel_modelo']} de operaciones simuladas (modelo {d['modelo']}). {top}."))
        cnt["clasificador"] += 1
    if d["id_red"] is not None:
        ops.append(alert_op(d["rfc"], "red_circular", 90.0,
                            f"Participa en {d['num_ciclos']} ciclo(s) de facturación circular (red {d['id_red']})."))
        cnt["red_circular"] += 1
    if len(ops) >= 1000:
        flush(db.alertas, ops); ops = []
anom = (b.filter("nivel_modelo = 'bajo' and id_red is null and (es_outlier = 1 or cluster_pequeno = 1)")
        .orderBy(F.desc("puntaje_anomalia")).limit(a.max_anomalias).select("rfc", "puntaje_anomalia", "cluster_pequeno"))
for r in anom.toLocalIterator():
    motivo = "pertenece a un grupo atípico poco común" if r["cluster_pequeno"] == 1 else "se aleja del comportamiento de su grupo"
    ops.append(alert_op(r["rfc"], "anomalia", r["puntaje_anomalia"], f"Comportamiento atípico: {motivo} (K-Means)."))
    cnt["anomalia"] += 1
    if len(ops) >= 1000:
        flush(db.alertas, ops); ops = []
flush(db.alertas, ops)
print("alertas:", cnt)

# ---------------- redes ----------------
# se guardan también los ciclos (listas ordenadas de RFC) para que la API pueda dibujar las aristas
cic = rd("analytics/ciclos").groupBy("id_red").agg(F.collect_list("nodos").alias("ciclos"))
ops = []
for r in rd("analytics/redes").join(cic, "id_red", "left").toLocalIterator():
    d = r.asDict()
    ops.append(UpdateOne({"id_red": d["id_red"]},
                         {"$set": {"rfcs_involucrados": list(d["rfcs"]), "monto_total": d["monto_aristas_internas"],
                                   "numero_ciclos": d["num_ciclos"], "numero_nodos": d["num_nodos"],
                                   "ciclos": [list(c) for c in (d["ciclos"] or [])], "corrida_id": RUN},
                          "$setOnInsert": {"fecha_deteccion": NOW}}, upsert=True))
flush(db.redes, ops)
print(f"redes: {len(ops):,} escritas, {db.redes.delete_many({'corrida_id': {'$ne': RUN}}).deleted_count} obsoletas eliminadas")

# ---------------- KPIs agregados ----------------
cf = rd("curated/cfdi").filter("tipo_comprobante = 'I' and estatus = 'Vigente'")
riesgo = riesgosos.select(F.col("rfc").alias("rfc_emisor"), F.lit(1).alias("en_riesgo"))
kp = (cf.join(pad.select(F.col("rfc").alias("rfc_emisor"), "estado", "sector"), "rfc_emisor")
      .join(F.broadcast(riesgo), "rfc_emisor", "left")
      .groupBy(F.date_format("fecha", "yyyy-MM").alias("periodo"), "estado", "sector")
      .agg(F.sum("subtotal").alias("total_facturado"),
           F.sum(F.when(F.col("en_riesgo") == 1, F.col("subtotal")).otherwise(0.0)).alias("total_en_riesgo"),
           F.countDistinct(F.when(F.col("en_riesgo") == 1, F.col("rfc_emisor"))).alias("num_alertas")))
ops = []
for r in kp.toLocalIterator():
    d = r.asDict()
    ops.append(UpdateOne({"periodo": d["periodo"], "entidad": d["estado"], "sector": d["sector"]},
                         {"$set": {"total_facturado": d["total_facturado"], "total_en_riesgo": d["total_en_riesgo"],
                                   "num_alertas": d["num_alertas"], "corrida_id": RUN}}, upsert=True))
    if len(ops) >= 1000:
        flush(db.kpis_agregados, ops); ops = []
flush(db.kpis_agregados, ops)
db.kpis_agregados.delete_many({"corrida_id": {"$ne": RUN}})
print(f"kpis_agregados: {db.kpis_agregados.count_documents({}):,} documentos")

# ---------------- índices (idempotentes) ----------------
db.contribuyentes.create_index("rfc", unique=True)
db.contribuyentes.create_index([("nivel_riesgo", 1), ("puntaje_riesgo", -1)])
db.contribuyentes.create_index("estado")
db.alertas.create_index("id_alerta", unique=True)
db.alertas.create_index([("estatus", 1), ("puntaje", -1)])
db.alertas.create_index("rfc")
db.alertas.create_index("expira_en", expireAfterSeconds=0)   # la API fija expira_en al descartar una alerta
db.redes.create_index("id_red", unique=True)
db.redes.create_index("rfcs_involucrados")
db.kpis_agregados.create_index([("periodo", 1), ("entidad", 1), ("sector", 1)])
try:
    st = db.command("dbstats")
    print(f"Tamaño de datos en MongoDB: {st['dataSize'] / 1e6:.1f} MB (límite M0: 512 MB)")
except Exception:
    pass
print({c: db[c].count_documents({}) for c in ["contribuyentes", "alertas", "redes", "kpis_agregados"]})
spark.stop()
