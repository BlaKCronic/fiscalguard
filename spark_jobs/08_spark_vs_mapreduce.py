"""
FiscalGuard - Fase 5 / Job 8: comparativa Spark vs MapReduce.

Misma lógica que mapreduce/MontoPorEmisor.java: monto facturado (en centavos) y número de CFDI
de ingreso vigentes por RFC emisor, leyendo la zona raw (CSV) de HDFS.

1. Mide el tiempo de Spark (lectura + agregación + escritura, igual que el job MapReduce).
2. Si existe la salida de MapReduce, verifica que los dos resultados sean IDÉNTICOS
   (mismos RFC, mismos conteos, mismos centavos).
3. Guarda un resumen en /data/resultados/comparativa_mr_spark.json (en Windows: data\\resultados).

Uso: spark-submit ... 08_spark_vs_mapreduce.py [BASE] [--mr-out RUTA_HDFS] [--mr-seg SEGUNDOS]
"""
import argparse
import json
import os
import time
import datetime as dt

from pyspark.sql import SparkSession, functions as F, types as T

ap = argparse.ArgumentParser()
ap.add_argument("base", nargs="?", default="hdfs://namenode:9000/fiscalguard")
ap.add_argument("--mr-out", default=None, help="salida del job MapReduce (def. BASE/analytics/mr_monto_emisor)")
ap.add_argument("--mr-seg", type=float, default=None, help="segundos que tardó MapReduce (para el resumen)")
ap.add_argument("--resumen", default="/data/resultados/comparativa_mr_spark.json")
a = ap.parse_args()
BASE = a.base
MR_OUT = a.mr_out or f"{BASE}/analytics/mr_monto_emisor"
SP_OUT = f"{BASE}/analytics/spark_monto_emisor"
RFC_RE = r"^[A-ZÑ&]{3,4}[0-9]{6}[A-Z0-9]{3}$"

spark = (SparkSession.builder.appName("fiscalguard-spark-vs-mapreduce")
         .config("spark.sql.shuffle.partitions", "24").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in (
    "uuid", "rfc_emisor", "rfc_receptor", "fecha", "subtotal", "iva", "total",
    "tipo_comprobante", "uso_cfdi", "forma_pago", "clave_prod_serv", "estatus")])

# ---------------- 1. Spark: misma agregación que MapReduce ----------------
t0 = time.time()
raw = spark.read.option("header", True).schema(SCHEMA).csv(f"{BASE}/raw/cfdi")
validos = (raw.withColumn("rfc", F.upper(F.trim("rfc_emisor")))
           .withColumn("total_d", F.trim("total").cast("double"))
           .filter((F.trim("tipo_comprobante") == "I") & (F.trim("estatus") == "Vigente")
                   & F.col("rfc").rlike(RFC_RE) & (F.col("total_d") > 0)))
agg = (validos.groupBy("rfc")
       .agg(F.count("*").alias("num_cfdi"),
            F.sum(F.round(F.col("total_d") * 100).cast("long")).alias("monto_centavos")))
agg.write.mode("overwrite").option("sep", "\t").csv(SP_OUT)
spark_seg = time.time() - t0
print(f"\nSPARK_SEGUNDOS={spark_seg:.1f}")

sp = spark.read.option("sep", "\t").schema("rfc string, num_cfdi long, monto_centavos long").csv(SP_OUT).cache()
sp_rfcs, sp_cfdi, sp_cent = sp.agg(F.count("*"), F.sum("num_cfdi"), F.sum("monto_centavos")).first()
print(f"Spark: {sp_rfcs:,} emisores, {sp_cfdi:,} CFDI válidos, ${sp_cent / 100:,.2f}")

# ---------------- 2. Verificación contra MapReduce ----------------
verif = {"disponible": False}
try:
    mr = spark.read.option("sep", "\t").schema("rfc string, num_cfdi long, monto_centavos long").csv(MR_OUT)
    j = sp.alias("s").join(mr.alias("m"), "rfc", "full_outer")
    dif = j.filter(F.col("s.num_cfdi").isNull() | F.col("m.num_cfdi").isNull()
                   | (F.col("s.num_cfdi") != F.col("m.num_cfdi"))
                   | (F.col("s.monto_centavos") != F.col("m.monto_centavos")))
    n_dif = dif.count()
    mr_rfcs, mr_cfdi, mr_cent = mr.agg(F.count("*"), F.sum("num_cfdi"), F.sum("monto_centavos")).first()
    verif = {"disponible": True, "emisores_mapreduce": mr_rfcs, "cfdi_mapreduce": mr_cfdi,
             "monto_mapreduce": mr_cent / 100, "rfc_con_diferencias": n_dif, "identicos": n_dif == 0}
    print(f"MapReduce: {mr_rfcs:,} emisores, {mr_cfdi:,} CFDI válidos, ${mr_cent / 100:,.2f}")
    print(">>> Resultados IDÉNTICOS en ambos motores" if n_dif == 0 else f">>> {n_dif:,} RFC con diferencias")
    if n_dif:
        dif.show(10, truncate=False)
except Exception as e:  # noqa: BLE001 - la salida de MR es opcional
    print("(sin salida de MapReduce para verificar):", str(e)[:150])

# ---------------- 3. Resumen ----------------
res = {
    "fecha": dt.datetime.now().isoformat(timespec="seconds"),
    "entrada": f"{BASE}/raw/cfdi",
    "spark": {"segundos": round(spark_seg, 1), "emisores": sp_rfcs, "cfdi_validos": sp_cfdi, "monto": sp_cent / 100,
              "recursos": spark.sparkContext.getConf().get("spark.master", "")},
    "mapreduce": {"segundos": a.mr_seg},
    "verificacion": verif,
}
if a.mr_seg:
    res["aceleracion_spark"] = round(a.mr_seg / spark_seg, 2)
    r = a.mr_seg / spark_seg
    print(f"\nSpark: {spark_seg:.0f} s  |  MapReduce: {a.mr_seg:.0f} s  ->  "
          + (f"Spark fue {r:.1f}x más rápido" if r >= 1 else f"MapReduce fue {1 / r:.1f}x más rápido (datos pequeños: pesa el arranque)"))
try:
    os.makedirs(os.path.dirname(a.resumen), exist_ok=True)
    with open(a.resumen, "w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, indent=2)
    print("Resumen guardado en", a.resumen)
except OSError as e:
    print("No se pudo guardar el resumen local:", e)
print(json.dumps(res, ensure_ascii=False, indent=2))
spark.stop()
