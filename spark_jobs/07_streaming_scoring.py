"""
FiscalGuard - Fase 4 / Job 7: puntuación en tiempo casi real (Spark Structured Streaming).

Kafka (cfdi-nuevos) -> micro-lotes -> enriquece cada CFDI con las variables del emisor
-> aplica el clasificador entrenado -> alerta inmediata en MongoDB (tipo "streaming").

Una alerta se genera cuando:
  - la probabilidad del emisor >= umbral (el mismo del batch, guardado en scores_clasificador), o
  - el receptor está en la lista 69-B.
Hay UNA alerta por emisor (upsert) que acumula el número de CFDI alertados; no pisa el estatus
ni los comentarios del analista. Se imprime la latencia de cada lote (meta: < 10 s).
La latencia usa la marca de tiempo de Kafka: configura el tópico con LogAppendTime para que la
asigne el broker (ver instrucciones), así no depende del reloj de Windows.

Uso (requiere el paquete de Kafka para Spark y pymongo en el contenedor del driver):
  spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 07_streaming_scoring.py BASE
  [--uri MONGO_URI] [--bootstrap kafka:9092] [--topic cfdi-nuevos] [--trigger 2]
  [--from-beginning] [--seconds N]   (N = detenerse tras N segundos; 0 = indefinido)
"""
import argparse
import hashlib
import os
import time
import datetime as dt

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

NUM = ["monto_emitido", "num_cfdi_emitidos", "num_clientes", "pct_cancelados", "pct_montos_redondos",
       "pct_clave_fuera_sector", "benford_mad", "hhi_clientes", "meses_activos", "pico_mensual",
       "cv_mensual", "num_contrapartes_69b", "pct_monto_con_69b", "monto_recibido",
       "num_cfdi_recibidos", "num_proveedores", "antiguedad_dias", "monto_por_dia_antiguedad"]

SCHEMA = T.StructType([
    T.StructField("uuid", T.StringType()), T.StructField("rfc_emisor", T.StringType()),
    T.StructField("rfc_receptor", T.StringType()), T.StructField("fecha", T.StringType()),
    T.StructField("subtotal", T.DoubleType()), T.StructField("iva", T.DoubleType()),
    T.StructField("total", T.DoubleType()), T.StructField("tipo_comprobante", T.StringType()),
    T.StructField("uso_cfdi", T.StringType()), T.StructField("forma_pago", T.StringType()),
    T.StructField("clave_prod_serv", T.StringType()), T.StructField("estatus", T.StringType())])


def procesar_lote(batch_df, batch_id, ctx):
    """Se ejecuta por cada micro-lote. batch_df: CFDI parseados + columna ts (marca de tiempo de Kafka)."""
    t_ini = time.time()
    b = batch_df.filter(F.col("uuid").isNotNull() & F.col("rfc_emisor").isNotNull()).cache()
    n = b.count()
    if n == 0:
        b.unpersist()
        return
    feat, l69 = ctx["feat"], ctx["l69"]
    ev = (b.join(F.broadcast(feat), b["rfc_emisor"] == feat["rfc"], "left").drop(feat["rfc"])
          .join(F.broadcast(l69), b["rfc_receptor"] == l69["rfc_69b"], "left")
          .withColumn("rec69", F.col("rfc_69b").isNotNull())
          .withColumn("sin_actividad", F.when(F.col("monto_emitido").isNull(), 1.0).otherwise(0.0))
          .fillna(0.0, subset=NUM))
    sc = ctx["model"].transform(ev).withColumn("prob", vector_to_array("probability")[1])
    thr = ctx["umbral"]
    al = (sc.filter((F.col("prob") >= thr) | F.col("rec69"))
          .groupBy("rfc_emisor").agg(F.max("prob").alias("prob"), F.count("*").alias("n"),
                                     F.sum("subtotal").alias("monto"), F.max(F.col("rec69").cast("int")).alias("rec69"),
                                     F.max("uuid").alias("uuid"))
          .limit(5000).collect())
    ahora = dt.datetime.utcnow()
    for r in al:
        motivos = []
        if r["prob"] >= thr:
            motivos.append(f"emisor con riesgo {round(r['prob'] * 100, 1)}/100")
        if r["rec69"]:
            motivos.append("factura a un contribuyente de la lista 69-B")
        aid = "ALR-" + hashlib.sha1(f"{r['rfc_emisor']}|streaming".encode()).hexdigest()[:12].upper()
        ctx["db"].alertas.update_one(
            {"id_alerta": aid},
            {"$set": {"rfc": r["rfc_emisor"], "tipo": "streaming", "puntaje": round(r["prob"] * 100, 1),
                      "descripcion": "CFDI en vivo: " + "; ".join(motivos) + f". Último CFDI {r['uuid']}.",
                      "fecha_actualizacion": ahora, "ultimo_cfdi": r["uuid"]},
             "$inc": {"cfdis_alertados": int(r["n"])},
             "$setOnInsert": {"estatus": "nueva", "analista_asignado": None, "fecha_creacion": ahora, "comentarios": []}},
            upsert=True)
    st = b.agg(F.min(F.col("ts").cast("double")).alias("a"), F.max(F.col("ts").cast("double")).alias("b")).first()
    fin = time.time()
    lat_max, lat_min = fin - st["a"], fin - st["b"]
    print(f"[lote {batch_id}] {n} CFDI | {len(al)} emisores alertados | procesamiento {fin - t_ini:.1f} s | "
          f"latencia extremo a extremo: mín {lat_min:.1f} s, máx {lat_max:.1f} s "
          f"{'OK (< 10 s)' if lat_max < 10 else 'SUPERA 10 s'}", flush=True)
    b.unpersist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base", nargs="?", default="hdfs://namenode:9000/fiscalguard")
    ap.add_argument("--uri", default=os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
    ap.add_argument("--db", default="fiscalguard")
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="cfdi-nuevos")
    ap.add_argument("--trigger", type=int, default=2)
    ap.add_argument("--max-per-trigger", type=int, default=5000)
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--seconds", type=int, default=0)
    a = ap.parse_args()
    import pymongo
    spark = (SparkSession.builder.appName("fiscalguard-streaming").config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    B = a.base
    feat = spark.read.parquet(f"{B}/analytics/features_contribuyente").select("rfc", *NUM).cache()
    l69 = spark.read.parquet(f"{B}/curated/lista_69b").select(F.col("rfc").alias("rfc_69b")).cache()
    umbral = spark.read.parquet(f"{B}/analytics/scores_clasificador").select("umbral").first()["umbral"]
    model = PipelineModel.load(f"{B}/models/clasificador_efos")
    db = pymongo.MongoClient(a.uri, serverSelectionTimeoutMS=15000)[a.db]
    db.client.admin.command("ping")
    ctx = {"feat": feat, "l69": l69, "umbral": float(umbral), "model": model, "db": db}
    print(f"Listo. Emisores con variables: {feat.count():,} | umbral de alerta: {umbral} | escuchando '{a.topic}'...", flush=True)
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", a.bootstrap).option("subscribe", a.topic)
           .option("startingOffsets", "earliest" if a.from_beginning else "latest")
           .option("maxOffsetsPerTrigger", a.max_per_trigger).option("failOnDataLoss", "false").load())
    ev = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("j"), F.col("timestamp").alias("ts")).select("j.*", "ts")
    q = (ev.writeStream.foreachBatch(lambda df, i: procesar_lote(df, i, ctx))
         .option("checkpointLocation", f"{B}/checkpoints/streaming").trigger(processingTime=f"{a.trigger} seconds").start())
    if a.seconds > 0:
        q.awaitTermination(a.seconds)
        q.stop()
    else:
        q.awaitTermination()


if __name__ == "__main__":
    main()
