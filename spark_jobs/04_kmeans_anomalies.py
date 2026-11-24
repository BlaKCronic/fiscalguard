"""
FiscalGuard - Fase 3 / Job 4: K-Means para detección de anomalías (no supervisado).

analytics/features_contribuyente -> analytics/scores_anomalia (+ analytics/perfil_clusters)

- Anomalía = outlier (distancia al centroide >= cuantil) O pertenecer a un cluster pequeño (< --min-cluster,
  def. 5% de la población): un grupo de comportamiento raro y compacto no es outlier de su propio centroide.
- Solo contribuyentes con actividad. Variables monetarias/conteos con log1p, luego estandarización.
- k se elige por coeficiente de silueta. Anomalía = distancia al centroide >= cuantil (def. 0.99).
- NO usa la etiqueta 69-B ni la verdad terreno para ajustar; solo las muestra al final para interpretar.

Uso: spark-submit ... 04_kmeans_anomalies.py [BASE] [--quantile 0.99]
"""
import argparse
import math
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

ap = argparse.ArgumentParser()
ap.add_argument("base", nargs="?", default="hdfs://namenode:9000/fiscalguard")
ap.add_argument("--quantile", type=float, default=0.99)
ap.add_argument("--min-cluster", type=float, default=0.05, help="fracción mínima para no ser cluster pequeño")
a = ap.parse_args()
BASE = a.base

spark = (SparkSession.builder.appName("fiscalguard-kmeans")
         .config("spark.sql.shuffle.partitions", "24").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

LOG_COLS = ["monto_emitido", "num_cfdi_emitidos", "num_clientes", "monto_recibido", "num_cfdi_recibidos",
            "num_proveedores", "antiguedad_dias", "monto_por_dia_antiguedad"]
RAW_COLS = ["pct_cancelados", "pct_montos_redondos", "pct_clave_fuera_sector", "benford_mad",
            "hhi_clientes", "meses_activos", "pico_mensual", "cv_mensual"]

feat = spark.read.parquet(f"{BASE}/analytics/features_contribuyente")
# benford_mad es NULL con < 30 CFDI (no hay muestra suficiente). Rellenarlo con 0 ("cumple Benford perfecto")
# fabricaba un cluster artificial de contribuyentes de poca actividad; se imputa con la mediana.
med_benford = feat.filter("benford_mad is not null").approxQuantile("benford_mad", [0.5], 0.01)[0]
df = (feat.filter(F.col("monto_emitido").isNotNull())
      .fillna({"benford_mad": med_benford})
      .fillna(0.0, subset=LOG_COLS + RAW_COLS))
for c in LOG_COLS:
    df = df.withColumn(c + "_log", F.log1p(F.col(c)))
cols = [c + "_log" for c in LOG_COLS] + RAW_COLS
print(f"Contribuyentes con actividad: {df.count():,}")

prep = Pipeline(stages=[
    VectorAssembler(inputCols=cols, outputCol="raw_features", handleInvalid="keep"),
    StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)]).fit(df)
X = prep.transform(df).cache()

# ---------- elegir k por silueta ----------
ev = ClusteringEvaluator(metricName="silhouette", distanceMeasure="squaredEuclidean")
res = {}
for k in [3, 4, 5, 6, 8]:
    m = KMeans(k=k, seed=42, maxIter=50, featuresCol="features").fit(X)
    res[k] = (ev.evaluate(m.transform(X)), m)
    print(f"k={k}: silueta = {res[k][0]:.4f}")
best_k = max(res, key=lambda k: res[k][0])
model = res[best_k][1]
print(f">>> k elegido = {best_k} (silueta {res[best_k][0]:.4f})")

# ---------- distancia al centroide ----------
centers = [c.tolist() for c in model.clusterCenters()]
bc = spark.sparkContext.broadcast(centers)


@F.udf("double")
def dist(v, k):
    c = bc.value[k]
    x = v.toArray()
    return float(math.sqrt(sum((p - q) ** 2 for p, q in zip(x, c))))


n_total = X.count()
pred = model.transform(X).withColumn("distancia", dist("features", "prediction"))
q = pred.approxQuantile("distancia", [a.quantile], 0.001)[0]
size = pred.groupBy("prediction").count().withColumnRenamed("count", "tam_cluster")
out = (pred.join(size, "prediction")
       .withColumn("es_outlier", (F.col("distancia") >= q).cast("int"))
       .withColumn("cluster_pequeno", (F.col("tam_cluster") < a.min_cluster * F.lit(n_total)).cast("int"))
       .withColumn("es_anomalia", F.greatest("es_outlier", "cluster_pequeno"))
       .withColumn("puntaje_anomalia", F.round(F.least(F.lit(100.0), 100.0 * F.col("distancia") / F.lit(q)), 1))
       .select("rfc", F.col("prediction").alias("cluster"), "tam_cluster", "distancia",
               "puntaje_anomalia", "es_outlier", "cluster_pequeno", "es_anomalia", "en_lista_69b"))
out.write.mode("overwrite").parquet(f"{BASE}/analytics/scores_anomalia")
sa = spark.read.parquet(f"{BASE}/analytics/scores_anomalia").cache()
print(f"Umbral de distancia (cuantil {a.quantile}): {q:.3f}")
print(f"Outliers: {sa.filter('es_outlier = 1').count():,}   en cluster pequeño: {sa.filter('cluster_pequeno = 1').count():,}   "
      f"anomalías (unión): {sa.filter('es_anomalia = 1').count():,}")

# ---------- perfil de clusters (interpretación) ----------
perfil = (pred.groupBy("prediction").agg(
    F.count("*").alias("n"), F.avg("antiguedad_dias").alias("antig"), F.avg("num_clientes").alias("clientes"),
    F.avg("pct_montos_redondos").alias("redondos"), F.avg("pct_clave_fuera_sector").alias("fuera_sector"),
    F.avg("benford_mad").alias("benford"), F.avg("pico_mensual").alias("pico"),
    F.avg("en_lista_69b").alias("pct_en_69b")).orderBy("prediction"))
perfil.write.mode("overwrite").parquet(f"{BASE}/analytics/perfil_clusters")
perfil.show(truncate=False)

# ---------- verdad terreno (solo para interpretar) ----------
try:
    gt = (spark.read.option("header", True).csv(f"{BASE}/ground_truth/efos_verdad.csv")
          .select("rfc", F.col("es_efos").cast("int").alias("es_efos")))
    j = sa.join(gt, "rfc", "left").fillna(0, subset=["es_efos"])
    tot = j.filter("es_efos = 1").count()
    base_rate = tot / max(j.count(), 1)
    print("=== VERDAD TERRENO (K-Means no la usó) ===")
    print(f"EFOS activos: {tot:,} ({base_rate:.2%} de la población)")
    for nombre, cond in [("solo outliers", "es_outlier = 1"), ("solo cluster pequeño", "cluster_pequeno = 1"),
                         ("unión (es_anomalia)", "es_anomalia = 1")]:
        flag = j.filter(cond)
        nf = flag.count()
        ne = flag.filter("es_efos = 1").count()
        prec = ne / max(nf, 1)
        print(f"{nombre:22s} marcados={nf:>7,}  EFOS={ne:>6,}  precisión={prec:.3f}  "
              f"recall={ne / max(tot, 1):.3f}  lift={prec / max(base_rate, 1e-9):.1f}x")
    print("EFOS por cluster:")
    j.groupBy("cluster").agg(F.count("*").alias("n"), F.sum("es_efos").alias("efos"),
                             F.round(F.avg("es_efos"), 4).alias("tasa_efos")).orderBy("cluster").show()
except Exception as e:
    print("(verdad terreno no disponible):", str(e)[:120])
spark.stop()
