"""
FiscalGuard - Fase 3 / Job 3: clasificador de EFOS (Random Forest y GBT, Spark MLlib).

analytics/features_contribuyente -> models/clasificador_efos + analytics/scores_clasificador
                                    + analytics/importancia_variables

- Etiqueta: en_lista_69b (NO es feature). Split estratificado 70/15/15.
- Desbalance: pesos de clase (weightCol). Validación cruzada (3 folds) optimizando AUC-PR.
- Umbral: el más alto que logre recall >= 0.85 en validación; se reporta en test.
- Puntaje 0-100 = probabilidad x 100. Nivel: alto (p >= umbral), medio (p >= umbral/2), bajo.
- Si existe {BASE}/ground_truth/efos_verdad.csv se mide también contra la verdad terreno
  (solo para evaluar; jamás se usa para entrenar).

Uso: spark-submit ... 03_train_classifier.py [BASE] [--no-cv]
"""
import sys
import numpy as np
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.functions import vector_to_array

args = [a for a in sys.argv[1:] if not a.startswith("--")]
BASE = args[0] if args else "hdfs://namenode:9000/fiscalguard"
USE_CV = "--no-cv" not in sys.argv
SEED, TARGET_RECALL = 42, 0.85

spark = (SparkSession.builder.appName("fiscalguard-clasificador")
         .config("spark.sql.shuffle.partitions", "24").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

NUM = ["monto_emitido", "num_cfdi_emitidos", "num_clientes", "pct_cancelados", "pct_montos_redondos",
       "pct_clave_fuera_sector", "benford_mad", "hhi_clientes", "meses_activos", "pico_mensual",
       "cv_mensual", "num_contrapartes_69b", "pct_monto_con_69b", "monto_recibido",
       "num_cfdi_recibidos", "num_proveedores", "antiguedad_dias", "monto_por_dia_antiguedad"]
FEATS = NUM + ["sin_actividad"]

feat = spark.read.parquet(f"{BASE}/analytics/features_contribuyente")
df = (feat.withColumn("sin_actividad", F.when(F.col("monto_emitido").isNull(), 1.0).otherwise(0.0))
      .fillna(0.0, subset=NUM)
      .withColumn("label", F.col("en_lista_69b").cast("double")))

# ---------- split estratificado 70/15/15 ----------
p_tr, p_va, p_te = df.filter("label = 1").randomSplit([0.7, 0.15, 0.15], SEED)
n_tr, n_va, n_te = df.filter("label = 0").randomSplit([0.7, 0.15, 0.15], SEED)
train, val, test = p_tr.union(n_tr), p_va.union(n_va), p_te.union(n_te)
npos = train.filter("label = 1").count()
nneg = train.count() - npos
peso = nneg / max(npos, 1)
train = train.withColumn("peso", F.when(F.col("label") == 1, F.lit(peso)).otherwise(F.lit(1.0))).cache()
print(f"Train: {npos:,} positivos / {nneg:,} negativos (peso clase positiva = {peso:.1f}); "
      f"val={val.count():,} test={test.count():,}")

asm = VectorAssembler(inputCols=FEATS, outputCol="features", handleInvalid="keep")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="peso", seed=SEED)
gbt = GBTClassifier(labelCol="label", featuresCol="features", weightCol="peso", seed=SEED)
ev_pr = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
ev_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

grids = {
    "RandomForest": (rf, ParamGridBuilder().addGrid(rf.numTrees, [50, 100]).addGrid(rf.maxDepth, [6, 10]).build()),
    "GBT": (gbt, ParamGridBuilder().addGrid(gbt.maxIter, [30, 60]).addGrid(gbt.maxDepth, [3, 5]).build()),
}

fitted = {}
for name, (clf, grid) in grids.items():
    pipe = Pipeline(stages=[asm, clf])
    if USE_CV:
        cv = CrossValidator(estimator=pipe, estimatorParamMaps=grid, evaluator=ev_pr,
                            numFolds=3, seed=SEED, parallelism=1)
        model = cv.fit(train).bestModel
    else:
        model = pipe.fit(train)
    fitted[name] = model
    print(f"{name}: AUC-PR val = {ev_pr.evaluate(model.transform(val)):.4f}   "
          f"AUC-ROC val = {ev_roc.evaluate(model.transform(val)):.4f}")

best_name = max(fitted, key=lambda n: ev_pr.evaluate(fitted[n].transform(val)))
best = fitted[best_name]
print(f"\n>>> Modelo elegido (AUC-PR en validación): {best_name}")


def probs(model, d):
    return model.transform(d).select(vector_to_array("probability")[1].alias("p"), "label").toPandas()


def metrics(p, y, t):
    pred = p >= t
    tp = int((pred & (y == 1)).sum()); fp = int((pred & (y == 0)).sum()); fn = int((~pred & (y == 1)).sum())
    rec = tp / max(tp + fn, 1); prec = tp / max(tp + fp, 1)
    return dict(recall=rec, precision=prec, f1=2 * rec * prec / max(rec + prec, 1e-9), tp=tp, fp=fp, fn=fn)


pv = probs(best, val)
thr = 0.02
for t in np.arange(0.02, 0.99, 0.01):
    if metrics(pv.p.values, pv.label.values, t)["recall"] >= TARGET_RECALL:
        thr = float(round(t, 2))
mv = metrics(pv.p.values, pv.label.values, thr)
pt = probs(best, test)
mt = metrics(pt.p.values, pt.label.values, thr)
print(f"Umbral (recall>={TARGET_RECALL:.0%} en validación): {thr}")
print(f"VALIDACIÓN: recall={mv['recall']:.3f} precisión={mv['precision']:.3f}")
print(f"TEST      : recall={mt['recall']:.3f} precisión={mt['precision']:.3f} F1={mt['f1']:.3f} "
      f"(TP={mt['tp']} FP={mt['fp']} FN={mt['fn']})")
print(f"TEST      : AUC-ROC={ev_roc.evaluate(best.transform(test)):.4f}  AUC-PR={ev_pr.evaluate(best.transform(test)):.4f}")

# ---------- importancia de variables ----------
imp = sorted(zip(FEATS, best.stages[-1].featureImportances.toArray().tolist()), key=lambda x: -x[1])
print("\nImportancia de variables:")
for n, v in imp:
    print(f"  {n:28s} {v:.4f}")
spark.createDataFrame(imp, ["variable", "importancia"]).write.mode("overwrite").parquet(f"{BASE}/analytics/importancia_variables")

# ---------- guardar modelo y puntuar a todos los contribuyentes ----------
best.write().overwrite().save(f"{BASE}/models/clasificador_efos")
scores = (best.transform(df)
          .withColumn("prob", vector_to_array("probability")[1])
          .withColumn("puntaje_riesgo", F.round(F.col("prob") * 100, 1))
          .withColumn("nivel_riesgo", F.when(F.col("prob") >= thr, "alto")
                      .when(F.col("prob") >= thr / 2, "medio").otherwise("bajo"))
          .select("rfc", "sector", "estado", "en_lista_69b", "prob", "puntaje_riesgo", "nivel_riesgo")
          .withColumn("modelo", F.lit(best_name)).withColumn("umbral", F.lit(thr)))
scores.write.mode("overwrite").parquet(f"{BASE}/analytics/scores_clasificador")
sc = spark.read.parquet(f"{BASE}/analytics/scores_clasificador").cache()
print("\nDistribución por nivel y estatus en 69-B:")
sc.groupBy("nivel_riesgo", "en_lista_69b").count().orderBy("nivel_riesgo", "en_lista_69b").show()
print("Candidatos NUEVOS (riesgo alto y NO están en la lista 69-B):",
      sc.filter("nivel_riesgo = 'alto' and en_lista_69b = 0").count())

# ---------- evaluación contra verdad terreno (opcional) ----------
try:
    gt = (spark.read.option("header", True).csv(f"{BASE}/ground_truth/efos_verdad.csv")
          .select("rfc", F.col("es_efos").cast("int").alias("es_efos")))
    j = sc.join(gt, "rfc", "left").fillna(0, subset=["es_efos"])
    tot = j.filter("es_efos = 1").count()
    sin = j.filter("es_efos = 1 and en_lista_69b = 0")
    n_sin = sin.count()
    alto = j.filter("nivel_riesgo = 'alto'")
    n_alto = alto.count()
    n_alto_efos = alto.filter("es_efos = 1").count()
    n_sin_alto = sin.filter("nivel_riesgo = 'alto'").count()
    n_sin_alto_medio = sin.filter("nivel_riesgo in ('alto', 'medio')").count()
    print("\n=== VERDAD TERRENO ===")
    print(f"EFOS reales: {tot:,}  (listados: {tot - n_sin:,}, NO listados: {n_sin:,})")
    print(f"Recall global nivel alto:                 {n_alto_efos / max(tot, 1):.3f}")
    print(f"Recall EFOS NO listados (alto):           {n_sin_alto / max(n_sin, 1):.3f}")
    print(f"Recall EFOS NO listados (alto o medio):   {n_sin_alto_medio / max(n_sin, 1):.3f}")
    print(f"Precisión real de las alertas 'alto':     {n_alto_efos / max(n_alto, 1):.3f}")
except Exception as e:
    print("\n(verdad terreno no disponible, se omite esa evaluación):", str(e)[:120])
spark.stop()
