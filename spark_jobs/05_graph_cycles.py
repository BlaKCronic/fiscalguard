"""
FiscalGuard - Fase 3 / Job 5: detección de facturación circular (A -> B -> C -> A).

curated/cfdi -> analytics/ciclos, analytics/redes, analytics/nodos_en_ciclo

Método (solo DataFrames de Spark, sin dependencias extra):
  1. Aristas dirigidas emisor->receptor con >= --min-cfdi comprobantes vigentes (filtra ruido).
  2. Búsqueda de ciclos de longitud 3..--max-len por extensión de rutas; cada ciclo se reporta
     una sola vez (se arranca desde su RFC mínimo).
  3. Redes = componentes conexas de los ciclos que comparten RFC (propagación de etiqueta mínima).
  monto_ratio = arista menor / arista mayor del ciclo: cercano a 1 = montos casi idénticos (sospechoso).

Uso: spark-submit --driver-memory 2g ... 05_graph_cycles.py [BASE] [--min-cfdi 10] [--max-len 6]
"""
import argparse
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("base", nargs="?", default="hdfs://namenode:9000/fiscalguard")
ap.add_argument("--min-cfdi", type=int, default=10)
ap.add_argument("--max-len", type=int, default=6)
a = ap.parse_args()
BASE = a.base

spark = (SparkSession.builder.appName("fiscalguard-ciclos")
         .config("spark.sql.shuffle.partitions", "24")
         .config("spark.sql.maxPlanStringLength", "4000")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

cfdi = spark.read.parquet(f"{BASE}/curated/cfdi").filter("tipo_comprobante = 'I' and estatus = 'Vigente'")
edges = (cfdi.groupBy("rfc_emisor", "rfc_receptor")
         .agg(F.count("*").alias("n"), F.sum("subtotal").alias("monto"))
         .filter(F.col("n") >= a.min_cfdi).filter("rfc_emisor <> rfc_receptor")
         .select(F.col("rfc_emisor").alias("src"), F.col("rfc_receptor").alias("dst"), "n", "monto").cache())
print(f"Aristas con >= {a.min_cfdi} CFDI: {edges.count():,}")

e = edges.select(F.col("src").alias("e_src"), F.col("dst").alias("e_dst"), F.col("monto").alias("e_m"))
paths = (edges.filter("src < dst")
         .select(F.col("src").alias("start"), F.col("dst").alias("cur"), F.array("src", "dst").alias("path"),
                 F.col("monto").alias("m_sum"), F.col("monto").alias("m_min"), F.col("monto").alias("m_max")))

cycles = None
for L in range(2, a.max_len + 1):
    j = paths.join(e, F.col("cur") == F.col("e_src"))
    if L >= 3:  # cerrar ciclo: la arista vuelve al nodo de inicio
        closed = (j.filter(F.col("e_dst") == F.col("start"))
                  .select("path", (F.col("m_sum") + F.col("e_m")).alias("m_sum"),
                          F.least("m_min", "e_m").alias("m_min"), F.greatest("m_max", "e_m").alias("m_max")))
        cycles = closed if cycles is None else cycles.union(closed)
    if L == a.max_len:
        break
    paths = (j.filter((F.col("e_dst") > F.col("start")) & (~F.expr("array_contains(path, e_dst)")))
             .select("start", F.col("e_dst").alias("cur"), F.concat("path", F.array("e_dst")).alias("path"),
                     (F.col("m_sum") + F.col("e_m")).alias("m_sum"),
                     F.least("m_min", "e_m").alias("m_min"), F.greatest("m_max", "e_m").alias("m_max")).cache())
    print(f"  rutas de longitud {L + 1}: {paths.count():,}")

if cycles is None:
    print("max-len < 3: nada que buscar")
    spark.stop()
    raise SystemExit
cycles = (cycles.withColumn("ciclo_id", F.md5(F.concat_ws("|", "path")))
          .dropDuplicates(["ciclo_id"])
          .select("ciclo_id", F.col("path").alias("nodos"), F.size("path").alias("longitud"),
                  F.col("m_sum").alias("monto_total"), (F.col("m_min") / F.col("m_max")).alias("monto_ratio")))
# se escribe y relee para cortar el linaje (si no, el plan crece y el driver se queda sin memoria)
cycles.write.mode("overwrite").parquet(f"{BASE}/analytics/_tmp_ciclos")
cycles = spark.read.parquet(f"{BASE}/analytics/_tmp_ciclos").cache()
n_cyc = cycles.count()
print(f"Ciclos encontrados: {n_cyc:,}")
if n_cyc == 0:
    spark.stop()
    raise SystemExit

# ---------- redes = componentes conexas (etiqueta mínima) ----------
members = cycles.select("ciclo_id", F.explode("nodos").alias("rfc")).cache()
label = members.select("rfc").distinct().withColumn("lbl", F.col("rfc")).localCheckpoint()
for it in range(20):
    cyc_lbl = members.join(label, "rfc").groupBy("ciclo_id").agg(F.min("lbl").alias("cl"))
    new = members.join(cyc_lbl, "ciclo_id").groupBy("rfc").agg(F.min("cl").alias("lbl")).localCheckpoint()
    changed = new.alias("n").join(label.alias("o"), "rfc").filter(F.col("n.lbl") != F.col("o.lbl")).count()
    label = new
    if changed == 0:
        break
print(f"Propagación de etiquetas: {it + 1} iteraciones")
red_id = F.concat(F.lit("RED_"), F.substring(F.md5("lbl"), 1, 8))
nodos = (members.groupBy("rfc").agg(F.countDistinct("ciclo_id").alias("num_ciclos"))
         .join(label, "rfc").withColumn("id_red", red_id).drop("lbl"))
ciclos = (cycles.join(members.join(nodos.select("rfc", "id_red"), "rfc").groupBy("ciclo_id")
                      .agg(F.min("id_red").alias("id_red")), "ciclo_id"))
aristas_int = (edges.join(nodos.select(F.col("rfc").alias("src"), F.col("id_red").alias("r1")), "src")
               .join(nodos.select(F.col("rfc").alias("dst"), F.col("id_red").alias("r2")), "dst")
               .filter("r1 = r2").groupBy(F.col("r1").alias("id_red")).agg(F.sum("monto").alias("monto_aristas_internas")))
redes = (nodos.groupBy("id_red").agg(F.collect_set("rfc").alias("rfcs"), F.count("*").alias("num_nodos"))
         .join(ciclos.groupBy("id_red").agg(F.count("*").alias("num_ciclos")), "id_red")
         .join(aristas_int, "id_red", "left"))

nodos.write.mode("overwrite").parquet(f"{BASE}/analytics/nodos_en_ciclo")
ciclos.write.mode("overwrite").parquet(f"{BASE}/analytics/ciclos")
redes.write.mode("overwrite").parquet(f"{BASE}/analytics/redes")
nodos = spark.read.parquet(f"{BASE}/analytics/nodos_en_ciclo").cache()
redes = spark.read.parquet(f"{BASE}/analytics/redes").cache()
print(f"Redes: {redes.count():,}   RFC en ciclos: {nodos.count():,}")
redes.select("id_red", "num_nodos", "num_ciclos", "monto_aristas_internas").orderBy(F.desc("monto_aristas_internas")).show(10, truncate=False)

# ---------- cruce con el clasificador ----------
try:
    sc = spark.read.parquet(f"{BASE}/analytics/scores_clasificador").select("rfc", "nivel_riesgo", "en_lista_69b")
    print("RFC en ciclos según el clasificador:")
    nodos.join(sc, "rfc", "left").groupBy("nivel_riesgo", "en_lista_69b").count().orderBy("nivel_riesgo").show()
except Exception as ex:
    print("(sin scores del clasificador):", str(ex)[:100])

# ---------- verdad terreno ----------
try:
    gt = (spark.read.option("header", True).csv(f"{BASE}/ground_truth/redes_verdad.csv")
          .select(F.col("id_red").alias("red_real"), "rfc"))
    true_nodes = gt.select("rfc").distinct()
    n_true = true_nodes.count()
    n_det = nodos.count()
    tp = true_nodes.join(nodos.select("rfc"), "rfc").count()
    print("=== VERDAD TERRENO (redes circulares inyectadas) ===")
    print(f"RFC reales en anillos: {n_true:,}   detectados: {tp:,}  -> recall de nodos {tp / max(n_true, 1):.3f}")
    print(f"Precisión de nodos (detectados que sí son de un anillo): {tp / max(n_det, 1):.3f}")
    per = (gt.join(nodos.select("rfc", "id_red"), "rfc", "left").groupBy("red_real")
           .agg(F.count("*").alias("miembros"), F.count("id_red").alias("detectados")))
    n_rings = per.count()
    n_full = per.filter("miembros = detectados").count()
    print(f"Anillos reales: {n_rings:,}   con todos sus miembros detectados: {n_full:,} ({n_full / max(n_rings, 1):.1%})")
except Exception as ex:
    print("(redes_verdad no disponible):", str(ex)[:120])
spark.stop()
