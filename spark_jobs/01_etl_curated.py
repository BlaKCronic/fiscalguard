"""
FiscalGuard - Fase 2 / Job 1: ETL raw -> curated.

  raw/cfdi (CSV)       -> curated/cfdi (Parquet particionado por anio/mes)
  raw/padron (CSV)     -> curated/padron (Parquet)
  raw/lista_69b (CSV)  -> curated/lista_69b (Parquet)
Registros inválidos -> curated/cfdi_rechazados (con motivo). Reporte de calidad -> curated/_calidad_etl.

Uso:  spark-submit --master spark://spark-master:7077 /opt/jobs/01_etl_curated.py [BASE]
      BASE por defecto: hdfs://namenode:9000/fiscalguard
"""
import sys
from pyspark.sql import SparkSession, functions as F, types as T

BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000/fiscalguard"
RFC_RE = r"^[A-ZÑ&]{3,4}[0-9]{6}[A-Z0-9]{3}$"

spark = (SparkSession.builder.appName("fiscalguard-etl-curated")
         .config("spark.sql.shuffle.partitions", "24")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())

CFDI_SCHEMA = T.StructType([
    T.StructField("uuid", T.StringType()),
    T.StructField("rfc_emisor", T.StringType()),
    T.StructField("rfc_receptor", T.StringType()),
    T.StructField("fecha", T.StringType()),
    T.StructField("subtotal", T.DoubleType()),
    T.StructField("iva", T.DoubleType()),
    T.StructField("total", T.DoubleType()),
    T.StructField("tipo_comprobante", T.StringType()),
    T.StructField("uso_cfdi", T.StringType()),
    T.StructField("forma_pago", T.StringType()),
    T.StructField("clave_prod_serv", T.StringType()),
    T.StructField("estatus", T.StringType()),
])

# ---------------- CFDI ----------------
raw = (spark.read.option("header", True).schema(CFDI_SCHEMA).csv(f"{BASE}/raw/cfdi")
       .withColumn("rfc_emisor", F.upper(F.trim("rfc_emisor")))
       .withColumn("rfc_receptor", F.upper(F.trim("rfc_receptor")))
       .withColumn("fecha", F.to_timestamp("fecha", "yyyy-MM-dd'T'HH:mm:ss")))

# primer motivo de rechazo que aplique (null = registro válido)
motivo = (F.when(F.col("uuid").isNull() | (F.length("uuid") != 36), "uuid_invalido")
           .when(~F.col("rfc_emisor").rlike(RFC_RE), "rfc_emisor_invalido")
           .when(~F.col("rfc_receptor").rlike(RFC_RE), "rfc_receptor_invalido")
           .when(F.col("fecha").isNull(), "fecha_invalida")
           .when(F.col("subtotal").isNull() | (F.col("subtotal") <= 0), "monto_invalido")
           .when(F.col("total") < F.col("subtotal"), "total_menor_subtotal")
           .when(~F.col("tipo_comprobante").isin("I", "E"), "tipo_invalido")
           .when(~F.col("estatus").isin("Vigente", "Cancelado"), "estatus_invalido"))
marked = raw.withColumn("motivo_rechazo", motivo).cache()

rechazados = marked.filter("motivo_rechazo is not null")
validos = marked.filter("motivo_rechazo is null").drop("motivo_rechazo")
curated = (validos.dropDuplicates(["uuid"])
           .withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha")))

rechazados.write.mode("overwrite").parquet(f"{BASE}/curated/cfdi_rechazados")
# una carpeta por anio/mes; repartition evita miles de archivos pequeños
(curated.repartition("anio", "mes").write.mode("overwrite")
 .partitionBy("anio", "mes").parquet(f"{BASE}/curated/cfdi"))

# ---------------- Padrón y lista 69-B ----------------
padron = (spark.read.option("header", True).csv(f"{BASE}/raw/padron")
          .withColumn("rfc", F.upper(F.trim("rfc")))
          .filter(F.col("rfc").rlike(RFC_RE)).dropDuplicates(["rfc"])
          .withColumn("fecha_inicio_operaciones", F.to_date("fecha_inicio_operaciones")))
padron.write.mode("overwrite").parquet(f"{BASE}/curated/padron")

l69 = (spark.read.option("header", True).csv(f"{BASE}/raw/lista_69b")
       .withColumn("rfc", F.upper(F.trim("rfc")))
       .filter(F.col("rfc").rlike(RFC_RE)).dropDuplicates(["rfc"])
       .withColumn("fecha_publicacion", F.to_date("fecha_publicacion")))
l69.write.mode("overwrite").parquet(f"{BASE}/curated/lista_69b")

# ---------------- Reporte de calidad ----------------
n_in = marked.count()
n_rej = rechazados.count()
n_val = n_in - n_rej
n_out = spark.read.parquet(f"{BASE}/curated/cfdi").count()
print("=" * 60)
print(f"CFDI leídos:               {n_in:>12,}")
print(f"Rechazados (inválidos):    {n_rej:>12,}")
print(f"Duplicados por UUID:       {n_val - n_out:>12,}")
print(f"CFDI en curated:           {n_out:>12,}")
print("Rechazos por motivo:")
rechazados.groupBy("motivo_rechazo").count().orderBy(F.desc("count")).show(truncate=False)
print(f"Padrón: {padron.count():,}   Lista 69-B: {l69.count():,}")
print("=" * 60)

calidad = spark.createDataFrame(
    [("cfdi_leidos", n_in), ("cfdi_rechazados", n_rej),
     ("cfdi_duplicados", n_val - n_out), ("cfdi_curated", n_out)], ["metrica", "valor"])
calidad.write.mode("overwrite").parquet(f"{BASE}/curated/_calidad_etl")
spark.stop()
