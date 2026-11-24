"""
FiscalGuard - Fase 2 / Job 2: features por contribuyente.

curated/{cfdi,padron,lista_69b} -> analytics/features_contribuyente (Parquet, una fila por RFC)

Features (como emisor): monto_emitido, num_cfdi_emitidos, num_clientes, hhi_clientes,
  pct_cancelados, pct_montos_redondos, pico_mensual, cv_mensual, meses_activos,
  benford_mad, pct_clave_fuera_sector, antiguedad_dias, monto_por_dia_antiguedad,
  num_contrapartes_69b, pct_monto_con_69b.
  v4 (periodos largos, p. ej. 2014-2025): antiguedad_dias = días entre su ALTA y su PRIMER CFDI
  emitido (una empresa que factura recién creada), ya no días hasta el fin del periodo, que con
  12 años dejaba de distinguir a una EFOS de 2015. monto_por_dia_antiguedad = monto emitido por
  día entre su primer y su último CFDI (intensidad de facturación).
Features (como receptor): monto_recibido, num_cfdi_recibidos, num_proveedores.
Columna `en_lista_69b` = ETIQUETA de entrenamiento: NO usarla como feature en la Fase 3.
"""
import sys
from pyspark.sql import SparkSession, Window, functions as F

BASE = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000/fiscalguard"
spark = (SparkSession.builder.appName("fiscalguard-features")
         .config("spark.sql.shuffle.partitions", "24").getOrCreate())

cfdi = spark.read.parquet(f"{BASE}/curated/cfdi").filter("tipo_comprobante = 'I'")
padron = spark.read.parquet(f"{BASE}/curated/padron")
l69 = spark.read.parquet(f"{BASE}/curated/lista_69b").select("rfc").withColumn("es69", F.lit(True))

fecha_ref = cfdi.agg(F.max("fecha")).first()[0].date()  # "hoy" del conjunto de datos

# ---------- Sector de cada emisor y claves típicas por sector (data-driven) ----------
sector = padron.select(F.col("rfc").alias("rfc_emisor"), "sector")
base = cfdi.join(sector, "rfc_emisor", "left")

top_claves = (base.groupBy("sector", "clave_prod_serv").count()
              .withColumn("rk", F.row_number().over(Window.partitionBy("sector").orderBy(F.desc("count"))))
              .filter("rk <= 5").select("sector", "clave_prod_serv", F.lit(1).alias("en_top")))
base = (base.join(top_claves, ["sector", "clave_prod_serv"], "left")
        .withColumn("fuera_sector", F.when(F.col("en_top").isNull(), 1).otherwise(0)))

# primer dígito (Benford) y monto redondo
base = (base.withColumn("d1", F.floor(F.col("subtotal") / F.pow(F.lit(10.0), F.floor(F.log10("subtotal")))).cast("int"))
        .withColumn("redondo", F.when((F.col("subtotal") >= 10000) & (F.col("subtotal") % 10000 == 0), 1).otherwise(0))
        .withColumn("cancelado", F.when(F.col("estatus") == "Cancelado", 1).otherwise(0)))

# ---------- Agregados por emisor ----------
digit_cols = [F.sum(F.when(F.col("d1") == d, 1).otherwise(0)).alias(f"d{d}") for d in range(1, 10)]
agg = base.groupBy("rfc_emisor").agg(
    F.min("fecha").alias("primer_cfdi"), F.max("fecha").alias("ultimo_cfdi"),
    F.sum("subtotal").alias("monto_emitido"),
    F.count("*").alias("num_cfdi_emitidos"),
    F.countDistinct("rfc_receptor").alias("num_clientes"),
    F.avg("cancelado").alias("pct_cancelados"),
    F.avg("redondo").alias("pct_montos_redondos"),
    F.avg("fuera_sector").alias("pct_clave_fuera_sector"),
    *digit_cols)

import math
mad = sum(F.abs(F.col(f"d{d}") / F.col("num_cfdi_emitidos") - F.lit(math.log10(1 + 1 / d))) for d in range(1, 10)) / 9
agg = (agg.withColumn("benford_mad", F.when(F.col("num_cfdi_emitidos") >= 30, mad))
       .drop(*[f"d{d}" for d in range(1, 10)]))

# concentración de clientes (HHI sobre participación del monto)
pares = base.groupBy("rfc_emisor", "rfc_receptor").agg(F.sum("subtotal").alias("m"))
tot = pares.groupBy("rfc_emisor").agg(F.sum("m").alias("t"))
hhi = (pares.join(tot, "rfc_emisor")
       .groupBy("rfc_emisor").agg(F.sum((F.col("m") / F.col("t")) ** 2).alias("hhi_clientes")))

# comportamiento mensual
mensual = base.groupBy("rfc_emisor", "anio", "mes").agg(F.sum("subtotal").alias("m"))
mens = mensual.groupBy("rfc_emisor").agg(
    F.count("*").alias("meses_activos"), F.max("m").alias("mx"),
    F.avg("m").alias("mu"), F.stddev("m").alias("sd"))
mens = (mens.withColumn("pico_mensual", F.col("mx") / F.col("mu"))
        .withColumn("cv_mensual", F.coalesce(F.col("sd") / F.col("mu"), F.lit(0.0)))
        .select("rfc_emisor", "meses_activos", "pico_mensual", "cv_mensual"))

# conexión con contribuyentes de la lista 69-B (excluye al propio RFC)
a = (base.join(l69.withColumnRenamed("rfc", "rfc_receptor").withColumnRenamed("es69", "r69"), "rfc_receptor")
     .groupBy("rfc_emisor").agg(F.countDistinct("rfc_receptor").alias("num_contrapartes_69b"),
                                F.sum("subtotal").alias("monto_con_69b")))

# ---------- Lado receptor ----------
recib = cfdi.groupBy(F.col("rfc_receptor").alias("rfc")).agg(
    F.sum("subtotal").alias("monto_recibido"), F.count("*").alias("num_cfdi_recibidos"),
    F.countDistinct("rfc_emisor").alias("num_proveedores"))

# ---------- Ensamble ----------
feat = (padron.select("rfc", "tipo_persona", "regimen", "sector", "estado", "fecha_inicio_operaciones")
        .join(agg.withColumnRenamed("rfc_emisor", "rfc"), "rfc", "left")
        .join(hhi.withColumnRenamed("rfc_emisor", "rfc"), "rfc", "left")
        .join(mens.withColumnRenamed("rfc_emisor", "rfc"), "rfc", "left")
        .join(a.withColumnRenamed("rfc_emisor", "rfc"), "rfc", "left")
        .join(recib, "rfc", "left")
        .join(l69.withColumnRenamed("es69", "en_lista_69b"), "rfc", "left"))
feat = (feat.withColumn("antiguedad_dias", F.greatest(F.lit(0), F.datediff(
            F.coalesce("primer_cfdi", F.lit(fecha_ref)), "fecha_inicio_operaciones")))
        .withColumn("monto_por_dia_antiguedad",
                    F.col("monto_emitido") / (F.datediff("ultimo_cfdi", "primer_cfdi") + 1))
        .drop("primer_cfdi", "ultimo_cfdi")
        .withColumn("pct_monto_con_69b", F.coalesce(F.col("monto_con_69b") / F.col("monto_emitido"), F.lit(0.0)))
        .withColumn("num_contrapartes_69b", F.coalesce("num_contrapartes_69b", F.lit(0)))
        .withColumn("en_lista_69b", F.coalesce("en_lista_69b", F.lit(False)).cast("int"))
        .drop("monto_con_69b")
        .withColumn("periodo_fin", F.lit(str(fecha_ref))))

feat.write.mode("overwrite").parquet(f"{BASE}/analytics/features_contribuyente")

out = spark.read.parquet(f"{BASE}/analytics/features_contribuyente")
print(f"Features: {out.count():,} contribuyentes, {len(out.columns)} columnas")
out.printSchema()
(out.groupBy("en_lista_69b")
 .agg(F.count("*").alias("n"), F.avg("antiguedad_dias").alias("antig_prom"),
      F.avg("num_clientes").alias("clientes_prom"), F.avg("hhi_clientes").alias("hhi_prom"),
      F.avg("pct_montos_redondos").alias("redondos_prom"), F.avg("pct_clave_fuera_sector").alias("fuera_sector_prom"),
      F.avg("benford_mad").alias("benford_mad_prom")).show(truncate=False))
spark.stop()
