"""Prueba de humo: Spark lee la zona raw desde HDFS."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fiscalguard-smoke").getOrCreate()
base = "hdfs://namenode:9000/fiscalguard/raw"
cfdi = spark.read.option("header", True).option("inferSchema", True).csv(f"{base}/cfdi")
print("CFDI cargados:", cfdi.count())
cfdi.groupBy("estatus").count().show()
spark.read.option("header", True).csv(f"{base}/lista_69b").show(5)
spark.stop()
