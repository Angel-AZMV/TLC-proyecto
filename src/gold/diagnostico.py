"""
diagnostico.py
===============
Tablas Gold para los dashboards de análisis DIAGNÓSTICO.
Procesa tipo x año por separado (control de RAM).
Implementa saneamiento estricto PRE-agregación.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import sys

if '/home/jovyan/src' not in sys.path:
    sys.path.append('/home/jovyan/src')
from config import SILVER_PATH, GOLD_PATH, ANIOS, TIPOS_VEHICULO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def _escribir_gold(df: DataFrame, nombre: str, particion: list = None, modo: str = "overwrite"):
    destino = GOLD_PATH / "diagnostico" / nombre
    destino.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode(modo)
    if particion:
        writer = writer.partitionBy(*particion)
    writer.parquet(str(destino))
    log.info(f"Gold escrito ({modo}): diagnostico/{nombre} ({df.count():,} filas)")

def _preprocesar_columnas(df: DataFrame, metricas: list) -> DataFrame:
    """
    Escudo anti-errores de Java: Asegura que la columna exista y sea un número real (double)
    antes de que cualquier función matemática (como avg o sum) la toque.
    """
    for m in metricas:
        if m not in df.columns:
            df = df.withColumn(m, F.lit(0.0))
        else:
            df = df.withColumn(m, F.col(m).cast("double"))
    return df

def construir_tarifas_por_zona(spark: SparkSession):
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            
            silver = spark.read.parquet(str(path_tipo))
            
            # BLINDAJE PRE-AGREGACIÓN
            metricas = ["fare_amount", "tip_amount", "total_amount", "duracion_min"]
            silver = _preprocesar_columnas(silver, metricas)
            
            resultado = silver.filter(F.col("pu_location_id").isNotNull()) \
                .groupBy(
                    F.col("pu_location_id").alias("location_id"),
                    F.year("pickup_datetime").alias("anio"),
                    F.month("pickup_datetime").alias("mes")
                ).agg(
                    F.count("*").alias("total_viajes"),
                    F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
                    F.round(F.avg("tip_amount"), 2).alias("tip_promedio"),
                    F.round(F.avg("total_amount"), 2).alias("total_promedio"),
                    F.round(F.avg("duracion_min"), 2).alias("duracion_promedio")
                ).withColumn("tipo_vehiculo", F.lit(tipo))
            
            resultado_estricto = resultado.select(
                F.col("location_id").cast("integer"),
                F.col("anio").cast("integer"),
                F.col("mes").cast("integer"),
                F.col("total_viajes").cast("long"),
                F.col("fare_promedio").cast("double"),
                F.col("tip_promedio").cast("double"),
                F.col("total_promedio").cast("double"),
                F.col("duracion_promedio").cast("double"),
                F.col("tipo_vehiculo").cast("string")
            )
            
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado_estricto, "tarifas_por_zona", particion=["anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  tarifas_por_zona: {tipo} {anio} listo")

def construir_comparacion_vehiculos(spark: SparkSession):
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            
            silver = spark.read.parquet(str(path_tipo))
            
            # BLINDAJE PRE-AGREGACIÓN
            metricas = ["fare_amount", "tip_amount", "trip_distance", "duracion_min", "passenger_count"]
            silver = _preprocesar_columnas(silver, metricas)
            
            resultado = silver.groupBy(
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes")
            ).agg(
                F.count("*").alias("total_viajes"),
                F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
                F.round(F.avg("tip_amount"), 2).alias("tip_promedio"),
                F.round(F.avg("trip_distance"), 2).alias("distancia_promedio"),
                F.round(F.avg("duracion_min"), 2).alias("duracion_promedio"),
                F.round(F.avg("passenger_count"), 2).alias("pasajeros_promedio"),
                F.round(F.sum("fare_amount"), 2).alias("ingreso_total")
            ).withColumn("tipo_vehiculo", F.lit(tipo))
            
            resultado_estricto = resultado.select(
                F.col("tipo_vehiculo").cast("string"),
                F.col("anio").cast("integer"),
                F.col("mes").cast("integer"),
                F.col("total_viajes").cast("long"),
                F.col("fare_promedio").cast("double"),
                F.col("tip_promedio").cast("double"),
                F.col("distancia_promedio").cast("double"),
                F.col("duracion_promedio").cast("double"),
                F.col("pasajeros_promedio").cast("double"),
                F.col("ingreso_total").cast("double")
            )
            
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado_estricto, "comparacion_vehiculos", particion=["anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  comparacion_vehiculos: {tipo} {anio} listo")

def construir_duracion_distancia(spark: SparkSession):
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            
            silver = spark.read.parquet(str(path_tipo))
            
            # BLINDAJE PRE-AGREGACIÓN
            metricas = ["trip_distance", "duracion_min", "fare_amount"]
            silver = _preprocesar_columnas(silver, metricas)
            
            resultado = silver.groupBy(
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes"),
                "hora"
            ).agg(
                F.round(F.avg("trip_distance"), 2).alias("distancia_promedio"),
                F.round(F.avg("duracion_min"), 2).alias("duracion_promedio"),
                F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
                F.count("*").alias("total_viajes")
            ).withColumn("tipo_vehiculo", F.lit(tipo))
            
            resultado_estricto = resultado.select(
                F.col("tipo_vehiculo").cast("string"),
                F.col("anio").cast("integer"),
                F.col("mes").cast("integer"),
                F.col("hora").cast("integer"),
                F.col("distancia_promedio").cast("double"),
                F.col("duracion_promedio").cast("double"),
                F.col("fare_promedio").cast("double"),
                F.col("total_viajes").cast("long")
            )
            
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado_estricto, "duracion_distancia", particion=["anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  duracion_distancia: {tipo} {anio} listo")

def construir_todo_diagnostico(spark: SparkSession):
    construir_tarifas_por_zona(spark)
    construir_comparacion_vehiculos(spark)
    construir_duracion_distancia(spark)
    log.info("Gold diagnóstico completo")