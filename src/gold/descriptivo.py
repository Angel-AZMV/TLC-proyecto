"""
descriptivo.py
===============
Tablas Gold para los dashboards de análisis DESCRIPTIVO.
Procesa tipo x año por separado para evitar saturar memoria.
Granularidad: día para mayor detalle en dashboards.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
sys.path.append('/home/jovyan/src')
from config import SILVER_PATH, GOLD_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _escribir_gold(df: DataFrame, nombre: str, particion: list = None, modo: str = "overwrite"):
    destino = GOLD_PATH / "descriptivo" / nombre
    destino.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode(modo)
    if particion:
        writer = writer.partitionBy(*particion)
    writer.parquet(str(destino))
    log.info(f"Gold escrito ({modo}): descriptivo/{nombre} ({df.count():,} filas)")


def construir_viajes_por_hora(spark: SparkSession):
    """
    Dashboard 1 — Viajes por hora del día con granularidad diaria.
    Permite filtrar por fecha exacta, mes y año en Power BI.
    """
    from config import ANIOS, TIPOS_VEHICULO
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            silver = spark.read.parquet(str(path_tipo))
            resultado = silver.groupBy(
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes"),
                F.dayofmonth("pickup_datetime").alias("dia"),
                "hora"
            ).agg(
                F.count("*").alias("total_viajes"),
                F.round(F.avg("duracion_min"), 2).alias("duracion_promedio_min"),
                F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
            ).withColumn("tipo_vehiculo", F.lit(tipo))
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado, "viajes_por_hora", particion=["anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  viajes_por_hora: {tipo} {anio} listo")


def construir_viajes_por_zona(spark: SparkSession):
    """
    Dashboard 2 — Ranking de zonas con granularidad diaria.
    """
    from config import ANIOS, TIPOS_VEHICULO
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            silver = spark.read.parquet(str(path_tipo))
            por_origen = silver.groupBy(
                F.col("pu_location_id").alias("location_id"),
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes"),
                F.dayofmonth("pickup_datetime").alias("dia"),
            ).agg(F.count("*").alias("total_viajes_origen"))
            por_destino = silver.groupBy(
                F.col("do_location_id").alias("location_id"),
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes"),
                F.dayofmonth("pickup_datetime").alias("dia"),
            ).agg(F.count("*").alias("total_viajes_destino"))
            resultado = por_origen.join(
                por_destino, on=["location_id", "anio", "mes", "dia"], how="outer"
            ).fillna(0, subset=["total_viajes_origen", "total_viajes_destino"]) \
             .withColumn("tipo_vehiculo", F.lit(tipo))
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado, "viajes_por_zona", particion=["anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  viajes_por_zona: {tipo} {anio} listo")


def construir_tendencia_mensual(spark: SparkSession):
    """
    Dashboard 3 — Tendencia mensual de viajes y tarifas.
    """
    from config import ANIOS, TIPOS_VEHICULO
    primero = True
    for anio in ANIOS:
        for tipo in TIPOS_VEHICULO:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            silver = spark.read.parquet(str(path_tipo))
            resultado = silver.groupBy(
                F.year("pickup_datetime").alias("anio"),
                F.month("pickup_datetime").alias("mes"),
            ).agg(
                F.count("*").alias("total_viajes"),
                F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
                F.round(F.sum("fare_amount"), 2).alias("ingreso_total"),
                F.round(F.avg("trip_distance"), 2).alias("distancia_promedio"),
                F.round(F.avg("duracion_min"), 2).alias("duracion_promedio_min"),
            ).withColumn("tipo_vehiculo", F.lit(tipo))
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado, "tendencia_mensual", modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  tendencia_mensual: {tipo} {anio} listo")


def construir_todo_descriptivo(spark: SparkSession):
    construir_viajes_por_hora(spark)
    construir_viajes_por_zona(spark)
    construir_tendencia_mensual(spark)
    log.info("Gold descriptivo completo")
