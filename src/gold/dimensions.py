"""
dimensions.py
=============
Construye las tablas de dimensión del Gold:

- dim_ubicacion: catálogo de zonas NYC (desde taxi_zone_lookup.csv)
- dim_tiempo: calendario derivado del rango de fechas presente en Silver

Estas tablas son pequeñas (cientos de filas) y se recalculan completas
cada vez, no necesitan procesamiento incremental.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
sys.path.append('/home/jovyan/src')
from config import DIMS_PATH, GOLD_PATH, ANIOS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def construir_dim_ubicacion(spark: SparkSession) -> DataFrame:
    """
    Lee el catálogo oficial de zonas TLC (taxi_zone_lookup.csv) y lo
    estandariza como dimensión Gold.
    """
    archivo = DIMS_PATH / "taxi_zones.csv"

    df = spark.read.csv(str(archivo), header=True, inferSchema=True)

    dim_ubicacion = df.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zona"),
        F.col("service_zone").alias("tipo_zona"),
    )

    destino = GOLD_PATH / "dims" / "dim_ubicacion"
    destino.mkdir(parents=True, exist_ok=True)

    dim_ubicacion.write.mode("overwrite").parquet(str(destino))
    log.info(f"dim_ubicacion escrita: {dim_ubicacion.count()} zonas")

    return dim_ubicacion


def construir_dim_tiempo(spark: SparkSession) -> DataFrame:
    """
    Genera un calendario diario para los años configurados, con columnas
    derivadas útiles para los dashboards (nombre de día, nombre de mes,
    trimestre, fin de semana, etc).
    """
    anio_min = min(ANIOS)
    anio_max = max(ANIOS)

    fechas = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{anio_min}-01-01'),
            to_date('{anio_max}-12-31'),
            interval 1 day
        )) as fecha
    """)

    dim_tiempo = fechas.select(
        F.col("fecha"),
        F.year("fecha").alias("anio"),
        F.month("fecha").alias("mes"),
        F.date_format("fecha", "MMMM").alias("nombre_mes"),
        F.dayofmonth("fecha").alias("dia"),
        F.dayofweek("fecha").alias("dia_semana_num"),
        F.date_format("fecha", "EEEE").alias("nombre_dia"),
        F.quarter("fecha").alias("trimestre"),
        F.when(F.dayofweek("fecha").isin(1, 7), True).otherwise(False).alias("es_fin_semana"),
    )

    destino = GOLD_PATH / "dims" / "dim_tiempo"
    destino.mkdir(parents=True, exist_ok=True)

    dim_tiempo.write.mode("overwrite").parquet(str(destino))
    log.info(f"dim_tiempo escrita: {dim_tiempo.count()} dias")

    return dim_tiempo


def construir_todas_las_dimensiones(spark: SparkSession):
    """Construye dim_ubicacion y dim_tiempo en un solo paso."""
    construir_dim_ubicacion(spark)
    construir_dim_tiempo(spark)
    log.info("Todas las dimensiones Gold construidas")
