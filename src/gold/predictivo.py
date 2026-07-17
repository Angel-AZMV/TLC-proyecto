import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
sys.path.append('/home/jovyan/src')
from config import SILVER_PATH, GOLD_PATH, ANIOS, TIPOS_VEHICULO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _escribir_gold(df: DataFrame, nombre: str, particion: list = None, modo: str = "overwrite"):
    destino = GOLD_PATH / "predictivo" / nombre
    destino.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode(modo)
    if particion:
        writer = writer.partitionBy(*particion)
    writer.parquet(str(destino))
    log.info(f"Gold escrito ({modo}): predictivo/{nombre} ({df.count():,} filas)")


def construir_features_serie_tiempo(spark: SparkSession):
    primero = True
    for tipo in TIPOS_VEHICULO:
        for anio in ANIOS:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            silver = spark.read.parquet(str(path_tipo))
            resultado = silver.groupBy(
                F.to_date("pickup_datetime").alias("fecha")
            ).agg(
                F.count("*").alias("total_viajes")
            ).withColumn("dia_semana", F.dayofweek("fecha")) \
             .withColumn("mes", F.month("fecha")) \
             .withColumn("anio", F.lit(anio)) \
             .withColumn("tipo_vehiculo", F.lit(tipo))

            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado, "features_serie_tiempo",
                            particion=["tipo_vehiculo", "anio"], modo=modo)
            primero = False
            spark.catalog.clearCache()
            log.info(f"  features_serie_tiempo: {tipo} {anio} listo")

def construir_features_clustering(spark: SparkSession):
    """
    Grano: zona (location_id), consolidado sobre TODO el histórico
    (todos los tipos de vehículo y años juntos).
    """
    # Lee todas las particiones de silver de una sola vez
    silver_completo = spark.read.parquet(str(SILVER_PATH))

    resultado = silver_completo.groupBy(
        F.col("pu_location_id").alias("location_id")
    ).agg(
        F.count("*").alias("total_viajes"),
        F.round(F.avg("fare_amount"), 2).alias("fare_promedio"),
        F.round(F.avg("trip_distance"), 2).alias("distancia_promedio"),
        F.round(F.avg("duracion_min"), 2).alias("duracion_promedio"),
        F.round(F.avg("hora"), 2).alias("hora_promedio_salida"),
        F.round(F.avg(
            F.when(F.col("hora").isin(7, 8, 17, 18), 1).otherwise(0)
        ), 3).alias("pct_hora_pico"),
    )

    _escribir_gold(resultado, "features_clustering", particion=None, modo="overwrite")
    log.info(f"features_clustering: {resultado.count()} zonas")

def construir_features_clasificacion(spark: SparkSession):
    """
    Grano: Viaje individual (Yellow, Green, FHVHV, FHV).
    Target: Predicción de viaje al aeropuerto (1 = Sí, 0 = No).
    Nota: Se aplica una fracción de muestreo (0.01 = 1%) para que el dataset resultante 
    sea manejable en memoria local al entrenar el Random Forest con Scikit-Learn.
    """
    log.info("Iniciando construcción de features para clasificación (Aeropuerto)...")
    # Agregamos fhv a la lista maestra
    tipos_clasificacion = ["yellow", "green", "fhvhv", "fhv"]
    primero = True

    for tipo in tipos_clasificacion:
        for anio in ANIOS:
            path_tipo = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}"
            if not path_tipo.exists():
                continue
            
            silver = spark.read.parquet(str(path_tipo))
            
            # 1. Filtrar nulos esenciales
            silver_filtrado = silver.filter(
                F.col("do_location_id").isNotNull() & 
                F.col("trip_distance").isNotNull() &
                F.col("duracion_min").isNotNull() &
                F.col("fare_amount").isNotNull()
            )
            
            # 2. Definir las zonas TLC oficiales de los aeropuertos
            zonas_aeropuerto = [1, 132, 138]
            
            # 3. Selección de Features y Sampling (1%)
            resultado = silver_filtrado.sample(withReplacement=False, fraction=0.01, seed=42) \
                .select(
                    F.col("trip_distance").cast("double"),
                    F.col("duracion_min").cast("double"),
                    F.col("fare_amount").cast("double"),
                    F.col("hora").cast("integer"),
                    F.dayofweek("pickup_datetime").alias("dia_semana"),
                    # Convertir el tipo de vehículo a variables binarias (One-Hot Encoding manual)
                    F.when(F.lit(tipo) == "yellow", 1).otherwise(0).alias("es_yellow"),
                    F.when(F.lit(tipo) == "green", 1).otherwise(0).alias("es_green"),
                    F.when(F.lit(tipo) == "fhvhv", 1).otherwise(0).alias("es_fhvhv"),
                    # Nota: Si es_yellow=0, es_green=0 y es_fhvhv=0, el modelo sabrá que es 'fhv'
                    
                    # TARGET: 1 si el destino es un aeropuerto, 0 si no lo es
                    F.when(F.col("do_location_id").isin(zonas_aeropuerto), 1).otherwise(0).alias("target_aeropuerto"),
                    F.lit(anio).cast("integer").alias("anio")
                ).dropna()
            
            modo = "overwrite" if primero else "append"
            _escribir_gold(resultado, "features_clasificacion", particion=["anio"], modo=modo)
            primero = False
            
            spark.catalog.clearCache()
            log.info(f"  features_clasificacion: {tipo} {anio} listo")
            
    log.info("Dataset de clasificación finalizado.")