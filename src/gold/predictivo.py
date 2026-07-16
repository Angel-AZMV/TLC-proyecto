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