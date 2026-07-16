"""
spark_utils.py
==============
Manejo centralizado de la SparkSession, incluyendo:
- Creación con configuración optimizada para 8GB RAM
- Reintento automático de una función cuando Spark muere por memoria
"""

import time
import logging
from pyspark.sql import SparkSession

import sys
sys.path.append('/home/jovyan/src')
from config import SPARK_CONFIG, APP_NAME, CHECKPOINT_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def crear_spark_session() -> SparkSession:
    """Crea una SparkSession nueva con la configuración estándar del proyecto."""
    builder = SparkSession.builder.appName(APP_NAME)

    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(CHECKPOINT_PATH))

    log.info(f"SparkSession creada: {spark.version}")
    return spark


def reiniciar_spark(spark: SparkSession) -> SparkSession:
    """Detiene la sesión actual (si existe) y crea una nueva limpia."""
    try:
        spark.stop()
        log.info("SparkSession anterior detenida")
    except Exception:
        pass

    time.sleep(2) 
    return crear_spark_session()


def ejecutar_con_reintento(spark: SparkSession, funcion, *args, max_intentos: int = 3, **kwargs):
    """
    Ejecuta `funcion(spark, *args, **kwargs)`. Si Spark muere por memoria,
    reinicia la sesión automáticamente y reintenta hasta `max_intentos` veces.

    Retorna (spark, exito: bool) — siempre devuelve la sesión Spark vigente,
    porque puede haber sido reemplazada por una nueva tras el reinicio.
    """
    for intento in range(1, max_intentos + 1):
        try:
            funcion(spark, *args, **kwargs)
            return spark, True
        except Exception as e:
            log.error(f"[Intento {intento}/{max_intentos}] Falló: {e}")
            if intento < max_intentos:
                log.info("Reiniciando Spark y reintentando...")
                spark = reiniciar_spark(spark)
            else:
                log.error("Se agotaron los reintentos, se omite esta tarea")
                return spark, False

    return spark, False
