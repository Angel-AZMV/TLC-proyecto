"""
transform.py
============
Funciones que procesan UN archivo Bronze (un tipo de vehículo, un mes)
y lo escriben a Silver. Diseñadas para ejecutarse archivo por archivo
(no unión completa en memoria) por la limitación de RAM del entorno.
"""

import os
import logging
from pyspark.sql import SparkSession

import sys
sys.path.append('/home/jovyan/src')
from config import BRONZE_PATH, SILVER_PATH, LOGS_PATH
from silver.schemas import SELECTORS
from silver.cleaning_rules import (
    filtro_yellow_green, filtro_green_extra, filtro_fhv, filtro_fhvhv,
    filtro_fecha_correcta, agregar_columnas_calculadas,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

LOGS_PATH.mkdir(parents=True, exist_ok=True)
_AUDITORIA_PATH = LOGS_PATH / "auditoria_silver.csv"


def _registrar_auditoria(tipo: str, anio: int, mes: int, filas_entrada: int, filas_salida: int):
    """Guarda una línea de auditoría: cuántas filas entraron y cuántas pasaron el filtro."""
    existe = _AUDITORIA_PATH.exists()
    with open(_AUDITORIA_PATH, "a") as f:
        if not existe:
            f.write("tipo_vehiculo,anio,mes,filas_entrada,filas_salida,pct_retenido\n")
        pct = round(100 * filas_salida / filas_entrada, 2) if filas_entrada > 0 else 0
        f.write(f"{tipo},{anio},{mes},{filas_entrada},{filas_salida},{pct}\n")


def _ya_procesado(destino: str) -> bool:
    """Verifica si el mes ya fue procesado exitosamente (existe y tiene archivos)."""
    return os.path.exists(destino) and len(os.listdir(destino)) > 0


def _procesar_generico(spark: SparkSession, tipo: str, anio: int, mes: int, filtro_extra=None):
    """
    Lógica común a los 4 tipos de vehículo: lee Bronze, aplica el selector
    de schema correspondiente, filtra, agrega columnas calculadas y escribe Silver.
    """
    archivo = BRONZE_PATH / tipo / str(anio) / f"{tipo}_tripdata_{anio}-{mes:02d}.parquet"
    destino = SILVER_PATH / f"tipo_vehiculo={tipo}" / f"anio={anio}" / f"mes={mes}"

    if not archivo.exists():
        log.info(f"[SKIP] No existe en Bronze: {tipo} {anio}-{mes:02d}")
        return

    if _ya_procesado(str(destino)):
        log.info(f"[SKIP] Ya procesado: {tipo} {anio}-{mes:02d}")
        return

    destino.mkdir(parents=True, exist_ok=True)

    df_raw = spark.read.parquet(str(archivo))
    filas_entrada = df_raw.count()

    df = SELECTORS[tipo](df_raw)

    filtro = filtro_extra if filtro_extra is not None else None
    if filtro is not None:
        df = df.filter(filtro)

    df = df.filter(filtro_fecha_correcta(anio, mes))
    df = agregar_columnas_calculadas(df, anio, mes)

    filas_salida = df.count()

    df.write.mode("overwrite").parquet(str(destino))
    spark.catalog.clearCache()

    _registrar_auditoria(tipo, anio, mes, filas_entrada, filas_salida)
    log.info(f"[OK] {tipo} {anio}-{mes:02d}: {filas_entrada:,} -> {filas_salida:,} filas")


def procesar_yellow(spark: SparkSession, anio: int, mes: int):
    _procesar_generico(spark, "yellow", anio, mes, filtro_extra=filtro_yellow_green())


def procesar_green(spark: SparkSession, anio: int, mes: int):
    filtro = filtro_yellow_green() & filtro_green_extra()
    _procesar_generico(spark, "green", anio, mes, filtro_extra=filtro)


def procesar_fhv(spark: SparkSession, anio: int, mes: int):
    _procesar_generico(spark, "fhv", anio, mes, filtro_extra=filtro_fhv())


def procesar_fhvhv(spark: SparkSession, anio: int, mes: int):
    _procesar_generico(spark, "fhvhv", anio, mes, filtro_extra=filtro_fhvhv())


PROCESADORES = {
    "yellow": procesar_yellow,
    "green":  procesar_green,
    "fhv":    procesar_fhv,
    "fhvhv":  procesar_fhvhv,
}
