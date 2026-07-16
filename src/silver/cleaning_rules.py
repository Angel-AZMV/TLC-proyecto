"""
cleaning_rules.py
==================
Reglas de limpieza por tipo de vehículo, centralizadas en un solo lugar.
Cada función recibe un DataFrame ya con columnas renombradas al esquema
Silver y retorna el filtro booleano de PySpark a aplicar.

Justificación de cada regla: ver docs/decisiones_limpieza.md
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

import sys
sys.path.append('/home/jovyan/src')
from config import UMBRALES, PAYMENT_TYPES_VALIDOS_NEGATIVO, PAYMENT_TYPES_ERROR_NEGATIVO


def _duracion_min_expr():
    """Expresión reutilizable: duración del viaje en minutos."""
    return (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60


def filtro_yellow_green() -> "F.Column":
    """Reglas comunes para Yellow y Green Taxi (tienen tarifa y distancia)."""
    dur = _duracion_min_expr()

    return (
        ~((F.col("fare_amount") < 0) & (F.col("payment_type").isin(*PAYMENT_TYPES_ERROR_NEGATIVO)))
        & (F.col("fare_amount") <= UMBRALES["fare_amount_max"])
        & (F.col("fare_amount") >= 0)
        & ~((F.col("trip_distance") <= 0) & (dur > UMBRALES["gps_check_duracion_min"]))
        & (F.col("trip_distance") <= UMBRALES["trip_distance_max_taxi"])
        & (dur >= UMBRALES["duracion_min_min"])
        & (dur <= UMBRALES["duracion_min_max"])
    )


def filtro_green_extra() -> "F.Column":
    """Regla adicional solo para Green: tarifa mínima legal."""
    return F.col("fare_amount") >= UMBRALES["fare_amount_min"]


def filtro_fhv() -> "F.Column":
    """
    FHV: solo se conservan filas con locations completas)
    Sin tarifa/distancia que filtrar.
    """
    dur = _duracion_min_expr()

    return (
        F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
        & (dur >= UMBRALES["duracion_min_min"])
        & (dur <= UMBRALES["duracion_min_max"])
    )


def filtro_fhvhv() -> "F.Column":
    """Reglas para FHVHV (Uber/Lyft): más estrictas en driver_pay, sin devoluciones válidas."""
    dur = _duracion_min_expr()

    return (
        (F.col("fare_amount") >= UMBRALES["fare_amount_min"])
        & (F.col("driver_pay") > 0)
        & ~((F.col("trip_distance") <= 0) & (dur > UMBRALES["gps_check_duracion_min"]))
        & (F.col("trip_distance") <= UMBRALES["trip_distance_max_fhvhv"])
        & (dur >= UMBRALES["duracion_min_min"])
        & (dur <= UMBRALES["duracion_min_max"])
    )


def filtro_fecha_correcta(anio: int, mes: int) -> "F.Column":
    """
    Filtro crítico descubierto durante el desarrollo: algunos archivos Bronze
    contienen registros con pickup_datetime corrupto (ej. años como 2008
    dentro de un archivo de 2023-02). Se valida que el año/mes del dato
    coincida con el año/mes del archivo de origen.
    """
    return (F.year("pickup_datetime") == anio) & (F.month("pickup_datetime") == mes)


def agregar_columnas_calculadas(df: DataFrame, anio: int, mes: int) -> DataFrame:
    """Agrega las columnas derivadas comunes a todos los tipos de vehículo."""
    dur = _duracion_min_expr()
    return df.withColumns({
        "duracion_min": dur,
        "hora":         F.hour("pickup_datetime"),
        "dia_semana":   F.dayofweek("pickup_datetime"),
        "mes":          F.lit(mes),
        "anio":         F.lit(anio),
    })
