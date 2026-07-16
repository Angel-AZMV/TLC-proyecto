"""
config.py
=========
Configuración centralizada del proyecto TLC.
Toda ruta, año o umbral de limpieza vive aquí, en un solo lugar.
"""

from pathlib import Path

# ──────────────────────────────────────────────
# RUTAS BASE
# ──────────────────────────────────────────────
DATA_ROOT   = Path("/home/jovyan/data")
BRONZE_PATH = DATA_ROOT / "bronze"
SILVER_PATH = DATA_ROOT / "silver" / "trips"
GOLD_PATH   = DATA_ROOT / "gold"
DIMS_PATH   = DATA_ROOT / "dims"
LOGS_PATH   = DATA_ROOT / "logs"
CHECKPOINT_PATH = DATA_ROOT / "checkpoints"

# ──────────────────────────────────────────────
# AÑOS Y MESES A PROCESAR
# ──────────────────────────────────────────────
ANIOS = [2023, 2024, 2025]
MESES = list(range(1, 13))

TIPOS_VEHICULO = ["yellow", "green", "fhv", "fhvhv"]

# ──────────────────────────────────────────────
# UMBRALES DE LIMPIEZA (definidos en el EDA)
# ──────────────────────────────────────────────
UMBRALES = {
    "fare_amount_max":      300,    # outlier extremo (P99 ~$73, max real ~$1160)
    "fare_amount_min":      2.50,   # tarifa mínima legal en NYC
    "trip_distance_max_taxi":  80,  # yellow/green (P99 ~20mi)
    "trip_distance_max_fhvhv": 150, # fhvhv cubre trayectos más largos
    "duracion_min_min":     1,      # viaje fantasma si es menor
    "duracion_min_max":     200,    # ~3.3 horas, cubre tráfico extremo NYC
    "gps_check_duracion_min": 3,    # distancia=0 + duracion>3min => GPS malo
}

PAYMENT_TYPES_VALIDOS_NEGATIVO = (3, 4)  # no charge / dispute => fare negativo es válido
PAYMENT_TYPES_ERROR_NEGATIVO   = (0, 1, 2)  # fare negativo aquí es error real

# ──────────────────────────────────────────────
# CONFIGURACIÓN SPARK
# ──────────────────────────────────────────────
SPARK_CONFIG = {
    "spark.sql.session.timeZone":        "America/New_York",
    "spark.driver.memory":               "10g",
    "spark.executor.memory":             "4g",
    "spark.sql.shuffle.partitions":      "90",
    "spark.default.parallelism":         "90",
    "spark.memory.offHeap.enabled":      "true",
    "spark.memory.offHeap.size":         "2g",
    "spark.driver.maxResultSize":        "2g",
    "spark.sql.files.maxPartitionBytes": "134217728", 
    "spark.serializer":                  "org.apache.spark.serializer.KryoSerializer",
}

APP_NAME = "TLC_Pipeline"
