"""
schemas.py
==========
Define el SELECT de renombrado de columnas Bronze -> esquema Silver
unificado, por cada tipo de vehículo. Mantiene la lógica de mapeo
separada de la lógica de limpieza (cleaning_rules.py) y de la
orquestación (transform.py).
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def select_yellow(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("yellow").alias("tipo_vehiculo"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("PULocationID").cast("long").alias("pu_location_id"),
        F.col("DOLocationID").cast("long").alias("do_location_id"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.lit(None).cast("double").alias("driver_pay"),
        F.col("tolls_amount"),
        F.col("congestion_surcharge"),
        F.col("airport_fee"),
        F.col("trip_distance"),
        F.lit(None).cast("long").alias("trip_time_sec"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("payment_type").cast("integer"),
    )


def select_green(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("green").alias("tipo_vehiculo"),
        F.col("lpep_pickup_datetime").alias("pickup_datetime"),
        F.col("lpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("PULocationID").cast("long").alias("pu_location_id"),
        F.col("DOLocationID").cast("long").alias("do_location_id"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.lit(None).cast("double").alias("driver_pay"),
        F.col("tolls_amount"),
        F.col("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee"),
        F.col("trip_distance"),
        F.lit(None).cast("long").alias("trip_time_sec"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("payment_type").cast("integer"),
    )


def select_fhv(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("fhv").alias("tipo_vehiculo"),
        F.col("pickup_datetime"),
        F.col("dropOff_datetime").alias("dropoff_datetime"),
        F.col("PUlocationID").cast("long").alias("pu_location_id"),
        F.col("DOlocationID").cast("long").alias("do_location_id"),
        F.lit(None).cast("double").alias("fare_amount"),
        F.lit(None).cast("double").alias("tip_amount"),
        F.lit(None).cast("double").alias("total_amount"),
        F.lit(None).cast("double").alias("driver_pay"),
        F.lit(None).cast("double").alias("tolls_amount"),
        F.lit(None).cast("double").alias("congestion_surcharge"),
        F.lit(None).cast("double").alias("airport_fee"),
        F.lit(None).cast("double").alias("trip_distance"),
        F.lit(None).cast("long").alias("trip_time_sec"),
        F.lit(None).cast("double").alias("passenger_count"),
        F.lit(None).cast("integer").alias("payment_type"),
    )


def select_fhvhv(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("fhvhv").alias("tipo_vehiculo"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("PULocationID").cast("long").alias("pu_location_id"),
        F.col("DOLocationID").cast("long").alias("do_location_id"),
        F.col("base_passenger_fare").alias("fare_amount"),
        F.col("tips").alias("tip_amount"),
        F.lit(None).cast("double").alias("total_amount"),
        F.col("driver_pay"),
        F.col("tolls").alias("tolls_amount"),
        F.col("congestion_surcharge"),
        F.col("airport_fee"),
        F.col("trip_miles").alias("trip_distance"),
        F.col("trip_time").alias("trip_time_sec"),
        F.lit(None).cast("double").alias("passenger_count"),
        F.lit(None).cast("integer").alias("payment_type"),
    )


# Registro central: tipo -> función de selección
SELECTORS = {
    "yellow": select_yellow,
    "green":  select_green,
    "fhv":    select_fhv,
    "fhvhv":  select_fhvhv,
}
