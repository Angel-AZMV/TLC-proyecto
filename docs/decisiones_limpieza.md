# Decisiones de Limpieza y Diseño — Proyecto TLC Trip Record Data

## 1. Arquitectura

Medallion (Bronze / Silver / Gold) implementada con PySpark + Docker.

- **Bronze**: datos crudos descargados directamente de NYC TLC sin modificar.
- **Silver**: datos unificados, limpios y enriquecidos con columnas calculadas.
- **Gold**: tablas agregadas listas para consumo en Power BI.

## 2. Fuentes de datos

| Tipo | Descripción | Años | Filas aprox. (sin limpiar) |
|------|-------------|------|------------------------------|
| Yellow Taxi | Taxis amarillos, Manhattan/aeropuertos | 2023-2025 | ~128M |
| Green Taxi | Taxis verdes, outer boroughs | 2023-2025 | ~2M |
| FHV | Vehículos de alquiler tradicionales | 2023-2025 | ~58M |
| FHVHV | Uber/Lyft/similares | 2023-2025 | ~230M+ |

## 3. Esquema unificado (Silver)

```
tipo_vehiculo, pickup_datetime, dropoff_datetime, pu_location_id,
do_location_id, fare_amount, tip_amount, total_amount, driver_pay,
tolls_amount, congestion_surcharge, airport_fee, trip_distance,
trip_time_sec, passenger_count, payment_type, duracion_min,
hora, dia_semana, mes, anio
```

Columnas no disponibles en el dataset original quedan como `NULL`
(ej: `driver_pay` no existe en yellow/green, `fare_amount` no existe en FHV).

## 4. Hallazgos del EDA y reglas de limpieza aplicadas

### Yellow Taxi
- `passenger_count`, `congestion_surcharge`, `airport_fee`, `RatecodeID` tienen
  13.27% de nulos, proveniente 100% del VendorID 6 que no reporta esos campos.
  **Decisión:** se mantienen los nulos, no se elimina la fila (el viaje sí ocurrió).
- `fare_amount < 0` con `payment_type IN (3,4)` (no charge / dispute) es válido,
  representa devoluciones reales. Solo se eliminan negativos con
  `payment_type IN (0,1,2)` → 0.19% de las filas.
- `trip_distance <= 0` combinado con duración > 3 min indica fallo de GPS → eliminado.
- Percentiles de referencia (enero 2023): fare P95=$66, P99=$73, max=$1,160 (outlier).
  **Umbral aplicado:** `fare_amount <= 300`.
- Percentiles de distancia: P95=14.3mi, P99=20mi, max=258,928mi (imposible).
  **Umbral aplicado:** `trip_distance <= 80`.
- Duración: P95=36.5min, P99=57min, max=10,029min (~7 días, error).
  **Umbral aplicado:** `1 min <= duracion <= 200 min`.

### Green Taxi
- Mismas reglas que Yellow.
- Tarifa mínima real en NYC es $3.00, se aplica `fare_amount >= 2.50` como margen.
- Distancia GPS inválida: 3.05% de las filas (mayor que yellow, por operar en
  zonas con peor cobertura GPS).

### FHV (For-Hire Vehicle)
- **Hallazgo crítico:** 78.92% de las filas no tienen `PULocationID`. Investigación
  por `dispatching_base_num` reveló que ciertas bases reportan 100% nulo en
  locations, mientras otras reportan casi perfecto (0.00-0.04%). No es error
  aleatorio, es falta de infraestructura GPS en bases pequeñas.
- **Decisión:** se conservan únicamente las filas con
  `pu_location_id` y `do_location_id` no nulos (21.06% del dataset original).
  Sin esto, FHV no aporta valor a los dashboards de zonas ni a los modelos
  predictivos de demanda geográfica.
- No tiene columnas de tarifa ni distancia en el dataset original.

### FHVHV (Uber/Lyft)
- El dataset más limpio: 0% nulos en locations, fechas, fare.
- `driver_pay <= 0`: 1.57% de las filas. A diferencia de Yellow, Uber/Lyft no
  registra devoluciones como filas negativas (las correcciones se hacen
  internamente). **Decisión:** se eliminan completamente, no se asume como
  devolución válida.
- Tarifa mínima: `base_passenger_fare >= 2.50`.
- Distancia máxima ajustada a 150 millas (mayor que taxis porque Uber/Lyft
  cubre trayectos interestatales ocasionales).

## 5. Resumen de impacto de limpieza (enero 2023, referencia)

| Tipo | Filas originales | % eliminado aprox. |
|------|-------------------|---------------------|
| Yellow | 3,066,766 | ~2% |
| Green | 68,211 | ~5% |
| FHV | 1,114,320 | ~79% (decisión de diseño, no error) |
| FHVHV | 18,479,031 | ~2% |

## 6. Catálogo de zonas

Se incorpora `taxi_zone_lookup.csv` (fuente oficial TLC) como dimensión de
ubicación para traducir `location_id` (1-263) a nombre de zona, borough y
tipo de servicio en los dashboards.

## 7. Particionamiento físico

Silver y Gold se particionan por `tipo_vehiculo / anio / mes` para que las
consultas de Power BI y los procesos de Gold no escaneen el dataset completo.

## 8. Incidentes conocidos durante el desarrollo

- Se detectaron registros con `pickup_datetime` corruptos en el dataset
  original (ej. fechas de 2008 dentro de un archivo de 2023-02), confirmado
  como error de origen en TLC, no de procesamiento propio. Se corrige
  filtrando por `YEAR(pickup_datetime)` y `MONTH(pickup_datetime)` coincidente
  con el archivo de origen.
