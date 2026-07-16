# TLC Proyecto — Pipeline de Datos NYC Taxi

Proyecto de arquitectura medallion (bronze → silver → gold) sobre datos públicos de la NYC Taxi & Limousine Commission (TLC), con dashboards en Power BI y modelos predictivos (series de tiempo, clustering, clasificación).

## Estructura del repositorio

```
├── notebooks/
│   └── 01_eda_bronze.ipynb               # Exploración de datos crudos (no productivo, opcional)
├── pipelines/                             # Pipeline productivo, ejecutar en este orden
│   ├── 01_download.py                    # Descarga de datos crudos NYC TLC (script, no notebook)
│   ├── 02_bronze_to_silver_v2.ipynb      # Limpieza y estandarización
│   ├── 03_silver_to_gold.ipynb           # Tablas descriptivas y diagnósticas
│   └── 04_silver_to_gold_predictivo.ipynb # Modelos predictivos (Prophet)
├── src/                                   # Módulos reutilizables
│   ├── config.py                         # Rutas y constantes del proyecto
│   ├── spark_utils.py                    # Creación de sesión Spark
│   ├── silver/
│   │   ├── cleaning_rules.py             # Reglas de limpieza aplicadas en bronze -> silver
│   │   ├── schemas.py                    # Esquemas esperados por tipo de vehículo
│   │   └── transform.py                  # Transformaciones bronze -> silver
│   └── gold/
│       ├── dimensions.py                 # dim_tiempo, dim_ubicacion
│       ├── descriptivo.py                # viajes_por_hora, viajes_por_zona, tendencia_mensual
│       ├── diagnostico.py                # (deprecado, ver notas abajo)
│       └── predictivo.py                 # features_serie_tiempo, forecast_demanda
├── docs/                                  # Documentación adicional
└── data/
    ├── dims/                             # taxi_zones.csv (fuente para dim_ubicacion)
    └── gold/                             # Tablas Gold ya generadas (incluidas en el repo)
```

> `pipelines/` contiene el flujo productivo real (ejecutar en orden 01→04). `notebooks/` es solo para exploración de datos (EDA), no forma parte del pipeline.

> **Nota:** `data/bronze/` y `data/silver/trips/` **no están incluidas** en este repositorio por su tamaño (~30GB). Se regeneran localmente siguiendo los pasos de abajo.

---

## Opción rápida: usar `gold` ya generado

Si solo necesitas trabajar con Power BI o los notebooks de modelos predictivos, **no necesitas reproducir todo el pipeline**. La carpeta `data/gold/` ya viene incluida en este repo — con clonar es suficiente:

```bash
git clone https://github.com/Angel-AZMV/TLC-proyecto.git
cd TLC-proyecto
```

Los archivos parquet de `data/gold/` quedan listos para abrir en Power BI o para correr `04_silver_to_gold_predictivo.ipynb`.

---

## Opción completa: reproducir el pipeline desde cero

Necesario solo si vas a modificar la lógica de bronze→silver→gold.

### Requisitos previos

- Python 3.11
- PySpark 3.5.0
- Librerías: `pandas`, `prophet==1.3.0`, `scikit-learn`, `duckdb==1.5.4`

```bash
pip install pyspark==3.5.0 prophet==1.3.0 scikit-learn duckdb==1.5.4 pandas --break-system-packages
```

### Paso 1 — Descargar datos crudos (bronze)

Script con argumentos por línea de comandos. **Nota:** el flag `--year` no filtra la descarga — el script siempre usa el rango de años definido en `ANIOS` (`src/config.py`), independientemente del valor pasado. Solo `--dtype` es necesario para elegir el tipo de vehículo:

```bash
python pipelines/01_download.py --dtype yellow --workers 4
python pipelines/01_download.py --dtype green --workers 4
python pipelines/01_download.py --dtype fhv --workers 4
python pipelines/01_download.py --dtype fhvhv --workers 4
```

Cada ejecución descarga automáticamente todos los años configurados para ese tipo de vehículo. `--workers` controla la descarga en paralelo (ajustable según tu conexión). El script detecta archivos ya descargados y los omite (`[SKIP] Ya existe: ...`), así que se puede interrumpir y volver a correr sin duplicar trabajo.

Descarga los archivos parquet mensuales de NYC TLC directo desde la fuente oficial (`https://d37ci6vzurychx.cloudfront.net/trip-data`), guardados como `data/bronze/{tipo}/{tipo}_tripdata_YYYY-MM.parquet`.

**Fuente de datos:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Paso 2 — Bronze a Silver

```bash
jupyter nbconvert --to notebook --execute pipelines/02_bronze_to_silver_v2.ipynb
```

Limpieza, tipado, y estandarización de columnas (lógica en `src/silver/cleaning_rules.py`, `schemas.py` y `transform.py`). Particiona por `tipo_vehiculo` y `anio` (con subpartición `mes` según el tipo).

> El pipeline procesa archivo por archivo (no une todo en memoria) por límite de RAM del entorno, e incluye reintento automático ante fallos de memoria de Spark. También tiene resume automático: si ya procesó una combinación tipo/año/mes, la salta. Es normal ver mensajes de `[SKIP]` o reintentos en el log — no son errores.

### Paso 3 — Silver a Gold (descriptivo)

```bash
jupyter nbconvert --to notebook --execute pipelines/03_silver_to_gold.ipynb
```

Construye:
- `gold/dims/dim_tiempo`, `gold/dims/dim_ubicacion` (desde `data/dims/taxi_zones.csv`)
- `gold/descriptivo/viajes_por_hora`, `viajes_por_zona`, `tendencia_mensual`

### Paso 4 — Silver a Gold (predictivo)

```bash
jupyter nbconvert --to notebook --execute pipelines/04_silver_to_gold_predictivo.ipynb
```

Construye `gold/predictivo/features_serie_tiempo` y entrena modelos Prophet (con feriados de EE. UU. como regresor) para los 4 tipos de vehículo, exportando el forecast a `gold/predictivo/forecast_demanda`.

### Opcional — Exploración de datos

```bash
jupyter nbconvert --to notebook --execute notebooks/01_eda_bronze.ipynb
```

No es parte del pipeline productivo; útil solo para explorar visualmente los datos crudos.

---

## Modelo de datos (Power BI)

El modelo es una **constelación de hechos** (fact constellation), no un star schema simple: varias tablas de hechos (`viajes_por_zona`, `viajes_por_hora`, `tendencia_mensual`) comparten dimensiones conformadas (`dim_tiempo`, `dim_ubicacion`). Cada fact table tiene un grano distinto, intencional según la pregunta de negocio que responde. `tipo_vehiculo` se maneja como dimensión degenerada dentro de cada fact table.

---

## Notas de decisiones tomadas

- **Outlier de yellow (22–24 sep 2023):** se excluyó de la serie de tiempo usada para entrenar Prophet por evidencia de falla de ingesta específica del proveedor (confirmado por comparación cruzada contra los demás tipos de vehículo, que no mostraron la misma caída).
- **`diagnostico.py`** (`comparacion_vehiculos`, `duracion_distancia`, `tarifas_por_zona`) quedó deprecado por redundancia con `tendencia_mensual`; no se usa en el modelo final de Power BI.

---

## Equipo

Repositorio del proyecto de curso — arquitectura de datos con PySpark, Power BI y modelos predictivos sobre datos de NYC TLC.
